use std::collections::HashSet;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{self, Sender};
use std::thread;
use std::time::{Duration, Instant};

use reqwest::header::{CONTENT_RANGE, ETAG};
use reqwest::{StatusCode, Url};
use serde::{Deserialize, Serialize};

use crate::cli::{DownloadArgs, RepoType};
use crate::error::{AppError, AppResult};
use crate::hub::{HubClient, TreeEntry};
use crate::patterns::PatternSet;
use crate::text::escape_control;

const COPY_BUFFER_SIZE: usize = 64 * 1024;
const CHECKPOINT_BYTES: u64 = 8 * 1024 * 1024;
const PROGRESS_INTERVAL: Duration = Duration::from_millis(200);
const RESUME_VERSION: u8 = 1;
const FNV1A_64_OFFSET_BASIS: u64 = 0xcbf29ce484222325;
const FNV1A_64_PRIME: u64 = 0x00000100000001b3;

struct DownloadTask {
    index: usize,
    remote_path: String,
    destination: PathBuf,
    partial_path: PathBuf,
    metadata_path: PathBuf,
    metadata_temporary_path: PathBuf,
    url: Url,
    expected_length: u64,
    repo: String,
    repo_type: RepoType,
    commit: String,
    oid: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct ResumeMetadataV1 {
    version: u8,
    repo: String,
    repo_type: RepoType,
    commit: String,
    path: String,
    oid: String,
    size: u64,
    etag: Option<String>,
    ranges: Vec<RangeProgress>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct RangeProgress {
    start: u64,
    next: u64,
    end: u64,
}

struct FileTransfer {
    metadata: Mutex<ResumeMetadataV1>,
    transferred: AtomicU64,
    fallback: AtomicBool,
}

#[derive(Clone, Copy)]
struct WorkItem {
    file_index: usize,
    kind: WorkKind,
}

#[derive(Clone, Copy)]
enum WorkKind {
    Range(usize),
    Full,
}

pub fn execute(
    client: &HubClient,
    args: &DownloadArgs,
    current_directory: &Path,
    output: &mut dyn Write,
    progress_output: &mut dyn Write,
    interactive_progress: bool,
) -> AppResult<()> {
    if args.jobs == 0 {
        return Err(AppError::message("jobs must be greater than zero"));
    }
    let revision = client.resolve_revision(&args.repo, args.repo_type, &args.revision)?;
    let entries = client.list_tree(&args.repo, args.repo_type, &revision)?;
    let patterns = PatternSet::compile(&args.patterns)?;
    let mut files: Vec<&TreeEntry> = entries
        .iter()
        .filter(|entry| entry.is_file() && patterns.selects(&entry.path))
        .collect();
    files.sort_unstable_by(|left, right| left.path.cmp(&right.path));
    if files.is_empty() {
        return Err(AppError::message("download patterns matched no files"));
    }
    if args.dry_run {
        for entry in files {
            writeln!(output, "{}", escape_control(&entry.path))
                .map_err(|error| AppError::io("could not write dry-run output", error))?;
        }
        return Ok(());
    }

    let root = destination_root(args.directory.as_deref(), current_directory);
    prepare_root(&root)?;
    let total_bytes = files.iter().try_fold(0_u64, |total, entry| {
        total.checked_add(entry.size).ok_or_else(|| {
            AppError::message("selected repository files exceed the supported total size")
        })
    })?;
    let tasks = prepare_tasks(client, args, &revision, &root, &files)?;
    let recovered = inspect_recovered_downloads(&tasks)?;
    preflight(&root, &tasks, &recovered, args.force)?;
    prepare_task_directories(&root, &tasks)?;
    let transfers = prepare_transfers(&tasks, &recovered, args.jobs)?;
    let mut progress = ProgressWriter::new(progress_output, interactive_progress, &tasks);
    progress.start_batch(tasks.len(), total_bytes, &root, args.jobs)?;
    let download_result = download_concurrently(
        client,
        &tasks,
        &transfers,
        args.jobs,
        output,
        &mut progress,
        interactive_progress,
    );
    merge_results(download_result, progress.finish_batch())
}

struct ProgressWriter<'a> {
    output: &'a mut dyn Write,
    interactive: bool,
    disabled: bool,
    rendered_lines: usize,
    files: Vec<ProgressFile>,
}

impl<'a> ProgressWriter<'a> {
    fn new(output: &'a mut dyn Write, interactive: bool, tasks: &[DownloadTask]) -> Self {
        Self {
            output,
            interactive,
            disabled: false,
            rendered_lines: 0,
            files: tasks
                .iter()
                .map(|task| ProgressFile {
                    path: escape_control(&task.remote_path).into_owned(),
                    active: None,
                })
                .collect(),
        }
    }

    fn start_batch(
        &mut self,
        file_count: usize,
        total_bytes: u64,
        root: &Path,
        transfer_limit: usize,
    ) -> AppResult<()> {
        let noun = if file_count == 1 { "file" } else { "files" };
        let transfer_noun = if transfer_limit == 1 {
            "transfer"
        } else {
            "transfers"
        };
        let root = escape_control(&root.to_string_lossy()).into_owned();
        self.write(&format!(
            "Downloading {file_count} {noun} ({}) to {root} with up to {transfer_limit} simultaneous {transfer_noun}\n",
            format_bytes(total_bytes),
        ))
    }

    fn started(
        &mut self,
        index: usize,
        expected_length: u64,
        transferred: u64,
        started_at: Instant,
    ) -> AppResult<()> {
        self.clear_active()?;
        let file = self.progress_file_mut(index)?;
        if file.active.is_some() {
            return Err(AppError::message("download progress started twice"));
        }
        file.active = Some(ActiveProgress {
            expected_length: Some(expected_length),
            transferred,
            session_start_transferred: transferred,
            started_at,
            state: if transferred == 0 {
                TransferState::Downloading
            } else {
                TransferState::Resuming
            },
        });
        if self.interactive {
            self.render_active()
        } else if transferred == 0 {
            self.write_file_line(index, "downloading")
        } else {
            self.write_file_line(index, "resuming")
        }
    }

    fn restarted(&mut self, index: usize) -> AppResult<()> {
        self.clear_active()?;
        let active = self.active_progress_mut(index)?;
        active.transferred = 0;
        active.session_start_transferred = 0;
        active.started_at = Instant::now();
        active.state = TransferState::Downloading;
        if self.interactive {
            self.render_active()
        } else {
            self.write_file_line(index, "restarting without ranges")
        }
    }

    fn advanced(&mut self, index: usize, transferred: u64) -> AppResult<()> {
        self.clear_active()?;
        let active = self.active_progress_mut(index)?;
        active.transferred = active.transferred.max(transferred);
        active.state = TransferState::Downloading;
        self.render_active()
    }

    fn syncing(&mut self, index: usize, transferred: u64) -> AppResult<()> {
        self.clear_active()?;
        let active = self.active_progress_mut(index)?;
        active.transferred = active.transferred.max(transferred);
        active.state = TransferState::Syncing;
        self.render_active()
    }

    fn finished(&mut self, index: usize, transferred: u64) -> AppResult<()> {
        self.complete(index, transferred, "downloaded")
    }

    fn failed(&mut self, index: usize) -> AppResult<()> {
        let transferred = self.active_progress_mut(index)?.transferred;
        self.complete(index, transferred, "failed")
    }

    fn finish_batch(&mut self) -> AppResult<()> {
        self.clear_active()
    }

    fn complete(&mut self, index: usize, transferred: u64, state: &str) -> AppResult<()> {
        self.clear_active()?;
        self.active_progress_mut(index)?.transferred = transferred;
        let line = self.format_file_line(index, state)?;
        self.progress_file_mut(index)?.active = None;
        self.write_line(&line)?;
        self.render_active()
    }

    fn progress_file_mut(&mut self, index: usize) -> AppResult<&mut ProgressFile> {
        self.files
            .get_mut(index)
            .ok_or_else(|| AppError::message("download progress index is out of range"))
    }

    fn active_progress_mut(&mut self, index: usize) -> AppResult<&mut ActiveProgress> {
        self.progress_file_mut(index)?
            .active
            .as_mut()
            .ok_or_else(|| AppError::message("download progress is not active"))
    }

    fn write_file_line(&mut self, index: usize, state: &str) -> AppResult<()> {
        let line = self.format_file_line(index, state)?;
        self.write_line(&line)
    }

    fn format_file_line(&self, index: usize, state: &str) -> AppResult<String> {
        let file = self
            .files
            .get(index)
            .ok_or_else(|| AppError::message("download progress index is out of range"))?;
        let active = file
            .active
            .as_ref()
            .ok_or_else(|| AppError::message("download progress is not active"))?;
        let transfer = format_transfer(
            active.transferred,
            active
                .transferred
                .saturating_sub(active.session_start_transferred),
            active.expected_length,
            active.started_at.elapsed(),
        );
        Ok(format!(
            "[{}/{}] {state} {} {transfer}",
            index + 1,
            self.files.len(),
            file.path,
        ))
    }

    fn render_active(&mut self) -> AppResult<()> {
        if !self.interactive {
            return Ok(());
        }
        let mut block = String::new();
        let mut rendered_lines = 0;
        for (index, file) in self.files.iter().enumerate() {
            let Some(active) = file.active.as_ref() else {
                continue;
            };
            if rendered_lines > 0 {
                block.push('\n');
            }
            let state = active.state.as_str();
            let transfer = format_transfer(
                active.transferred,
                active
                    .transferred
                    .saturating_sub(active.session_start_transferred),
                active.expected_length,
                active.started_at.elapsed(),
            );
            block.push_str(&format!(
                "[{}/{}] {state} {} {transfer}",
                index + 1,
                self.files.len(),
                file.path,
            ));
            rendered_lines += 1;
        }
        self.rendered_lines = rendered_lines;
        self.write(&block)
    }

    fn clear_active(&mut self) -> AppResult<()> {
        if !self.interactive || self.rendered_lines == 0 {
            return Ok(());
        }
        let mut control = String::from("\r\x1b[2K");
        for _ in 1..self.rendered_lines {
            control.push_str("\x1b[1A\r\x1b[2K");
        }
        self.rendered_lines = 0;
        self.write(&control)
    }

    fn write_line(&mut self, line: &str) -> AppResult<()> {
        self.write(&format!("{line}\n"))
    }

    fn write(&mut self, message: &str) -> AppResult<()> {
        if self.disabled || message.is_empty() {
            return Ok(());
        }
        let result = self
            .output
            .write_all(message.as_bytes())
            .and_then(|()| self.output.flush());
        match result {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == io::ErrorKind::BrokenPipe => {
                self.disabled = true;
                Ok(())
            }
            Err(error) => {
                self.disabled = true;
                Err(AppError::io("could not write download progress", error))
            }
        }
    }
}

struct ProgressFile {
    path: String,
    active: Option<ActiveProgress>,
}

struct ActiveProgress {
    expected_length: Option<u64>,
    transferred: u64,
    session_start_transferred: u64,
    started_at: Instant,
    state: TransferState,
}

#[derive(Clone, Copy)]
enum TransferState {
    Downloading,
    Resuming,
    Syncing,
}

impl TransferState {
    fn as_str(self) -> &'static str {
        match self {
            Self::Downloading => "downloading",
            Self::Resuming => "resuming",
            Self::Syncing => "syncing",
        }
    }
}

enum DownloadEvent {
    Advanced { index: usize, transferred: u64 },
    Failed { index: usize, error: AppError },
}

struct TransferProgress<'a> {
    sender: &'a Sender<DownloadEvent>,
    index: usize,
    report_periodically: bool,
    last_rendered_at: Instant,
}

impl<'a> TransferProgress<'a> {
    fn new(sender: &'a Sender<DownloadEvent>, index: usize, report_periodically: bool) -> Self {
        Self {
            sender,
            index,
            report_periodically,
            last_rendered_at: Instant::now(),
        }
    }

    fn advance(&mut self, transferred: u64) -> AppResult<()> {
        if !self.report_periodically || self.last_rendered_at.elapsed() < PROGRESS_INTERVAL {
            return Ok(());
        }
        self.last_rendered_at = Instant::now();
        send_event(
            self.sender,
            DownloadEvent::Advanced {
                index: self.index,
                transferred,
            },
        )
    }
}

fn send_event(sender: &Sender<DownloadEvent>, event: DownloadEvent) -> AppResult<()> {
    sender
        .send(event)
        .map_err(|_| AppError::message("download progress coordinator stopped"))
}

fn format_transfer(
    transferred: u64,
    session_transferred: u64,
    expected: Option<u64>,
    elapsed: Duration,
) -> String {
    let elapsed_seconds = elapsed.as_secs_f64();
    let bytes_per_second = if session_transferred == 0 || elapsed_seconds <= 0.0 {
        None
    } else {
        Some(session_transferred as f64 / elapsed_seconds)
    };
    let rate = bytes_per_second
        .map(format_byte_value)
        .unwrap_or_else(|| "0 B".to_owned());
    match expected {
        Some(expected) => {
            let percentage = if expected == 0 {
                100.0
            } else {
                (transferred as f64 * 100.0 / expected as f64).min(100.0)
            };
            let eta = format_eta(transferred, expected, bytes_per_second);
            format!(
                "{}/{} ({percentage:.1}%) {rate}/s ETA {eta}",
                format_bytes(transferred),
                format_bytes(expected)
            )
        }
        None => format!("{} {rate}/s", format_bytes(transferred)),
    }
}

fn format_eta(transferred: u64, expected: u64, bytes_per_second: Option<f64>) -> String {
    if transferred >= expected {
        return "0s".to_owned();
    }
    let Some(bytes_per_second) = bytes_per_second.filter(|rate| *rate > 0.0 && rate.is_finite())
    else {
        return "--".to_owned();
    };
    let seconds = (expected - transferred) as f64 / bytes_per_second;
    if !seconds.is_finite() || seconds > u64::MAX as f64 {
        return "--".to_owned();
    }
    format_duration(seconds.ceil() as u64)
}

fn format_duration(seconds: u64) -> String {
    let hours = seconds / 3600;
    let minutes = seconds % 3600 / 60;
    let seconds = seconds % 60;
    if hours > 0 {
        format!("{hours}h {minutes:02}m {seconds:02}s")
    } else if minutes > 0 {
        format!("{minutes}m {seconds:02}s")
    } else {
        format!("{seconds}s")
    }
}

fn format_bytes(bytes: u64) -> String {
    format_byte_value(bytes as f64)
}

fn format_byte_value(mut bytes: f64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut unit = 0;
    while bytes >= 1024.0 && unit < UNITS.len() - 1 {
        bytes /= 1024.0;
        unit += 1;
    }
    if unit == 0 || bytes >= 100.0 {
        format!("{bytes:.0} {}", UNITS[unit])
    } else {
        format!("{bytes:.1} {}", UNITS[unit])
    }
}

fn prepare_tasks(
    client: &HubClient,
    args: &DownloadArgs,
    revision: &str,
    root: &Path,
    files: &[&TreeEntry],
) -> AppResult<Vec<DownloadTask>> {
    let mut tasks = Vec::with_capacity(files.len());
    let mut sidecar_paths = HashSet::with_capacity(files.len().saturating_mul(3));
    let repo = args.repo.to_string();
    for (index, entry) in files.iter().enumerate() {
        let relative = repository_path(&entry.path)?;
        let destination = root.join(relative);
        let parent = destination.parent().ok_or_else(|| {
            AppError::message(format!(
                "download destination has no parent: {}",
                destination.display()
            ))
        })?;
        let url = client.file_url(&args.repo, args.repo_type, revision, &entry.path)?;
        let (partial_path, metadata_path, metadata_temporary_path) =
            sidecar_paths_for(parent, args.repo_type, &repo, &entry.path);
        for path in [&partial_path, &metadata_path, &metadata_temporary_path] {
            if !sidecar_paths.insert(path.clone()) {
                return Err(AppError::message(format!(
                    "selected files produce the same download sidecar path: {}",
                    path.display()
                )));
            }
        }
        tasks.push(DownloadTask {
            index,
            remote_path: entry.path.clone(),
            destination,
            partial_path,
            metadata_path,
            metadata_temporary_path,
            url,
            expected_length: entry.size,
            repo: repo.clone(),
            repo_type: args.repo_type,
            commit: revision.to_owned(),
            oid: entry.oid.clone(),
        });
    }
    validate_task_paths(&tasks, &sidecar_paths)?;
    Ok(tasks)
}

fn prepare_task_directories(root: &Path, tasks: &[DownloadTask]) -> AppResult<()> {
    for task in tasks {
        let parent = task.destination.parent().ok_or_else(|| {
            AppError::message(format!(
                "download destination has no parent: {}",
                task.destination.display()
            ))
        })?;
        ensure_directories(root, parent)?;
    }
    Ok(())
}

fn validate_task_paths(tasks: &[DownloadTask], sidecar_paths: &HashSet<PathBuf>) -> AppResult<()> {
    let mut destination_paths = HashSet::with_capacity(tasks.len());
    for task in tasks {
        if !destination_paths.insert(task.destination.clone()) {
            return Err(AppError::message(format!(
                "selected files produce the same download destination: {}",
                task.destination.display()
            )));
        }
    }
    for task in tasks {
        if let Some(path) = task
            .destination
            .ancestors()
            .find(|path| sidecar_paths.contains(*path))
        {
            return Err(AppError::message(format!(
                "download sidecar path conflicts with a selected repository file: {}",
                path.display()
            )));
        }
        for sidecar in [
            &task.partial_path,
            &task.metadata_path,
            &task.metadata_temporary_path,
        ] {
            if let Some(path) = sidecar
                .ancestors()
                .find(|path| destination_paths.contains(*path))
            {
                return Err(AppError::message(format!(
                    "download sidecar path conflicts with a selected repository file: {}",
                    path.display()
                )));
            }
        }
        if let Some(path) = task
            .destination
            .ancestors()
            .skip(1)
            .find(|path| destination_paths.contains(*path))
        {
            return Err(AppError::message(format!(
                "selected repository file is the parent of another selected file: {}",
                path.display()
            )));
        }
    }
    Ok(())
}

fn sidecar_paths_for(
    directory: &Path,
    repo_type: RepoType,
    repo: &str,
    remote_path: &str,
) -> (PathBuf, PathBuf, PathBuf) {
    let identity = format!("{repo_type}\0{repo}\0{remote_path}");
    let stem = format!(".xhf-{:016x}", stable_hash(identity.as_bytes()));
    (
        directory.join(format!("{stem}.part")),
        directory.join(format!("{stem}.meta")),
        directory.join(format!("{stem}.meta.tmp")),
    )
}

fn stable_hash(value: &[u8]) -> u64 {
    value.iter().fold(FNV1A_64_OFFSET_BASIS, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(FNV1A_64_PRIME)
    })
}

enum MetadataRead {
    Missing,
    Invalid,
    Valid(ResumeMetadataV1),
}

fn read_metadata(path: &Path) -> AppResult<MetadataRead> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            return Err(AppError::message(format!(
                "resume metadata cannot be a symbolic link: {}",
                path.display()
            )));
        }
        Ok(metadata) if !metadata.is_file() => {
            return Err(AppError::message(format!(
                "resume metadata is not a regular file: {}",
                path.display()
            )));
        }
        Ok(_) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Ok(MetadataRead::Missing);
        }
        Err(error) => {
            return Err(AppError::io(
                format!("could not inspect resume metadata {}", path.display()),
                error,
            ));
        }
    }
    let bytes = fs::read(path).map_err(|error| {
        AppError::io(
            format!("could not read resume metadata {}", path.display()),
            error,
        )
    })?;
    Ok(match serde_json::from_slice(&bytes) {
        Ok(metadata) => MetadataRead::Valid(metadata),
        Err(_) => MetadataRead::Invalid,
    })
}

fn regular_file_length(path: &Path, description: &str) -> AppResult<Option<u64>> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => Err(AppError::message(format!(
            "{description} cannot be a symbolic link: {}",
            path.display()
        ))),
        Ok(metadata) if !metadata.is_file() => Err(AppError::message(format!(
            "{description} is not a regular file: {}",
            path.display()
        ))),
        Ok(metadata) => Ok(Some(metadata.len())),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(AppError::io(
            format!("could not inspect {description} {}", path.display()),
            error,
        )),
    }
}

fn metadata_matches(task: &DownloadTask, metadata: &ResumeMetadataV1) -> bool {
    metadata.version == RESUME_VERSION
        && metadata.repo == task.repo
        && metadata.repo_type == task.repo_type
        && metadata.commit == task.commit
        && metadata.path == task.remote_path
        && metadata.oid == task.oid
        && metadata.size == task.expected_length
        && ranges_are_valid(metadata.size, &metadata.ranges)
}

fn ranges_are_valid(size: u64, ranges: &[RangeProgress]) -> bool {
    if size == 0 {
        return ranges.is_empty();
    }
    if ranges.is_empty() {
        return false;
    }
    let mut expected_start = 0_u64;
    for range in ranges {
        let Some(after_end) = range.end.checked_add(1) else {
            return false;
        };
        if range.start != expected_start
            || range.start > range.end
            || range.end >= size
            || range.next < range.start
            || range.next > after_end
        {
            return false;
        }
        expected_start = after_end;
    }
    expected_start == size
}

fn metadata_is_complete(metadata: &ResumeMetadataV1) -> bool {
    metadata.ranges.iter().all(|range| {
        range
            .end
            .checked_add(1)
            .is_some_and(|after_end| range.next == after_end)
    })
}

fn inspect_recovered_destination(task: &DownloadTask) -> AppResult<bool> {
    let MetadataRead::Valid(metadata) = read_metadata(&task.metadata_path)? else {
        return Ok(false);
    };
    if !metadata_matches(task, &metadata) || !metadata_is_complete(&metadata) {
        return Ok(false);
    }
    if regular_file_length(&task.partial_path, "partial download")?.is_some() {
        return Ok(false);
    }
    Ok(regular_file_length(&task.destination, "download target")?
        .is_some_and(|length| length == task.expected_length))
}

fn inspect_recovered_downloads(tasks: &[DownloadTask]) -> AppResult<Vec<bool>> {
    tasks.iter().map(inspect_recovered_destination).collect()
}

fn prepare_transfers(
    tasks: &[DownloadTask],
    recovered: &[bool],
    jobs: usize,
) -> AppResult<Vec<Option<FileTransfer>>> {
    let mut transfers = Vec::with_capacity(tasks.len());
    for (task, recovered) in tasks.iter().zip(recovered.iter().copied()) {
        if recovered {
            remove_sidecar_file(&task.metadata_path, "resume metadata")?;
            remove_sidecar_file(&task.metadata_temporary_path, "temporary resume metadata")?;
            transfers.push(None);
        } else {
            transfers.push(Some(prepare_transfer(task, jobs)?));
        }
    }
    Ok(transfers)
}

fn prepare_transfer(task: &DownloadTask, jobs: usize) -> AppResult<FileTransfer> {
    remove_sidecar_file(&task.metadata_temporary_path, "temporary resume metadata")?;
    let mut metadata = match (
        read_metadata(&task.metadata_path)?,
        regular_file_length(&task.partial_path, "partial download")?,
    ) {
        (MetadataRead::Valid(metadata), Some(length))
            if length == task.expected_length && metadata_matches(task, &metadata) =>
        {
            metadata
        }
        _ => reset_resume_state(task, jobs)?,
    };
    if repartition_unfinished_ranges(&mut metadata, jobs)? {
        write_metadata_atomic(task, &metadata)?;
    }
    let transferred = transferred_bytes(&metadata)?;
    Ok(FileTransfer {
        metadata: Mutex::new(metadata),
        transferred: AtomicU64::new(transferred),
        fallback: AtomicBool::new(false),
    })
}

fn reset_resume_state(task: &DownloadTask, jobs: usize) -> AppResult<ResumeMetadataV1> {
    remove_sidecar_file(&task.metadata_path, "resume metadata")?;
    remove_sidecar_file(&task.metadata_temporary_path, "temporary resume metadata")?;
    regular_file_length(&task.partial_path, "partial download")?;
    let partial = OpenOptions::new()
        .create(true)
        .truncate(true)
        .read(true)
        .write(true)
        .open(&task.partial_path)
        .map_err(|error| {
            AppError::io(
                format!(
                    "could not create partial download {}",
                    task.partial_path.display()
                ),
                error,
            )
        })?;
    partial.set_len(task.expected_length).map_err(|error| {
        AppError::io(
            format!(
                "could not size partial download {}",
                task.partial_path.display()
            ),
            error,
        )
    })?;
    partial.sync_all().map_err(|error| {
        AppError::io(
            format!(
                "could not sync partial download {}",
                task.partial_path.display()
            ),
            error,
        )
    })?;
    let metadata = ResumeMetadataV1 {
        version: RESUME_VERSION,
        repo: task.repo.clone(),
        repo_type: task.repo_type,
        commit: task.commit.clone(),
        path: task.remote_path.clone(),
        oid: task.oid.clone(),
        size: task.expected_length,
        etag: None,
        ranges: partition_ranges(task.expected_length, jobs)?,
    };
    write_metadata_atomic(task, &metadata)?;
    Ok(metadata)
}

fn partition_ranges(size: u64, jobs: usize) -> AppResult<Vec<RangeProgress>> {
    if size == 0 {
        return Ok(Vec::new());
    }
    let jobs = u64::try_from(jobs)
        .map_err(|_| AppError::message("job count exceeds the supported range count"))?;
    let part_count = size.min(jobs);
    let capacity = usize::try_from(part_count)
        .map_err(|_| AppError::message("range count exceeds platform capacity"))?;
    let base_length = size / part_count;
    let longer_ranges = size % part_count;
    let mut ranges = Vec::with_capacity(capacity);
    let mut start = 0_u64;
    for index in 0..part_count {
        let length = base_length + u64::from(index < longer_ranges);
        let end = start
            .checked_add(length - 1)
            .ok_or_else(|| AppError::message("download range exceeds u64"))?;
        ranges.push(RangeProgress {
            start,
            next: start,
            end,
        });
        start = end
            .checked_add(1)
            .ok_or_else(|| AppError::message("download range exceeds u64"))?;
    }
    Ok(ranges)
}

fn repartition_unfinished_ranges(metadata: &mut ResumeMetadataV1, jobs: usize) -> AppResult<bool> {
    let jobs = u64::try_from(jobs)
        .map_err(|_| AppError::message("job count exceeds the supported range count"))?;
    let mut pending = Vec::new();
    let mut pending_bytes = 0_u64;
    for (index, range) in metadata.ranges.iter().enumerate() {
        let after_end = range
            .end
            .checked_add(1)
            .ok_or_else(|| AppError::message("download range exceeds u64"))?;
        if range.next < after_end {
            let length = after_end - range.next;
            pending_bytes = pending_bytes
                .checked_add(length)
                .ok_or_else(|| AppError::message("pending byte count exceeds u64"))?;
            pending.push((index, length, 1_u64));
        }
    }
    let target = jobs.min(pending_bytes);
    let pending_count = u64::try_from(pending.len())
        .map_err(|_| AppError::message("pending range count exceeds u64"))?;
    if target <= pending_count {
        return Ok(false);
    }
    let mut remaining = target - pending_count;
    for (_, length, parts) in &mut pending {
        let extra = remaining.min(*length - 1);
        *parts += extra;
        remaining -= extra;
        if remaining == 0 {
            break;
        }
    }
    let mut rebuilt = Vec::new();
    let mut pending_index = 0_usize;
    for (range_index, range) in metadata.ranges.iter().enumerate() {
        let after_end = range
            .end
            .checked_add(1)
            .ok_or_else(|| AppError::message("download range exceeds u64"))?;
        if range.next >= after_end {
            rebuilt.push(range.clone());
            continue;
        }
        if range.next > range.start {
            rebuilt.push(RangeProgress {
                start: range.start,
                next: range.next,
                end: range.next - 1,
            });
        }
        let (source_index, _, parts) = pending
            .get(pending_index)
            .copied()
            .ok_or_else(|| AppError::message("pending range disappeared while repartitioning"))?;
        if source_index != range_index {
            return Err(AppError::message(
                "pending range order changed while repartitioning",
            ));
        }
        rebuilt.extend(partition_interval(range.next, range.end, parts)?);
        pending_index += 1;
    }
    if !ranges_are_valid(metadata.size, &rebuilt) {
        return Err(AppError::message(
            "repartitioned resume ranges failed validation",
        ));
    }
    metadata.ranges = rebuilt;
    Ok(true)
}

fn partition_interval(start: u64, end: u64, parts: u64) -> AppResult<Vec<RangeProgress>> {
    let length = end
        .checked_sub(start)
        .and_then(|length| length.checked_add(1))
        .ok_or_else(|| AppError::message("download interval is invalid"))?;
    if parts == 0 || parts > length {
        return Err(AppError::message(
            "download interval partition count is invalid",
        ));
    }
    let capacity = usize::try_from(parts)
        .map_err(|_| AppError::message("range count exceeds platform capacity"))?;
    let base_length = length / parts;
    let longer_ranges = length % parts;
    let mut ranges = Vec::with_capacity(capacity);
    let mut next_start = start;
    for index in 0..parts {
        let range_length = base_length + u64::from(index < longer_ranges);
        let range_end = next_start
            .checked_add(range_length - 1)
            .ok_or_else(|| AppError::message("download range exceeds u64"))?;
        ranges.push(RangeProgress {
            start: next_start,
            next: next_start,
            end: range_end,
        });
        next_start = range_end
            .checked_add(1)
            .ok_or_else(|| AppError::message("download range exceeds u64"))?;
    }
    Ok(ranges)
}

fn transferred_bytes(metadata: &ResumeMetadataV1) -> AppResult<u64> {
    metadata.ranges.iter().try_fold(0_u64, |total, range| {
        let transferred = range
            .next
            .checked_sub(range.start)
            .ok_or_else(|| AppError::message("resume range starts after its next offset"))?;
        total
            .checked_add(transferred)
            .ok_or_else(|| AppError::message("resume byte count exceeds u64"))
    })
}

fn remove_sidecar_file(path: &Path, description: &str) -> AppResult<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => Err(AppError::message(format!(
            "{description} cannot be a symbolic link: {}",
            path.display()
        ))),
        Ok(metadata) if !metadata.is_file() => Err(AppError::message(format!(
            "{description} is not a regular file: {}",
            path.display()
        ))),
        Ok(_) => fs::remove_file(path).map_err(|error| {
            AppError::io(
                format!("could not remove {description} {}", path.display()),
                error,
            )
        }),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(AppError::io(
            format!("could not inspect {description} {}", path.display()),
            error,
        )),
    }
}

fn write_metadata_atomic(task: &DownloadTask, metadata: &ResumeMetadataV1) -> AppResult<()> {
    remove_sidecar_file(&task.metadata_temporary_path, "temporary resume metadata")?;
    let result = write_metadata_file(&task.metadata_temporary_path, metadata);
    if let Err(error) = result {
        return Err(cleanup_metadata_write(task, error));
    }
    replace_metadata_file(&task.metadata_temporary_path, &task.metadata_path)
}

fn write_metadata_file(path: &Path, metadata: &ResumeMetadataV1) -> AppResult<()> {
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
        .map_err(|error| {
            AppError::io(
                format!(
                    "could not create temporary resume metadata {}",
                    path.display()
                ),
                error,
            )
        })?;
    serde_json::to_writer(&mut file, metadata)
        .map_err(|error| AppError::json("could not encode resume metadata", error))?;
    file.write_all(b"\n").map_err(|error| {
        AppError::io(
            format!(
                "could not write temporary resume metadata {}",
                path.display()
            ),
            error,
        )
    })?;
    file.sync_all().map_err(|error| {
        AppError::io(
            format!(
                "could not sync temporary resume metadata {}",
                path.display()
            ),
            error,
        )
    })
}

fn cleanup_metadata_write(task: &DownloadTask, original: AppError) -> AppError {
    match fs::remove_file(&task.metadata_temporary_path) {
        Ok(()) => original,
        Err(error) if error.kind() == io::ErrorKind::NotFound => original,
        Err(error) => AppError::message(format!(
            "{original}; additionally could not remove temporary resume metadata {}: {error}",
            task.metadata_temporary_path.display()
        )),
    }
}

#[cfg(unix)]
fn replace_metadata_file(source: &Path, destination: &Path) -> AppResult<()> {
    fs::rename(source, destination).map_err(|error| {
        AppError::io(
            format!(
                "could not replace resume metadata {}",
                destination.display()
            ),
            error,
        )
    })
}

#[cfg(not(unix))]
fn replace_metadata_file(source: &Path, destination: &Path) -> AppResult<()> {
    remove_sidecar_file(destination, "resume metadata")?;
    fs::rename(source, destination).map_err(|error| {
        AppError::io(
            format!(
                "could not replace resume metadata {}",
                destination.display()
            ),
            error,
        )
    })
}

fn download_concurrently(
    client: &HubClient,
    tasks: &[DownloadTask],
    transfers: &[Option<FileTransfer>],
    jobs: usize,
    output: &mut dyn Write,
    progress: &mut ProgressWriter<'_>,
    report_periodically: bool,
) -> AppResult<()> {
    if tasks.len() != transfers.len() {
        return Err(AppError::message("download state length mismatch"));
    }
    let mut completed = transfers.iter().map(Option::is_none).collect::<Vec<_>>();
    let mut failed = vec![false; tasks.len()];
    let mut first_error = None;

    for (task, transfer) in tasks.iter().zip(transfers) {
        let Some(transfer) = transfer.as_ref() else {
            continue;
        };
        let transferred = transfer.transferred.load(Ordering::Acquire);
        if let Err(error) = progress.started(
            task.index,
            task.expected_length,
            transferred,
            Instant::now(),
        ) {
            record_error(&mut first_error, error);
            break;
        }
    }

    if first_error.is_none() {
        match collect_range_work(transfers) {
            Ok(work) => {
                if let Err(error) = run_work_phase(
                    client,
                    tasks,
                    transfers,
                    &work,
                    jobs,
                    progress,
                    report_periodically,
                    &mut failed,
                ) {
                    record_error(&mut first_error, error);
                }
            }
            Err(error) => record_error(&mut first_error, error),
        }
    }

    if first_error.is_none() {
        let mut full_work = Vec::new();
        for (task, transfer) in tasks.iter().zip(transfers) {
            let Some(transfer) = transfer.as_ref() else {
                continue;
            };
            if transfer.fallback.swap(false, Ordering::AcqRel) {
                match reset_for_full_download(task, transfer)
                    .and_then(|()| progress.restarted(task.index))
                {
                    Ok(()) => full_work.push(WorkItem {
                        file_index: task.index,
                        kind: WorkKind::Full,
                    }),
                    Err(error) => {
                        mark_failed(task.index, &mut failed, progress, error, &mut first_error);
                        break;
                    }
                }
            }
        }
        if first_error.is_none()
            && let Err(error) = run_work_phase(
                client,
                tasks,
                transfers,
                &full_work,
                jobs,
                progress,
                report_periodically,
                &mut failed,
            )
        {
            record_error(&mut first_error, error);
        }
    }

    for (task, transfer) in tasks.iter().zip(transfers) {
        let Some(transfer) = transfer.as_ref() else {
            continue;
        };
        if failed.get(task.index).copied().unwrap_or(true) {
            continue;
        }
        match transfer_is_complete(transfer) {
            Ok(true) => match finalize_download(task, transfer, progress) {
                Ok(()) => match completed.get_mut(task.index) {
                    Some(completed) => *completed = true,
                    None => record_error(
                        &mut first_error,
                        AppError::message("download completion index is out of range"),
                    ),
                },
                Err(error) => {
                    mark_failed(task.index, &mut failed, progress, error, &mut first_error);
                }
            },
            Ok(false) if first_error.is_none() => mark_failed(
                task.index,
                &mut failed,
                progress,
                AppError::message(format!(
                    "download workers stopped before completing {:?}",
                    task.remote_path
                )),
                &mut first_error,
            ),
            Ok(false) => {}
            Err(error) => {
                mark_failed(task.index, &mut failed, progress, error, &mut first_error);
            }
        }
    }

    for (task, completed) in tasks.iter().zip(completed.iter().copied()) {
        if !completed {
            continue;
        }
        if let Err(error) = writeln!(
            output,
            "{}",
            escape_control(&task.destination.to_string_lossy())
        ) {
            record_error(
                &mut first_error,
                AppError::io("could not write download output", error),
            );
            break;
        }
    }
    match first_error {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

fn collect_range_work(transfers: &[Option<FileTransfer>]) -> AppResult<Vec<WorkItem>> {
    let mut work = Vec::new();
    for (file_index, transfer) in transfers.iter().enumerate() {
        let Some(transfer) = transfer.as_ref() else {
            continue;
        };
        if transfer.fallback.load(Ordering::Acquire) {
            continue;
        }
        let metadata = lock_metadata(transfer)?;
        for (range_index, range) in metadata.ranges.iter().enumerate() {
            let after_end = range
                .end
                .checked_add(1)
                .ok_or_else(|| AppError::message("download range exceeds u64"))?;
            if range.next < after_end {
                work.push(WorkItem {
                    file_index,
                    kind: WorkKind::Range(range_index),
                });
            }
        }
    }
    Ok(work)
}

#[allow(clippy::too_many_arguments)]
fn run_work_phase(
    client: &HubClient,
    tasks: &[DownloadTask],
    transfers: &[Option<FileTransfer>],
    work: &[WorkItem],
    jobs: usize,
    progress: &mut ProgressWriter<'_>,
    report_periodically: bool,
    failed: &mut [bool],
) -> AppResult<()> {
    if work.is_empty() {
        return Ok(());
    }
    let worker_count = jobs.min(work.len());
    let next_work = AtomicUsize::new(0);
    let stop = AtomicBool::new(false);
    let mut first_error = None;

    thread::scope(|scope| {
        let (sender, receiver) = mpsc::channel();
        let mut handles = Vec::with_capacity(worker_count);
        for worker_index in 0..worker_count {
            let worker_sender = sender.clone();
            let next_work = &next_work;
            let stop = &stop;
            let spawn_result = thread::Builder::new()
                .name(format!("xhf-download-{}", worker_index + 1))
                .spawn_scoped(scope, move || {
                    download_worker(
                        client,
                        tasks,
                        transfers,
                        work,
                        next_work,
                        stop,
                        worker_sender,
                        report_periodically,
                    )
                });
            match spawn_result {
                Ok(handle) => handles.push(handle),
                Err(error) => {
                    stop.store(true, Ordering::Release);
                    record_error(
                        &mut first_error,
                        AppError::io("could not start download worker", error),
                    );
                    break;
                }
            }
        }
        drop(sender);

        for event in receiver {
            handle_download_event(event, progress, failed, &mut first_error, &stop);
        }

        for handle in handles {
            match handle.join() {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    stop.store(true, Ordering::Release);
                    record_error(&mut first_error, error);
                }
                Err(_) => {
                    stop.store(true, Ordering::Release);
                    record_error(
                        &mut first_error,
                        AppError::message("download worker terminated unexpectedly"),
                    );
                }
            }
        }
    });

    match first_error {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

#[allow(clippy::too_many_arguments)]
fn download_worker(
    client: &HubClient,
    tasks: &[DownloadTask],
    transfers: &[Option<FileTransfer>],
    work: &[WorkItem],
    next_work: &AtomicUsize,
    stop: &AtomicBool,
    sender: Sender<DownloadEvent>,
    report_periodically: bool,
) -> AppResult<()> {
    while !stop.load(Ordering::Acquire) {
        let index = next_work.fetch_add(1, Ordering::Relaxed);
        let Some(item) = work.get(index).copied() else {
            break;
        };
        if let Err(error) =
            perform_work(client, tasks, transfers, item, &sender, report_periodically)
        {
            stop.store(true, Ordering::Release);
            send_event(
                &sender,
                DownloadEvent::Failed {
                    index: item.file_index,
                    error,
                },
            )?;
            break;
        }
    }
    Ok(())
}

fn perform_work(
    client: &HubClient,
    tasks: &[DownloadTask],
    transfers: &[Option<FileTransfer>],
    item: WorkItem,
    sender: &Sender<DownloadEvent>,
    report_periodically: bool,
) -> AppResult<()> {
    let task = tasks
        .get(item.file_index)
        .ok_or_else(|| AppError::message("download work index is out of range"))?;
    let transfer = transfers
        .get(item.file_index)
        .and_then(Option::as_ref)
        .ok_or_else(|| AppError::message("download work has no transfer state"))?;
    let mut progress = TransferProgress::new(sender, item.file_index, report_periodically);
    match item.kind {
        WorkKind::Range(range_index) => {
            download_range(client, task, transfer, range_index, &mut progress)
        }
        WorkKind::Full => download_full(client, task, transfer, &mut progress),
    }
}

fn handle_download_event(
    event: DownloadEvent,
    progress: &mut ProgressWriter<'_>,
    failed: &mut [bool],
    first_error: &mut Option<AppError>,
    stop: &AtomicBool,
) {
    match event {
        DownloadEvent::Advanced { index, transferred } => {
            handle_progress_result(progress.advanced(index, transferred), first_error, stop)
        }
        DownloadEvent::Failed { index, error } => {
            stop.store(true, Ordering::Release);
            if let Some(failed) = failed.get_mut(index) {
                *failed = true;
            } else {
                record_error(
                    first_error,
                    AppError::message("download failure index is out of range"),
                );
            }
            let error = match progress.failed(index) {
                Ok(()) => error,
                Err(progress_error) => {
                    AppError::message(format!("{error}; additionally {progress_error}"))
                }
            };
            record_error(first_error, error);
        }
    }
}

fn mark_failed(
    index: usize,
    failed: &mut [bool],
    progress: &mut ProgressWriter<'_>,
    error: AppError,
    first_error: &mut Option<AppError>,
) {
    if let Some(failed) = failed.get_mut(index) {
        *failed = true;
    } else {
        record_error(
            first_error,
            AppError::message("download failure index is out of range"),
        );
    }
    let error = match progress.failed(index) {
        Ok(()) => error,
        Err(progress_error) => AppError::message(format!("{error}; additionally {progress_error}")),
    };
    record_error(first_error, error);
}

fn handle_progress_result(
    result: AppResult<()>,
    first_error: &mut Option<AppError>,
    stop: &AtomicBool,
) {
    if let Err(error) = result {
        stop.store(true, Ordering::Release);
        record_error(first_error, error);
    }
}

fn record_error(first_error: &mut Option<AppError>, error: AppError) {
    if first_error.is_none() {
        *first_error = Some(error);
    }
}

fn merge_results(primary: AppResult<()>, secondary: AppResult<()>) -> AppResult<()> {
    match (primary, secondary) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
        (Err(primary), Err(secondary)) => Err(AppError::message(format!(
            "{primary}; additionally {secondary}"
        ))),
    }
}

fn destination_root(configured: Option<&Path>, current_directory: &Path) -> PathBuf {
    match configured {
        Some(path) if path.is_absolute() => path.to_path_buf(),
        Some(path) => current_directory.join(path),
        None => current_directory.to_path_buf(),
    }
}

fn prepare_root(root: &Path) -> AppResult<()> {
    match fs::symlink_metadata(root) {
        Ok(metadata) if metadata.file_type().is_symlink() => Err(AppError::message(format!(
            "download directory cannot be a symbolic link: {}",
            root.display()
        ))),
        Ok(metadata) if !metadata.is_dir() => Err(AppError::message(format!(
            "download destination is not a directory: {}",
            root.display()
        ))),
        Ok(_) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            fs::create_dir_all(root).map_err(|error| {
                AppError::io(
                    format!("could not create download directory {}", root.display()),
                    error,
                )
            })
        }
        Err(error) => Err(AppError::io(
            format!("could not inspect download directory {}", root.display()),
            error,
        )),
    }
}

fn preflight(
    root: &Path,
    tasks: &[DownloadTask],
    recovered: &[bool],
    force: bool,
) -> AppResult<()> {
    if tasks.len() != recovered.len() {
        return Err(AppError::message("download recovery state length mismatch"));
    }
    for (task, recovered) in tasks.iter().zip(recovered.iter().copied()) {
        if let Some(parent) = task.destination.parent() {
            validate_parent_chain(root, parent)?;
        }
        match fs::symlink_metadata(&task.destination) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(AppError::message(format!(
                    "refusing to replace symbolic link {}",
                    task.destination.display()
                )));
            }
            Ok(metadata) if !metadata.is_file() => {
                return Err(AppError::message(format!(
                    "download target is not a regular file: {}",
                    task.destination.display()
                )));
            }
            Ok(_) if !force && !recovered => {
                return Err(AppError::message(format!(
                    "download target already exists; use --force to replace it: {}",
                    task.destination.display()
                )));
            }
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(AppError::io(
                    format!(
                        "could not inspect download target {}",
                        task.destination.display()
                    ),
                    error,
                ));
            }
        }
    }
    Ok(())
}

fn repository_path(path: &str) -> AppResult<PathBuf> {
    let mut relative = PathBuf::new();
    for component in path.split('/') {
        if component.is_empty() || component == "." || component == ".." || component.contains('\\')
        {
            return Err(AppError::message(format!(
                "unsafe repository path: {path:?}"
            )));
        }
        relative.push(component);
    }
    if relative.as_os_str().is_empty() {
        Err(AppError::message("repository path cannot be empty"))
    } else {
        Ok(relative)
    }
}

fn validate_parent_chain(root: &Path, parent: &Path) -> AppResult<()> {
    let relative = parent.strip_prefix(root).map_err(|error| {
        AppError::message(format!(
            "download path {} escapes destination {}: {error}",
            parent.display(),
            root.display()
        ))
    })?;
    let mut current = root.to_path_buf();
    for component in relative.components() {
        current.push(component.as_os_str());
        match fs::symlink_metadata(&current) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(AppError::message(format!(
                    "refusing to traverse symbolic link {}",
                    current.display()
                )));
            }
            Ok(metadata) if !metadata.is_dir() => {
                return Err(AppError::message(format!(
                    "download parent is not a directory: {}",
                    current.display()
                )));
            }
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => break,
            Err(error) => {
                return Err(AppError::io(
                    format!("could not inspect download parent {}", current.display()),
                    error,
                ));
            }
        }
    }
    Ok(())
}

fn ensure_directories(root: &Path, parent: &Path) -> AppResult<()> {
    let relative = parent.strip_prefix(root).map_err(|error| {
        AppError::message(format!(
            "download path {} escapes destination {}: {error}",
            parent.display(),
            root.display()
        ))
    })?;
    let mut current = root.to_path_buf();
    for component in relative.components() {
        current.push(component.as_os_str());
        match fs::symlink_metadata(&current) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(AppError::message(format!(
                    "refusing to traverse symbolic link {}",
                    current.display()
                )));
            }
            Ok(metadata) if !metadata.is_dir() => {
                return Err(AppError::message(format!(
                    "download parent is not a directory: {}",
                    current.display()
                )));
            }
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => fs::create_dir(&current)
                .map_err(|error| {
                    AppError::io(
                        format!("could not create download directory {}", current.display()),
                        error,
                    )
                })?,
            Err(error) => {
                return Err(AppError::io(
                    format!("could not inspect download directory {}", current.display()),
                    error,
                ));
            }
        }
    }
    Ok(())
}

fn lock_metadata(
    transfer: &FileTransfer,
) -> AppResult<std::sync::MutexGuard<'_, ResumeMetadataV1>> {
    transfer
        .metadata
        .lock()
        .map_err(|_| AppError::message("resume metadata lock is poisoned"))
}

fn if_range_validator(etag: Option<&str>) -> Option<&str> {
    etag.filter(|value| !value.starts_with("W/"))
}

fn download_range(
    client: &HubClient,
    task: &DownloadTask,
    transfer: &FileTransfer,
    range_index: usize,
    progress: &mut TransferProgress<'_>,
) -> AppResult<()> {
    if transfer.fallback.load(Ordering::Acquire) {
        return Ok(());
    }
    let (start, end, etag) = {
        let metadata = lock_metadata(transfer)?;
        let range = metadata
            .ranges
            .get(range_index)
            .ok_or_else(|| AppError::message("download range index is out of bounds"))?;
        let after_end = range
            .end
            .checked_add(1)
            .ok_or_else(|| AppError::message("download range exceeds u64"))?;
        if range.next >= after_end {
            return Ok(());
        }
        (range.next, range.end, metadata.etag.clone())
    };
    let mut response = client.file_range_response(
        task.url.clone(),
        &task.remote_path,
        start,
        end,
        if_range_validator(etag.as_deref()),
    )?;
    match response.status() {
        StatusCode::PARTIAL_CONTENT => {
            let response_etag = validate_range_response(
                &response,
                start,
                end,
                task.expected_length,
                etag.as_deref(),
                &task.remote_path,
            )?;
            write_response_range(
                &mut response,
                task,
                transfer,
                range_index,
                start,
                end,
                response_etag.as_deref(),
                Some(progress),
                true,
            )
        }
        StatusCode::OK => {
            transfer.fallback.store(true, Ordering::Release);
            Ok(())
        }
        status => Err(AppError::message(format!(
            "range request for {:?} returned unexpected successful status {status}",
            task.remote_path
        ))),
    }
}

fn download_full(
    client: &HubClient,
    task: &DownloadTask,
    transfer: &FileTransfer,
    progress: &mut TransferProgress<'_>,
) -> AppResult<()> {
    if task.expected_length == 0 {
        return Ok(());
    }
    let mut response = client.file_response_for_url(task.url.clone(), &task.remote_path)?;
    if response.status() != StatusCode::OK {
        return Err(AppError::message(format!(
            "full request for {:?} returned unexpected successful status {}",
            task.remote_path,
            response.status()
        )));
    }
    if let Some(length) = response.content_length()
        && length != task.expected_length
    {
        return Err(AppError::message(format!(
            "downloaded file {:?} has unexpected response length: expected {}, received {length}",
            task.remote_path, task.expected_length
        )));
    }
    let response_etag = response_etag(&response, &task.remote_path)?;
    write_response_range(
        &mut response,
        task,
        transfer,
        0,
        0,
        task.expected_length - 1,
        response_etag.as_deref(),
        Some(progress),
        false,
    )
}

#[allow(clippy::too_many_arguments)]
fn write_response_range(
    response: &mut reqwest::blocking::Response,
    task: &DownloadTask,
    transfer: &FileTransfer,
    range_index: usize,
    start: u64,
    end: u64,
    response_etag: Option<&str>,
    mut progress: Option<&mut TransferProgress<'_>>,
    observe_fallback: bool,
) -> AppResult<()> {
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&task.partial_path)
        .map_err(|error| {
            AppError::io(
                format!(
                    "could not open partial download {}",
                    task.partial_path.display()
                ),
                error,
            )
        })?;
    file.seek(SeekFrom::Start(start)).map_err(|error| {
        AppError::io(
            format!(
                "could not seek partial download {}",
                task.partial_path.display()
            ),
            error,
        )
    })?;
    let after_end = end
        .checked_add(1)
        .ok_or_else(|| AppError::message("download range exceeds u64"))?;
    let mut buffer = [0_u8; COPY_BUFFER_SIZE];
    let mut next = start;
    let mut checkpointed = start;
    while next < after_end {
        if observe_fallback && transfer.fallback.load(Ordering::Acquire) {
            return checkpoint_before_return(
                &file,
                task,
                transfer,
                range_index,
                next,
                response_etag,
                Ok(()),
            );
        }
        let remaining = after_end - next;
        let read_limit = usize::try_from(remaining.min(COPY_BUFFER_SIZE as u64))
            .map_err(|_| AppError::message("download read size exceeds platform capacity"))?;
        let read = match response.read(&mut buffer[..read_limit]) {
            Ok(read) => read,
            Err(error) => {
                let original = AppError::io(
                    format!("could not read downloaded file {:?}", task.remote_path),
                    error,
                );
                return checkpoint_before_return(
                    &file,
                    task,
                    transfer,
                    range_index,
                    next,
                    response_etag,
                    Err(original),
                );
            }
        };
        if read == 0 {
            let original = AppError::message(format!(
                "downloaded range for {:?} was truncated at byte {next}; expected through byte {end}",
                task.remote_path
            ));
            return checkpoint_before_return(
                &file,
                task,
                transfer,
                range_index,
                next,
                response_etag,
                Err(original),
            );
        }
        if let Err(error) = file.write_all(&buffer[..read]) {
            let original = AppError::io(
                format!("could not write downloaded file {:?}", task.remote_path),
                error,
            );
            return checkpoint_before_return(
                &file,
                task,
                transfer,
                range_index,
                next,
                response_etag,
                Err(original),
            );
        }
        let read = u64::try_from(read)
            .map_err(|_| AppError::message("download chunk length exceeds u64"))?;
        next = next
            .checked_add(read)
            .ok_or_else(|| AppError::message("downloaded byte count exceeds u64"))?;
        let previous = transfer.transferred.fetch_add(read, Ordering::AcqRel);
        let transferred = previous
            .checked_add(read)
            .ok_or_else(|| AppError::message("download progress exceeds u64"))?;
        if let Some(progress) = progress.as_deref_mut()
            && let Err(error) = progress.advance(transferred)
        {
            return checkpoint_before_return(
                &file,
                task,
                transfer,
                range_index,
                next,
                response_etag,
                Err(error),
            );
        }
        if next < after_end && next - checkpointed >= CHECKPOINT_BYTES {
            checkpoint_range(&file, task, transfer, range_index, next, response_etag)?;
            checkpointed = next;
        }
    }

    let mut extra = [0_u8; 1];
    match response.read(&mut extra) {
        Ok(0) => checkpoint_range(&file, task, transfer, range_index, next, response_etag),
        Ok(_) => {
            let original = AppError::message(format!(
                "downloaded range for {:?} exceeded requested end byte {end}",
                task.remote_path
            ));
            checkpoint_before_return(
                &file,
                task,
                transfer,
                range_index,
                checkpointed,
                response_etag,
                Err(original),
            )
        }
        Err(error) => {
            let original = AppError::io(
                format!("could not finish downloaded file {:?}", task.remote_path),
                error,
            );
            checkpoint_before_return(
                &file,
                task,
                transfer,
                range_index,
                checkpointed,
                response_etag,
                Err(original),
            )
        }
    }
}

fn checkpoint_before_return(
    file: &File,
    task: &DownloadTask,
    transfer: &FileTransfer,
    range_index: usize,
    next: u64,
    response_etag: Option<&str>,
    result: AppResult<()>,
) -> AppResult<()> {
    let checkpoint = checkpoint_range(file, task, transfer, range_index, next, response_etag);
    merge_results(result, checkpoint)
}

fn checkpoint_range(
    file: &File,
    task: &DownloadTask,
    transfer: &FileTransfer,
    range_index: usize,
    next: u64,
    response_etag: Option<&str>,
) -> AppResult<()> {
    file.sync_data().map_err(|error| {
        AppError::io(
            format!(
                "could not sync partial download {}",
                task.partial_path.display()
            ),
            error,
        )
    })?;
    let mut metadata = lock_metadata(transfer)?;
    let range = metadata
        .ranges
        .get_mut(range_index)
        .ok_or_else(|| AppError::message("download range index is out of bounds"))?;
    let after_end = range
        .end
        .checked_add(1)
        .ok_or_else(|| AppError::message("download range exceeds u64"))?;
    if next < range.next || next > after_end {
        return Err(AppError::message(format!(
            "invalid checkpoint for {:?}: byte {next} is outside {}-{}",
            task.remote_path, range.next, after_end
        )));
    }
    range.next = next;
    match (metadata.etag.as_deref(), response_etag) {
        (Some(expected), Some(received)) if expected != received => {
            return Err(AppError::message(format!(
                "download validator changed while receiving {:?}",
                task.remote_path
            )));
        }
        (None, Some(received)) => metadata.etag = Some(received.to_owned()),
        _ => {}
    }
    write_metadata_atomic(task, &metadata)
}

fn validate_range_response(
    response: &reqwest::blocking::Response,
    expected_start: u64,
    expected_end: u64,
    expected_size: u64,
    expected_etag: Option<&str>,
    remote_path: &str,
) -> AppResult<Option<String>> {
    let mut values = response.headers().get_all(CONTENT_RANGE).iter();
    let value = values.next().ok_or_else(|| {
        AppError::message(format!(
            "range response for {remote_path:?} omitted Content-Range"
        ))
    })?;
    if values.next().is_some() {
        return Err(AppError::message(format!(
            "range response for {remote_path:?} returned multiple Content-Range headers"
        )));
    }
    let value = value.to_str().map_err(|error| {
        AppError::message(format!(
            "range response for {remote_path:?} returned invalid Content-Range: {error}"
        ))
    })?;
    let (start, end, size) = parse_content_range(value)?;
    if start != expected_start || end != expected_end || size != expected_size {
        return Err(AppError::message(format!(
            "range response for {remote_path:?} returned {value:?}; expected bytes {expected_start}-{expected_end}/{expected_size}"
        )));
    }
    let expected_length = expected_end
        .checked_sub(expected_start)
        .and_then(|length| length.checked_add(1))
        .ok_or_else(|| AppError::message("download range length exceeds u64"))?;
    if let Some(length) = response.content_length()
        && length != expected_length
    {
        return Err(AppError::message(format!(
            "range response for {remote_path:?} has length {length}; expected {expected_length}"
        )));
    }
    let etag = response_etag(response, remote_path)?;
    if let (Some(expected), Some(received)) = (expected_etag, etag.as_deref())
        && expected != received
    {
        return Err(AppError::message(format!(
            "download validator changed while receiving {remote_path:?}"
        )));
    }
    Ok(etag)
}

fn parse_content_range(value: &str) -> AppResult<(u64, u64, u64)> {
    let value = value
        .strip_prefix("bytes ")
        .ok_or_else(|| AppError::message(format!("invalid Content-Range {value:?}")))?;
    let (range, size) = value
        .split_once('/')
        .ok_or_else(|| AppError::message(format!("invalid Content-Range {value:?}")))?;
    let (start, end) = range
        .split_once('-')
        .ok_or_else(|| AppError::message(format!("invalid Content-Range {value:?}")))?;
    let start = start
        .parse::<u64>()
        .map_err(|error| AppError::message(format!("invalid Content-Range start: {error}")))?;
    let end = end
        .parse::<u64>()
        .map_err(|error| AppError::message(format!("invalid Content-Range end: {error}")))?;
    let size = size
        .parse::<u64>()
        .map_err(|error| AppError::message(format!("invalid Content-Range size: {error}")))?;
    if start > end || end >= size {
        return Err(AppError::message(format!(
            "invalid Content-Range bounds: bytes {start}-{end}/{size}"
        )));
    }
    Ok((start, end, size))
}

fn response_etag(
    response: &reqwest::blocking::Response,
    remote_path: &str,
) -> AppResult<Option<String>> {
    let mut values = response.headers().get_all(ETAG).iter();
    let Some(value) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() {
        return Err(AppError::message(format!(
            "response for {remote_path:?} returned multiple ETag headers"
        )));
    }
    let value = value.to_str().map_err(|error| {
        AppError::message(format!(
            "response for {remote_path:?} returned invalid ETag: {error}"
        ))
    })?;
    Ok(Some(value.to_owned()))
}

fn reset_for_full_download(task: &DownloadTask, transfer: &FileTransfer) -> AppResult<()> {
    let metadata = reset_resume_state(task, 1)?;
    let mut current = lock_metadata(transfer)?;
    *current = metadata;
    transfer.transferred.store(0, Ordering::Release);
    transfer.fallback.store(false, Ordering::Release);
    Ok(())
}

fn transfer_is_complete(transfer: &FileTransfer) -> AppResult<bool> {
    let metadata = lock_metadata(transfer)?;
    Ok(metadata_is_complete(&metadata))
}

fn finalize_download(
    task: &DownloadTask,
    transfer: &FileTransfer,
    progress: &mut ProgressWriter<'_>,
) -> AppResult<()> {
    {
        let metadata = lock_metadata(transfer)?;
        if !metadata_is_complete(&metadata) {
            return Err(AppError::message(format!(
                "cannot finalize incomplete download {:?}",
                task.remote_path
            )));
        }
        let transferred = transferred_bytes(&metadata)?;
        if transferred != task.expected_length {
            return Err(AppError::message(format!(
                "completed download {:?} covers {transferred} bytes; expected {}",
                task.remote_path, task.expected_length
            )));
        }
    }
    let transferred = transfer.transferred.load(Ordering::Acquire);
    if transferred != task.expected_length {
        return Err(AppError::message(format!(
            "download progress for {:?} is {transferred} bytes; expected {}",
            task.remote_path, task.expected_length
        )));
    }
    let length = regular_file_length(&task.partial_path, "partial download")?
        .ok_or_else(|| AppError::message("completed partial download is missing"))?;
    if length != task.expected_length {
        return Err(AppError::message(format!(
            "partial download {:?} has length {length}; expected {}",
            task.remote_path, task.expected_length
        )));
    }
    progress.syncing(task.index, transferred)?;
    let partial = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&task.partial_path)
        .map_err(|error| {
            AppError::io(
                format!(
                    "could not open partial download {}",
                    task.partial_path.display()
                ),
                error,
            )
        })?;
    partial.sync_all().map_err(|error| {
        AppError::io(
            format!(
                "could not sync partial download {}",
                task.partial_path.display()
            ),
            error,
        )
    })?;
    drop(partial);
    replace_file(&task.partial_path, &task.destination)?;
    remove_sidecar_file(&task.metadata_path, "resume metadata")?;
    progress.finished(task.index, transferred)
}

#[cfg(unix)]
fn replace_file(source: &Path, destination: &Path) -> AppResult<()> {
    fs::rename(source, destination).map_err(|error| {
        AppError::io(
            format!("could not install download at {}", destination.display()),
            error,
        )
    })
}

#[cfg(not(unix))]
fn replace_file(source: &Path, destination: &Path) -> AppResult<()> {
    match fs::remove_file(destination) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(AppError::io(
                format!("could not replace download at {}", destination.display()),
                error,
            ));
        }
    }
    fs::rename(source, destination).map_err(|error| {
        AppError::io(
            format!("could not install download at {}", destination.display()),
            error,
        )
    })
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::io::{BufRead, BufReader, Write as _};
    use std::net::{TcpListener, TcpStream};
    use std::path::Path;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::thread::{self, JoinHandle};
    use std::time::{Duration, Instant};

    use reqwest::Url;

    use super::{
        DownloadTask, ProgressWriter, ResumeMetadataV1, execute, format_bytes, format_duration,
        format_transfer, inspect_recovered_downloads, lock_metadata, parse_content_range,
        partition_ranges, preflight, prepare_root, prepare_tasks, prepare_transfer,
        prepare_transfers, repartition_unfinished_ranges, repository_path, sidecar_paths_for,
        stable_hash, transferred_bytes, validate_parent_chain, write_metadata_atomic,
    };
    use crate::cli::{DownloadArgs, RepoType};
    use crate::hub::{HubClient, TreeEntry};

    const COMMIT: &str = "0123456789abcdef0123456789abcdef01234567";

    fn test_root() -> std::path::PathBuf {
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tmp")
    }

    fn test_directory(name: &str) -> std::path::PathBuf {
        test_root().join(format!("xhf-download-{name}-{}", std::process::id()))
    }

    fn local_task(directory: &Path, expected_length: u64) -> DownloadTask {
        let url = Url::parse(&format!(
            "https://huggingface.co/owner/repo/resolve/{COMMIT}/weights.bin"
        ))
        .unwrap();
        let destination = directory.join("weights.bin");
        let (partial_path, metadata_path, metadata_temporary_path) =
            sidecar_paths_for(directory, RepoType::Model, "owner/repo", "weights.bin");
        DownloadTask {
            index: 0,
            remote_path: "weights.bin".to_owned(),
            destination,
            partial_path,
            metadata_path,
            metadata_temporary_path,
            url,
            expected_length,
            repo: "owner/repo".to_owned(),
            repo_type: RepoType::Model,
            commit: COMMIT.to_owned(),
            oid: "oid".to_owned(),
        }
    }

    fn start_download_server() -> (String, Arc<AtomicBool>, Arc<AtomicUsize>, JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let address = listener.local_addr().unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let active = Arc::new(AtomicUsize::new(0));
        let maximum_active = Arc::new(AtomicUsize::new(0));
        let resolved = Arc::new(AtomicBool::new(false));
        let server_stop = Arc::clone(&stop);
        let server_active = Arc::clone(&active);
        let server_maximum_active = Arc::clone(&maximum_active);
        let handle = thread::spawn(move || {
            let mut connections = Vec::new();
            while !server_stop.load(Ordering::Acquire) {
                match listener.accept() {
                    Ok((stream, _)) => {
                        stream.set_nonblocking(false).unwrap();
                        let active = Arc::clone(&server_active);
                        let maximum_active = Arc::clone(&server_maximum_active);
                        let resolved = Arc::clone(&resolved);
                        connections.push(thread::spawn(move || {
                            serve_download_request(stream, active, maximum_active, resolved);
                        }));
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(2));
                    }
                    Err(error) => panic!("test server failed to accept a connection: {error}"),
                }
            }
            for connection in connections {
                connection.join().unwrap();
            }
        });
        (format!("http://{address}/"), stop, maximum_active, handle)
    }

    fn serve_download_request(
        mut stream: TcpStream,
        active: Arc<AtomicUsize>,
        maximum_active: Arc<AtomicUsize>,
        resolved: Arc<AtomicBool>,
    ) {
        let mut reader = BufReader::new(stream.try_clone().unwrap());
        let mut request_line = String::new();
        reader.read_line(&mut request_line).unwrap();
        loop {
            let mut header = String::new();
            reader.read_line(&mut header).unwrap();
            if header == "\r\n" || header.is_empty() {
                break;
            }
        }
        let path = request_line.split_whitespace().nth(1).unwrap();
        if path == "/api/models/owner/repo/revision/main?expand=sha" {
            assert!(
                !resolved.swap(true, Ordering::AcqRel),
                "revision resolved more than once"
            );
            let body = format!(r#"{{"sha":"{COMMIT}"}}"#);
            write_response(&mut stream, "application/json", body.as_bytes());
            return;
        }
        assert!(
            resolved.load(Ordering::Acquire),
            "request preceded revision resolution"
        );
        let tree_path = format!("/api/models/owner/repo/tree/{COMMIT}");
        if path == format!("{tree_path}?recursive=true&limit=1000") {
            let body = br#"[
                {"type":"file","oid":"3","size":4,"path":"c.bin"},
                {"type":"file","oid":"1","size":4,"path":"a.bin"}
            ]"#;
            write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nLink: <{tree_path}?cursor=next>; rel=\"next\"\r\nConnection: close\r\n\r\n",
                body.len()
            ).unwrap();
            stream.write_all(body).unwrap();
            return;
        }
        if path == format!("{tree_path}?cursor=next") {
            let body = br#"[
                {"type":"file","oid":"2","size":4,"path":"b.bin"}
            ]"#;
            write_response(&mut stream, "application/json", body);
            return;
        }
        if path.starts_with(&format!("/owner/repo/resolve/{COMMIT}/")) {
            let active_count = active.fetch_add(1, Ordering::AcqRel) + 1;
            maximum_active.fetch_max(active_count, Ordering::AcqRel);
            thread::sleep(Duration::from_millis(100));
            write_response(&mut stream, "application/octet-stream", b"data");
            active.fetch_sub(1, Ordering::AcqRel);
            return;
        }
        if path.starts_with("/owner/repo/resolve/main/") {
            write_response(&mut stream, "application/octet-stream", b"next");
            return;
        }
        write_response(&mut stream, "text/plain", b"not found");
    }

    fn write_response(stream: &mut TcpStream, content_type: &str, body: &[u8]) {
        write!(
            stream,
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nContent-Type: {content_type}\r\nConnection: close\r\n\r\n",
            body.len()
        )
        .unwrap();
        stream.write_all(body).unwrap();
        stream.flush().unwrap();
    }

    type RangeServer = (
        String,
        Arc<AtomicBool>,
        Arc<AtomicUsize>,
        Arc<Mutex<Vec<(u64, u64)>>>,
        JoinHandle<()>,
    );

    fn start_range_server(body: Vec<u8>, truncate_once: Option<(u64, u64)>) -> RangeServer {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let address = listener.local_addr().unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let active = Arc::new(AtomicUsize::new(0));
        let maximum_active = Arc::new(AtomicUsize::new(0));
        let ranges = Arc::new(Mutex::new(Vec::new()));
        let truncated = Arc::new(AtomicBool::new(false));
        let server_stop = Arc::clone(&stop);
        let server_active = Arc::clone(&active);
        let server_maximum_active = Arc::clone(&maximum_active);
        let server_ranges = Arc::clone(&ranges);
        let body = Arc::new(body);
        let handle = thread::spawn(move || {
            let mut connections = Vec::new();
            while !server_stop.load(Ordering::Acquire) {
                match listener.accept() {
                    Ok((stream, _)) => {
                        stream.set_nonblocking(false).unwrap();
                        let active = Arc::clone(&server_active);
                        let maximum_active = Arc::clone(&server_maximum_active);
                        let ranges = Arc::clone(&server_ranges);
                        let truncated = Arc::clone(&truncated);
                        let body = Arc::clone(&body);
                        connections.push(thread::spawn(move || {
                            serve_range_request(
                                stream,
                                &body,
                                truncate_once,
                                &truncated,
                                &active,
                                &maximum_active,
                                &ranges,
                            );
                        }));
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(2));
                    }
                    Err(error) => panic!("range server failed to accept a connection: {error}"),
                }
            }
            for connection in connections {
                connection.join().unwrap();
            }
        });
        (
            format!("http://{address}/"),
            stop,
            maximum_active,
            ranges,
            handle,
        )
    }

    fn serve_range_request(
        mut stream: TcpStream,
        body: &[u8],
        truncate_once: Option<(u64, u64)>,
        truncated: &AtomicBool,
        active: &AtomicUsize,
        maximum_active: &AtomicUsize,
        ranges: &Mutex<Vec<(u64, u64)>>,
    ) {
        let mut reader = BufReader::new(stream.try_clone().unwrap());
        let mut request_line = String::new();
        reader.read_line(&mut request_line).unwrap();
        let mut range = None;
        loop {
            let mut header = String::new();
            reader.read_line(&mut header).unwrap();
            if header == "\r\n" || header.is_empty() {
                break;
            }
            if let Some(value) = header
                .strip_prefix("range: bytes=")
                .or_else(|| header.strip_prefix("Range: bytes="))
            {
                let (start, end) = value.trim().split_once('-').unwrap();
                range = Some((start.parse::<u64>().unwrap(), end.parse::<u64>().unwrap()));
            }
        }
        let path = request_line.split_whitespace().nth(1).unwrap();
        if path == "/api/models/owner/repo/revision/main?expand=sha" {
            let body = format!(r#"{{"sha":"{COMMIT}"}}"#);
            write_response(&mut stream, "application/json", body.as_bytes());
            return;
        }
        let tree_path = format!("/api/models/owner/repo/tree/{COMMIT}");
        if path == format!("{tree_path}?recursive=true&limit=1000") {
            let tree = format!(
                r#"[{{"type":"file","oid":"weights-oid","size":{},"path":"weights.bin"}}]"#,
                body.len()
            );
            write_response(&mut stream, "application/json", tree.as_bytes());
            return;
        }
        assert_eq!(path, format!("/owner/repo/resolve/{COMMIT}/weights.bin"));
        let Some((start, end)) = range else {
            write_response(&mut stream, "application/octet-stream", body);
            return;
        };
        ranges.lock().unwrap().push((start, end));
        let active_count = active.fetch_add(1, Ordering::AcqRel) + 1;
        maximum_active.fetch_max(active_count, Ordering::AcqRel);
        if start != end {
            thread::sleep(Duration::from_millis(100));
        }
        let start_index = usize::try_from(start).unwrap();
        let end_index = usize::try_from(end).unwrap();
        let selected = &body[start_index..=end_index];
        write!(
            stream,
            "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nContent-Range: bytes {start}-{end}/{}\r\nETag: \"test-etag\"\r\nConnection: close\r\n\r\n",
            selected.len(),
            body.len()
        )
        .unwrap();
        let should_truncate =
            truncate_once == Some((start, end)) && !truncated.swap(true, Ordering::AcqRel);
        if should_truncate {
            let prefix_length = selected.len().min(4);
            stream.write_all(&selected[..prefix_length]).unwrap();
        } else {
            stream.write_all(selected).unwrap();
        }
        stream.flush().unwrap();
        active.fetch_sub(1, Ordering::AcqRel);
    }

    fn range_download_args(root: &Path, jobs: usize) -> DownloadArgs {
        DownloadArgs {
            repo: "owner/repo".parse().unwrap(),
            patterns: Vec::new(),
            repo_type: RepoType::Model,
            revision: "main".to_owned(),
            directory: Some(root.to_path_buf()),
            force: false,
            jobs,
            dry_run: false,
        }
    }

    fn stop_range_server(stop: &AtomicBool, server: JoinHandle<()>) {
        stop.store(true, Ordering::Release);
        server.join().unwrap();
    }

    #[test]
    fn converts_repository_paths_without_traversal() {
        assert_eq!(
            repository_path("folder/file.txt").unwrap(),
            std::path::Path::new("folder/file.txt")
        );
        assert!(repository_path("../file.txt").is_err());
        assert!(repository_path("folder\\file.txt").is_err());
    }

    #[test]
    fn rejects_file_in_parent_chain() {
        let root = test_directory("parent-file");
        if root.exists() {
            fs::remove_dir_all(&root).unwrap();
        }
        prepare_root(&root).unwrap();
        fs::write(root.join("parent"), b"not a directory").unwrap();
        assert!(validate_parent_chain(&root, &root.join("parent/child")).is_err());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn formats_progress_units_and_durations() {
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(1536), "1.5 KiB");
        assert_eq!(format_duration(5), "5s");
        assert_eq!(format_duration(65), "1m 05s");
        assert_eq!(format_duration(3665), "1h 01m 05s");
    }

    #[test]
    fn calculates_resumed_rate_from_current_session_bytes() {
        assert_eq!(
            format_transfer(768, 256, Some(1024), Duration::from_secs(2)),
            "768 B/1.0 KiB (75.0%) 128 B/s ETA 2s"
        );
        assert_eq!(
            format_transfer(768, 0, Some(1024), Duration::from_secs(2)),
            "768 B/1.0 KiB (75.0%) 0 B/s ETA --"
        );
    }

    #[test]
    fn derives_stable_sidecar_names_from_repository_identity() {
        assert_eq!(stable_hash(b""), 0xcbf29ce484222325);
        assert_eq!(stable_hash(b"a"), 0xaf63dc4c8601ec8c);
        let directory = test_directory("sidecar-names");
        let first = sidecar_paths_for(&directory, RepoType::Model, "o/r", "a.bin");
        let same = sidecar_paths_for(&directory, RepoType::Model, "o/r", "a.bin");
        let second = sidecar_paths_for(&directory, RepoType::Model, "o/r", "b.bin");
        assert_eq!(first, same);
        assert_ne!(first, second);
        assert_eq!(
            first.0.extension().and_then(|extension| extension.to_str()),
            Some("part")
        );
        assert_eq!(
            first.1.extension().and_then(|extension| extension.to_str()),
            Some("meta")
        );
    }

    #[test]
    fn partitions_a_file_into_balanced_contiguous_ranges() {
        assert_eq!(
            partition_ranges(12, 5).unwrap(),
            [
                super::RangeProgress {
                    start: 0,
                    next: 0,
                    end: 2,
                },
                super::RangeProgress {
                    start: 3,
                    next: 3,
                    end: 5,
                },
                super::RangeProgress {
                    start: 6,
                    next: 6,
                    end: 7,
                },
                super::RangeProgress {
                    start: 8,
                    next: 8,
                    end: 9,
                },
                super::RangeProgress {
                    start: 10,
                    next: 10,
                    end: 11,
                },
            ]
        );
        assert!(partition_ranges(0, 5).unwrap().is_empty());
        assert_eq!(partition_ranges(2, 5).unwrap().len(), 2);
    }

    #[test]
    fn resume_state_expands_pending_ranges_for_more_jobs() {
        let mut metadata = ResumeMetadataV1 {
            version: super::RESUME_VERSION,
            repo: "owner/repo".to_owned(),
            repo_type: RepoType::Model,
            commit: COMMIT.to_owned(),
            path: "weights.bin".to_owned(),
            oid: "weights-oid".to_owned(),
            size: 12,
            etag: Some("\"etag\"".to_owned()),
            ranges: vec![
                super::RangeProgress {
                    start: 0,
                    next: 3,
                    end: 5,
                },
                super::RangeProgress {
                    start: 6,
                    next: 6,
                    end: 11,
                },
            ],
        };

        assert!(repartition_unfinished_ranges(&mut metadata, 5).unwrap());
        assert_eq!(transferred_bytes(&metadata).unwrap(), 3);
        assert_eq!(
            metadata
                .ranges
                .iter()
                .filter(|range| range.next <= range.end)
                .count(),
            5
        );
        assert!(super::ranges_are_valid(metadata.size, &metadata.ranges));
    }

    #[test]
    fn rejects_partial_paths_that_conflict_with_repository_files() {
        let client = HubClient::new(None).unwrap();
        let args = DownloadArgs {
            repo: "owner/repo".parse().unwrap(),
            patterns: Vec::new(),
            repo_type: RepoType::Model,
            revision: "main".to_owned(),
            directory: None,
            force: false,
            jobs: 3,
            dry_run: false,
        };
        let (partial_path, _, _) = sidecar_paths_for(
            Path::new(""),
            args.repo_type,
            &args.repo.to_string(),
            "a.bin",
        );
        let partial_name = partial_path
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .to_owned();
        let first = TreeEntry {
            entry_type: "file".to_owned(),
            oid: "1".to_owned(),
            size: 4,
            path: "a.bin".to_owned(),
        };
        let conflict_paths = [partial_name.clone(), format!("{partial_name}/nested.bin")];
        for conflict_path in conflict_paths {
            let conflicting = TreeEntry {
                entry_type: "file".to_owned(),
                oid: "2".to_owned(),
                size: 4,
                path: conflict_path,
            };
            let files = [&first, &conflicting];
            let result = prepare_tasks(
                &client,
                &args,
                &args.revision,
                &test_directory("path-conflict"),
                &files,
            );
            assert!(result.is_err());
            assert!(
                result
                    .err()
                    .unwrap()
                    .to_string()
                    .contains("conflicts with a selected repository file")
            );
        }
    }

    #[test]
    fn reports_noninteractive_file_progress() {
        let directory = test_directory("progress");
        let mut progress_output = Vec::new();
        let tasks = [local_task(&directory, 4)];
        {
            let mut progress = ProgressWriter::new(&mut progress_output, false, &tasks);
            progress.start_batch(1, 4, &directory, 1).unwrap();
            progress.started(0, 4, 0, Instant::now()).unwrap();
            progress.advanced(0, 4).unwrap();
            progress.syncing(0, 4).unwrap();
            progress.finished(0, 4).unwrap();
            progress.finish_batch().unwrap();
        }

        let progress_output = String::from_utf8(progress_output).unwrap();
        assert!(progress_output.contains("Downloading 1 file (4 B)"));
        assert!(progress_output.contains("with up to 1 simultaneous transfer"));
        assert!(progress_output.contains("[1/1] downloading weights.bin"));
        assert!(progress_output.contains("[1/1] downloaded weights.bin"));
    }

    #[test]
    fn rejects_malformed_or_inconsistent_content_ranges() {
        assert_eq!(parse_content_range("bytes 2-4/10").unwrap(), (2, 4, 10));
        assert!(parse_content_range("items 2-4/10").is_err());
        assert!(parse_content_range("bytes 4-2/10").is_err());
        assert!(parse_content_range("bytes 2-10/10").is_err());
        assert!(parse_content_range("bytes 2-4/*").is_err());
    }

    #[test]
    fn orphan_partial_without_metadata_restarts_from_zero() {
        let root = test_directory("orphan-partial");
        if root.exists() {
            fs::remove_dir_all(&root).unwrap();
        }
        prepare_root(&root).unwrap();
        let task = local_task(&root, 12);
        fs::write(&task.partial_path, b"orphan bytes that cannot be trusted").unwrap();

        let transfer = prepare_transfer(&task, 3).unwrap();
        assert_eq!(transfer.transferred.load(Ordering::Acquire), 0);
        assert_eq!(fs::metadata(&task.partial_path).unwrap().len(), 12);
        let metadata: ResumeMetadataV1 =
            serde_json::from_slice(&fs::read(&task.metadata_path).unwrap()).unwrap();
        assert!(
            metadata
                .ranges
                .iter()
                .all(|range| range.next == range.start)
        );
        assert_eq!(lock_metadata(&transfer).unwrap().ranges.len(), 3);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn changed_file_identity_discards_persisted_offsets() {
        let root = test_directory("changed-identity");
        if root.exists() {
            fs::remove_dir_all(&root).unwrap();
        }
        prepare_root(&root).unwrap();
        let mut task = local_task(&root, 12);
        let transfer = prepare_transfer(&task, 2).unwrap();
        {
            let mut metadata = lock_metadata(&transfer).unwrap();
            metadata.ranges[0].next = 3;
            write_metadata_atomic(&task, &metadata).unwrap();
        }
        task.oid = "replacement-oid".to_owned();

        let replacement = prepare_transfer(&task, 2).unwrap();
        assert_eq!(replacement.transferred.load(Ordering::Acquire), 0);
        let metadata = lock_metadata(&replacement).unwrap();
        assert_eq!(metadata.oid, "replacement-oid");
        assert!(
            metadata
                .ranges
                .iter()
                .all(|range| range.next == range.start)
        );
        drop(metadata);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn completed_metadata_recovers_a_post_rename_crash() {
        let root = test_directory("post-rename-recovery");
        if root.exists() {
            fs::remove_dir_all(&root).unwrap();
        }
        prepare_root(&root).unwrap();
        let task = local_task(&root, 4);
        let transfer = prepare_transfer(&task, 2).unwrap();
        fs::write(&task.partial_path, b"data").unwrap();
        {
            let mut metadata = lock_metadata(&transfer).unwrap();
            for range in &mut metadata.ranges {
                range.next = range.end + 1;
            }
            write_metadata_atomic(&task, &metadata).unwrap();
        }
        fs::rename(&task.partial_path, &task.destination).unwrap();
        let tasks = [task];

        let recovered = inspect_recovered_downloads(&tasks).unwrap();
        assert_eq!(recovered, [true]);
        preflight(&root, &tasks, &recovered, false).unwrap();
        let transfers = prepare_transfers(&tasks, &recovered, 2).unwrap();
        assert!(transfers[0].is_none());
        assert!(!tasks[0].metadata_path.exists());
        assert_eq!(fs::read(&tasks[0].destination).unwrap(), b"data");
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn one_file_uses_all_jobs_for_distinct_ranges() {
        let body = (0_u8..100).collect::<Vec<_>>();
        let (endpoint, stop, maximum_active, ranges, server) =
            start_range_server(body.clone(), None);
        let client = HubClient::at_endpoint(&endpoint, None).unwrap();
        let root = test_directory("single-file-ranges");
        if root.exists() {
            fs::remove_dir_all(&root).unwrap();
        }
        let args = range_download_args(&root, 5);
        let mut output = Vec::new();
        let mut progress = Vec::new();

        let result = execute(
            &client,
            &args,
            &test_root(),
            &mut output,
            &mut progress,
            false,
        );
        stop_range_server(&stop, server);
        result.unwrap();

        assert_eq!(maximum_active.load(Ordering::Acquire), 5);
        let mut requested = ranges.lock().unwrap().clone();
        requested.sort_unstable();
        assert_eq!(requested, [(0, 19), (20, 39), (40, 59), (60, 79), (80, 99)]);
        assert_eq!(fs::read(root.join("weights.bin")).unwrap(), body);
        assert_eq!(
            String::from_utf8(output).unwrap(),
            format!("{}\n", root.join("weights.bin").display())
        );
        let mut entries = fs::read_dir(&root).unwrap();
        assert!(entries.all(|entry| {
            !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with(".xhf-")
        }));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn interrupted_ranges_continue_from_persisted_offsets() {
        let body = (0_u8..20).collect::<Vec<_>>();
        let root = test_directory("range-resume");
        if root.exists() {
            fs::remove_dir_all(&root).unwrap();
        }
        let args = range_download_args(&root, 2);
        let (first_endpoint, first_stop, _, _, first_server) =
            start_range_server(body.clone(), Some((0, 9)));
        let first_client = HubClient::at_endpoint(&first_endpoint, None).unwrap();
        let mut first_output = Vec::new();
        let mut first_progress = Vec::new();

        let first_result = execute(
            &first_client,
            &args,
            &test_root(),
            &mut first_output,
            &mut first_progress,
            false,
        );
        stop_range_server(&first_stop, first_server);
        assert!(first_result.is_err());
        assert!(first_output.is_empty());

        let (partial_path, metadata_path, _) =
            sidecar_paths_for(&root, RepoType::Model, "owner/repo", "weights.bin");
        assert!(partial_path.is_file());
        let metadata: ResumeMetadataV1 =
            serde_json::from_slice(&fs::read(&metadata_path).unwrap()).unwrap();
        assert_eq!(metadata.ranges[0].next, 4);

        let (second_endpoint, second_stop, _, second_ranges, second_server) =
            start_range_server(body.clone(), None);
        let second_client = HubClient::at_endpoint(&second_endpoint, None).unwrap();
        let mut second_output = Vec::new();
        let mut second_progress = Vec::new();
        let second_result = execute(
            &second_client,
            &args,
            &test_root(),
            &mut second_output,
            &mut second_progress,
            false,
        );
        stop_range_server(&second_stop, second_server);
        second_result.unwrap();

        let requested = second_ranges.lock().unwrap();
        assert!(requested.iter().all(|(start, _)| *start >= 4));
        assert!(requested.iter().any(|(start, _)| *start == 4));
        assert_eq!(fs::read(root.join("weights.bin")).unwrap(), body);
        assert!(!partial_path.exists());
        assert!(!metadata_path.exists());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn pins_revision_across_pages_and_downloads_with_bounded_concurrency() {
        let (endpoint, stop, maximum_active, server) = start_download_server();
        let client = HubClient::at_endpoint(&endpoint, None).unwrap();
        let root = test_directory("concurrency");
        if root.exists() {
            fs::remove_dir_all(&root).unwrap();
        }
        let args = DownloadArgs {
            repo: "owner/repo".parse().unwrap(),
            patterns: Vec::new(),
            repo_type: RepoType::Model,
            revision: "main".to_owned(),
            directory: Some(root.clone()),
            force: false,
            jobs: 2,
            dry_run: false,
        };
        let mut output = Vec::new();
        let mut progress = Vec::new();
        let result = execute(
            &client,
            &args,
            &test_root(),
            &mut output,
            &mut progress,
            false,
        );
        stop.store(true, Ordering::Release);
        server.join().unwrap();
        result.unwrap();

        assert_eq!(maximum_active.load(Ordering::Acquire), 2);
        let output = String::from_utf8(output).unwrap();
        assert_eq!(
            output.lines().collect::<Vec<_>>(),
            [
                root.join("a.bin").to_string_lossy(),
                root.join("b.bin").to_string_lossy(),
                root.join("c.bin").to_string_lossy(),
            ]
        );
        assert_eq!(fs::read(root.join("a.bin")).unwrap(), b"data");
        assert_eq!(fs::read(root.join("b.bin")).unwrap(), b"data");
        assert_eq!(fs::read(root.join("c.bin")).unwrap(), b"data");
        let progress = String::from_utf8(progress).unwrap();
        assert!(progress.contains("with up to 2 simultaneous transfers"));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn dry_run_uses_resolved_revision_without_creating_destination() {
        let (endpoint, stop, maximum_active, server) = start_download_server();
        let client = HubClient::at_endpoint(&endpoint, None).unwrap();
        let root = test_directory("pinned-dry-run");
        assert!(!root.exists());
        let args = DownloadArgs {
            repo: "owner/repo".parse().unwrap(),
            patterns: Vec::new(),
            repo_type: RepoType::Model,
            revision: "main".to_owned(),
            directory: Some(root.clone()),
            force: false,
            jobs: 2,
            dry_run: true,
        };
        let mut output = Vec::new();
        let mut progress = Vec::new();
        let result = execute(
            &client,
            &args,
            &test_root(),
            &mut output,
            &mut progress,
            false,
        );
        stop.store(true, Ordering::Release);
        server.join().unwrap();
        result.unwrap();
        assert_eq!(output, b"a.bin\nb.bin\nc.bin\n");
        assert_eq!(maximum_active.load(Ordering::Acquire), 0);
        assert!(progress.is_empty());
        assert!(!root.exists());
    }
}
