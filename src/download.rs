use std::collections::HashSet;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::{self, Sender};
use std::thread;
use std::time::{Duration, Instant};

use reqwest::Url;

use crate::cli::DownloadArgs;
use crate::error::{AppError, AppResult};
use crate::hub::{HubClient, TreeEntry};
use crate::patterns::PatternSet;
use crate::text::escape_control;

const COPY_BUFFER_SIZE: usize = 64 * 1024;
const PROGRESS_INTERVAL: Duration = Duration::from_millis(200);
const FNV1A_64_OFFSET_BASIS: u64 = 0xcbf29ce484222325;
const FNV1A_64_PRIME: u64 = 0x00000100000001b3;

struct DownloadTask {
    index: usize,
    remote_path: String,
    destination: PathBuf,
    temporary_path: PathBuf,
    url: Url,
    expected_length: u64,
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
    preflight(&root, &files, args.force)?;
    let total_bytes = files.iter().try_fold(0_u64, |total, entry| {
        total.checked_add(entry.size).ok_or_else(|| {
            AppError::message("selected repository files exceed the supported total size")
        })
    })?;
    let tasks = prepare_tasks(client, args, &revision, &root, &files)?;
    let worker_count = args.jobs.min(tasks.len());
    let mut progress = ProgressWriter::new(progress_output, interactive_progress, &tasks);
    progress.start_batch(tasks.len(), total_bytes, &root, worker_count)?;
    let download_result = download_concurrently(
        client,
        &tasks,
        worker_count,
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
        worker_count: usize,
    ) -> AppResult<()> {
        let noun = if file_count == 1 { "file" } else { "files" };
        let worker_noun = if worker_count == 1 {
            "worker"
        } else {
            "workers"
        };
        let root = escape_control(&root.to_string_lossy()).into_owned();
        self.write(&format!(
            "Downloading {file_count} {noun} ({}) to {root} with {worker_count} {worker_noun}\n",
            format_bytes(total_bytes),
        ))
    }

    fn started(
        &mut self,
        index: usize,
        expected_length: u64,
        started_at: Instant,
    ) -> AppResult<()> {
        self.clear_active()?;
        let file = self.progress_file_mut(index)?;
        if file.active.is_some() {
            return Err(AppError::message("download progress started twice"));
        }
        file.active = Some(ActiveProgress {
            expected_length: Some(expected_length),
            transferred: 0,
            started_at,
            state: TransferState::Downloading,
        });
        if self.interactive {
            self.render_active()
        } else {
            self.write_file_line(index, "downloading")
        }
    }

    fn set_expected_length(&mut self, index: usize, expected_length: u64) -> AppResult<()> {
        self.clear_active()?;
        self.active_progress_mut(index)?.expected_length = Some(expected_length);
        self.render_active()
    }

    fn advanced(&mut self, index: usize, transferred: u64) -> AppResult<()> {
        self.clear_active()?;
        self.active_progress_mut(index)?.transferred = transferred;
        self.render_active()
    }

    fn syncing(&mut self, index: usize, transferred: u64) -> AppResult<()> {
        self.clear_active()?;
        let active = self.active_progress_mut(index)?;
        active.transferred = transferred;
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
    started_at: Instant,
    state: TransferState,
}

#[derive(Clone, Copy)]
enum TransferState {
    Downloading,
    Syncing,
}

impl TransferState {
    fn as_str(self) -> &'static str {
        match self {
            Self::Downloading => "downloading",
            Self::Syncing => "syncing",
        }
    }
}

enum DownloadEvent {
    Started {
        index: usize,
        expected_length: u64,
        started_at: Instant,
    },
    ExpectedLength {
        index: usize,
        expected_length: u64,
    },
    Advanced {
        index: usize,
        transferred: u64,
    },
    Syncing {
        index: usize,
        transferred: u64,
    },
    Finished {
        index: usize,
        transferred: u64,
    },
    Failed {
        index: usize,
        error: AppError,
    },
}

struct TransferProgress<'a> {
    sender: &'a Sender<DownloadEvent>,
    index: usize,
    expected_length: u64,
    report_periodically: bool,
    started_at: Instant,
    last_rendered_at: Instant,
}

impl<'a> TransferProgress<'a> {
    fn new(
        sender: &'a Sender<DownloadEvent>,
        index: usize,
        expected_length: u64,
        report_periodically: bool,
    ) -> Self {
        let now = Instant::now();
        Self {
            sender,
            index,
            expected_length,
            report_periodically,
            started_at: now,
            last_rendered_at: now,
        }
    }

    fn set_expected_length(&self, expected_length: Option<u64>) -> AppResult<()> {
        if let Some(expected_length) = expected_length {
            send_event(
                self.sender,
                DownloadEvent::ExpectedLength {
                    index: self.index,
                    expected_length,
                },
            )?;
        }
        Ok(())
    }

    fn start(&mut self) -> AppResult<()> {
        self.started_at = Instant::now();
        self.last_rendered_at = self.started_at;
        send_event(
            self.sender,
            DownloadEvent::Started {
                index: self.index,
                expected_length: self.expected_length,
                started_at: self.started_at,
            },
        )
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

    fn syncing(&self, transferred: u64) -> AppResult<()> {
        if !self.report_periodically {
            return Ok(());
        }
        send_event(
            self.sender,
            DownloadEvent::Syncing {
                index: self.index,
                transferred,
            },
        )
    }

    fn finish(&self, transferred: u64) -> AppResult<()> {
        send_event(
            self.sender,
            DownloadEvent::Finished {
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

fn format_transfer(transferred: u64, expected: Option<u64>, elapsed: Duration) -> String {
    let elapsed_seconds = elapsed.as_secs_f64();
    let bytes_per_second = if transferred == 0 || elapsed_seconds <= 0.0 {
        None
    } else {
        Some(transferred as f64 / elapsed_seconds)
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
    let mut temporary_paths = HashSet::with_capacity(files.len());
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
        let temporary_path = temporary_path(parent, &url);
        if !temporary_paths.insert(temporary_path.clone()) {
            return Err(AppError::message(format!(
                "selected files produce the same temporary download path: {}",
                temporary_path.display()
            )));
        }
        tasks.push(DownloadTask {
            index,
            remote_path: entry.path.clone(),
            destination,
            temporary_path,
            url,
            expected_length: entry.size,
        });
    }
    validate_task_paths(&tasks, &temporary_paths)?;
    for task in &tasks {
        preflight_temporary_file(&task.temporary_path)?;
    }
    for task in &tasks {
        let parent = task.destination.parent().ok_or_else(|| {
            AppError::message(format!(
                "download destination has no parent: {}",
                task.destination.display()
            ))
        })?;
        ensure_directories(root, parent)?;
    }
    Ok(tasks)
}

fn validate_task_paths(
    tasks: &[DownloadTask],
    temporary_paths: &HashSet<PathBuf>,
) -> AppResult<()> {
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
            .find(|path| temporary_paths.contains(*path))
        {
            return Err(AppError::message(format!(
                "temporary download path conflicts with a selected repository file: {}",
                path.display()
            )));
        }
        if let Some(path) = task
            .temporary_path
            .ancestors()
            .find(|path| destination_paths.contains(*path))
        {
            return Err(AppError::message(format!(
                "temporary download path conflicts with a selected repository file: {}",
                path.display()
            )));
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

fn temporary_path(directory: &Path, url: &Url) -> PathBuf {
    directory.join(format!(".xhf-{:016x}.part", stable_url_hash(url.as_str())))
}

fn stable_url_hash(url: &str) -> u64 {
    url.as_bytes()
        .iter()
        .fold(FNV1A_64_OFFSET_BASIS, |hash, byte| {
            (hash ^ u64::from(*byte)).wrapping_mul(FNV1A_64_PRIME)
        })
}

fn preflight_temporary_file(path: &Path) -> AppResult<()> {
    match fs::symlink_metadata(path) {
        Ok(_) => Err(AppError::message(format!(
            "partial download already exists: {}; remove it before retrying",
            path.display()
        ))),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(AppError::io(
            format!("could not inspect partial download {}", path.display()),
            error,
        )),
    }
}

fn download_concurrently(
    client: &HubClient,
    tasks: &[DownloadTask],
    worker_count: usize,
    output: &mut dyn Write,
    progress: &mut ProgressWriter<'_>,
    report_periodically: bool,
) -> AppResult<()> {
    let next_task = AtomicUsize::new(0);
    let stop = AtomicBool::new(false);
    let mut completed = vec![false; tasks.len()];
    let mut first_error = None;

    thread::scope(|scope| {
        let (sender, receiver) = mpsc::channel();
        let mut handles = Vec::with_capacity(worker_count);
        for worker_index in 0..worker_count {
            let worker_sender = sender.clone();
            let next_task = &next_task;
            let stop = &stop;
            let spawn_result = thread::Builder::new()
                .name(format!("xhf-download-{}", worker_index + 1))
                .spawn_scoped(scope, move || {
                    download_worker(
                        client,
                        tasks,
                        next_task,
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
            handle_download_event(event, progress, &mut completed, &mut first_error, &stop);
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

    if first_error.is_none() && completed.iter().any(|completed| !completed) {
        record_error(
            &mut first_error,
            AppError::message("download workers stopped before completing all files"),
        );
    }
    for (task, completed) in tasks.iter().zip(completed) {
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

fn download_worker(
    client: &HubClient,
    tasks: &[DownloadTask],
    next_task: &AtomicUsize,
    stop: &AtomicBool,
    sender: Sender<DownloadEvent>,
    report_periodically: bool,
) -> AppResult<()> {
    loop {
        if stop.load(Ordering::Acquire) {
            return Ok(());
        }
        let index = next_task.fetch_add(1, Ordering::Relaxed);
        let Some(task) = tasks.get(index) else {
            return Ok(());
        };
        if stop.load(Ordering::Acquire) {
            return Ok(());
        }
        if let Err(error) = download_file(client, task, &sender, report_periodically) {
            stop.store(true, Ordering::Release);
            send_event(
                &sender,
                DownloadEvent::Failed {
                    index: task.index,
                    error,
                },
            )?;
            return Ok(());
        }
    }
}

fn handle_download_event(
    event: DownloadEvent,
    progress: &mut ProgressWriter<'_>,
    completed: &mut [bool],
    first_error: &mut Option<AppError>,
    stop: &AtomicBool,
) {
    match event {
        DownloadEvent::Started {
            index,
            expected_length,
            started_at,
        } => handle_progress_result(
            progress.started(index, expected_length, started_at),
            first_error,
            stop,
        ),
        DownloadEvent::ExpectedLength {
            index,
            expected_length,
        } => handle_progress_result(
            progress.set_expected_length(index, expected_length),
            first_error,
            stop,
        ),
        DownloadEvent::Advanced { index, transferred } => {
            handle_progress_result(progress.advanced(index, transferred), first_error, stop)
        }
        DownloadEvent::Syncing { index, transferred } => {
            handle_progress_result(progress.syncing(index, transferred), first_error, stop)
        }
        DownloadEvent::Finished { index, transferred } => {
            match completed.get_mut(index) {
                Some(completed) => *completed = true,
                None => {
                    stop.store(true, Ordering::Release);
                    record_error(
                        first_error,
                        AppError::message("download completion index is out of range"),
                    );
                }
            }
            handle_progress_result(progress.finished(index, transferred), first_error, stop);
        }
        DownloadEvent::Failed { index, error } => {
            stop.store(true, Ordering::Release);
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

fn preflight(root: &Path, files: &[&TreeEntry], force: bool) -> AppResult<()> {
    for entry in files {
        let relative = repository_path(&entry.path)?;
        let destination = root.join(&relative);
        if let Some(parent) = destination.parent() {
            validate_parent_chain(root, parent)?;
        }
        match fs::symlink_metadata(&destination) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(AppError::message(format!(
                    "refusing to replace symbolic link {}",
                    destination.display()
                )));
            }
            Ok(metadata) if !metadata.is_file() => {
                return Err(AppError::message(format!(
                    "download target is not a regular file: {}",
                    destination.display()
                )));
            }
            Ok(_) if !force => {
                return Err(AppError::message(format!(
                    "download target already exists; use --force to replace it: {}",
                    destination.display()
                )));
            }
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(AppError::io(
                    format!(
                        "could not inspect download target {}",
                        destination.display()
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

fn download_file(
    client: &HubClient,
    task: &DownloadTask,
    sender: &Sender<DownloadEvent>,
    report_periodically: bool,
) -> AppResult<()> {
    let mut progress = TransferProgress::new(
        sender,
        task.index,
        task.expected_length,
        report_periodically,
    );
    progress.start()?;
    let mut response = client.file_response_for_url(task.url.clone(), &task.remote_path)?;
    let expected_length = response.content_length();
    progress.set_expected_length(expected_length)?;
    let mut file = create_temporary_file(&task.temporary_path)?;
    let copied = match copy_and_sync(
        &mut response,
        &mut file,
        expected_length,
        &task.remote_path,
        &mut progress,
    ) {
        Ok(copied) => copied,
        Err(error) => {
            drop(file);
            return Err(cleanup_after_error(&task.temporary_path, error));
        }
    };
    drop(file);
    if let Err(error) = replace_file(&task.temporary_path, &task.destination) {
        return Err(cleanup_after_error(&task.temporary_path, error));
    }
    progress.finish(copied)
}

fn create_temporary_file(path: &Path) -> AppResult<File> {
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|error| {
            let context = if error.kind() == io::ErrorKind::AlreadyExists {
                format!(
                    "partial download already exists: {}; remove it before retrying",
                    path.display()
                )
            } else {
                format!("could not create partial download {}", path.display())
            };
            AppError::io(context, error)
        })
}

fn copy_and_sync(
    response: &mut reqwest::blocking::Response,
    file: &mut File,
    expected_length: Option<u64>,
    remote_path: &str,
    progress: &mut TransferProgress<'_>,
) -> AppResult<u64> {
    let copied = copy_stream(response, file, remote_path, progress)?;
    if let Some(expected) = expected_length
        && copied != expected
    {
        return Err(AppError::message(format!(
            "downloaded file {remote_path:?} was truncated: expected {expected} bytes, received {copied}"
        )));
    }
    progress.syncing(copied)?;
    file.sync_all().map_err(|error| {
        AppError::io(
            format!("could not sync downloaded file {remote_path:?}"),
            error,
        )
    })?;
    Ok(copied)
}

fn copy_stream<R: io::Read + ?Sized, W: Write + ?Sized>(
    response: &mut R,
    file: &mut W,
    remote_path: &str,
    progress: &mut TransferProgress<'_>,
) -> AppResult<u64> {
    let mut buffer = [0_u8; COPY_BUFFER_SIZE];
    let mut copied = 0_u64;
    loop {
        let read = response.read(&mut buffer).map_err(|error| {
            AppError::io(
                format!("could not read downloaded file {remote_path:?}"),
                error,
            )
        })?;
        if read == 0 {
            break;
        }
        file.write_all(&buffer[..read]).map_err(|error| {
            AppError::io(
                format!("could not write downloaded file {remote_path:?}"),
                error,
            )
        })?;
        let read = u64::try_from(read)
            .map_err(|_| AppError::message("download chunk length exceeds u64"))?;
        copied = copied
            .checked_add(read)
            .ok_or_else(|| AppError::message("downloaded byte count exceeds u64"))?;
        progress.advance(copied)?;
    }
    Ok(copied)
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

fn cleanup_after_error(path: &Path, original: AppError) -> AppError {
    match fs::remove_file(path) {
        Ok(()) => original,
        Err(cleanup) if cleanup.kind() == io::ErrorKind::NotFound => original,
        Err(cleanup) => AppError::message(format!(
            "{original}; additionally could not remove partial download {}: {cleanup}",
            path.display()
        )),
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::io::{BufRead, BufReader, Cursor, Write as _};
    use std::net::{TcpListener, TcpStream};
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::mpsc;
    use std::thread::{self, JoinHandle};
    use std::time::Duration;

    use reqwest::Url;

    use super::{
        DownloadTask, ProgressWriter, TransferProgress, copy_stream, create_temporary_file,
        execute, format_bytes, format_duration, handle_download_event, prepare_root, prepare_tasks,
        repository_path, stable_url_hash, temporary_path, validate_parent_chain,
    };
    use crate::cli::{DownloadArgs, RepoType};
    use crate::hub::{HubClient, TreeEntry};

    const COMMIT: &str = "0123456789abcdef0123456789abcdef01234567";

    fn test_directory(name: &str) -> std::path::PathBuf {
        std::path::PathBuf::from("/tmp/agents")
            .join(format!("xhf-download-{name}-{}", std::process::id()))
    }

    fn progress_task() -> DownloadTask {
        let url = Url::parse("https://huggingface.co/owner/repo/resolve/main/weights.bin").unwrap();
        let destination = PathBuf::from("/tmp/agents/model/weights.bin");
        DownloadTask {
            index: 0,
            remote_path: "weights.bin".to_owned(),
            temporary_path: temporary_path(Path::new("/tmp/agents/model"), &url),
            destination,
            url,
            expected_length: 4,
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
    fn derives_stable_partial_names_from_urls() {
        assert_eq!(stable_url_hash(""), 0xcbf29ce484222325);
        assert_eq!(stable_url_hash("a"), 0xaf63dc4c8601ec8c);
        let first = Url::parse("https://huggingface.co/o/r/resolve/main/a.bin").unwrap();
        let second = Url::parse("https://huggingface.co/o/r/resolve/main/b.bin").unwrap();
        let directory = Path::new("/tmp/agents/model");
        assert_eq!(
            temporary_path(directory, &first),
            temporary_path(directory, &first)
        );
        assert_ne!(
            temporary_path(directory, &first),
            temporary_path(directory, &second)
        );
    }

    #[test]
    fn refuses_to_replace_existing_partial_download() {
        let root = test_directory("existing-partial");
        if root.exists() {
            fs::remove_dir_all(&root).unwrap();
        }
        prepare_root(&root).unwrap();
        let url = Url::parse("https://huggingface.co/o/r/resolve/main/a.bin").unwrap();
        let partial = temporary_path(&root, &url);
        fs::write(&partial, b"partial").unwrap();
        let error = create_temporary_file(&partial).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("partial download already exists")
        );
        fs::remove_dir_all(root).unwrap();
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
        let first_url = client
            .file_url(&args.repo, args.repo_type, &args.revision, "a.bin")
            .unwrap();
        let partial_name = temporary_path(Path::new(""), &first_url)
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
                Path::new("/tmp/agents/model"),
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
        let mut progress_output = Vec::new();
        let mut downloaded = Vec::new();
        let tasks = [progress_task()];
        let (sender, receiver) = mpsc::channel();
        {
            let mut file_progress = TransferProgress::new(&sender, 0, 4, false);
            file_progress.start().unwrap();
            let mut source = Cursor::new(b"data");
            let copied = copy_stream(
                &mut source,
                &mut downloaded,
                "weights.bin",
                &mut file_progress,
            )
            .unwrap();
            file_progress.finish(copied).unwrap();
        }
        drop(sender);
        let stop = AtomicBool::new(false);
        let mut completed = vec![false];
        let mut first_error = None;
        {
            let mut progress = ProgressWriter::new(&mut progress_output, false, &tasks);
            progress
                .start_batch(1, 4, Path::new("/tmp/agents/model"), 1)
                .unwrap();
            for event in receiver {
                handle_download_event(
                    event,
                    &mut progress,
                    &mut completed,
                    &mut first_error,
                    &stop,
                );
            }
            progress.finish_batch().unwrap();
        }

        assert_eq!(downloaded, b"data");
        assert!(completed[0]);
        assert!(first_error.is_none());
        let progress_output = String::from_utf8(progress_output).unwrap();
        assert!(progress_output.contains("Downloading 1 file (4 B)"));
        assert!(progress_output.contains("with 1 worker"));
        assert!(progress_output.contains("[1/1] downloading weights.bin"));
        assert!(progress_output.contains("[1/1] downloaded weights.bin"));
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
            Path::new("/tmp/agents"),
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
        assert!(progress.contains("with 2 workers"));
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
            Path::new("/tmp/agents"),
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
