use std::env;
use std::ffi::OsString;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process;

#[cfg(unix)]
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt};

use crate::error::{AppError, AppResult};

#[derive(Clone, Debug)]
pub struct RuntimeContext {
    pub current_directory: PathBuf,
    pub credentials: Credentials,
}

impl RuntimeContext {
    pub fn capture() -> AppResult<Self> {
        let environment = EnvironmentSnapshot::capture();
        let current_directory = env::current_dir()
            .map_err(|error| AppError::io("could not determine the current directory", error))?;
        Ok(Self {
            current_directory,
            credentials: Credentials::from_environment(&environment),
        })
    }
}

#[derive(Clone, Debug, Default)]
pub struct EnvironmentSnapshot {
    xdg_config_home: Option<OsString>,
    home: Option<OsString>,
    hf_token: Option<OsString>,
}

impl EnvironmentSnapshot {
    fn capture() -> Self {
        Self {
            xdg_config_home: env::var_os("XDG_CONFIG_HOME"),
            home: env::var_os("HOME"),
            hf_token: env::var_os("HF_TOKEN"),
        }
    }

    #[cfg(test)]
    pub fn new(
        xdg_config_home: Option<PathBuf>,
        home: Option<PathBuf>,
        hf_token: Option<String>,
    ) -> Self {
        Self {
            xdg_config_home: xdg_config_home.map(PathBuf::into_os_string),
            home: home.map(PathBuf::into_os_string),
            hf_token: hf_token.map(OsString::from),
        }
    }
}

#[derive(Clone, Debug)]
pub struct Credentials {
    token_path: Option<PathBuf>,
    environment_token: Option<OsString>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TokenSource {
    Environment,
    File,
}

impl Credentials {
    pub fn from_environment(environment: &EnvironmentSnapshot) -> Self {
        let xdg_path = environment
            .xdg_config_home
            .as_ref()
            .map(PathBuf::from)
            .filter(|path| path.is_absolute());
        let config_root = xdg_path.or_else(|| {
            environment
                .home
                .as_ref()
                .map(PathBuf::from)
                .map(|home| home.join(".config"))
        });
        Self {
            token_path: config_root.map(|root| root.join("xhf").join("token")),
            environment_token: environment
                .hf_token
                .clone()
                .filter(|token| !token.is_empty()),
        }
    }

    pub fn load(&self) -> AppResult<Option<(String, TokenSource)>> {
        if let Some(token) = self.environment_token.as_ref() {
            let token = token
                .clone()
                .into_string()
                .map_err(|_| AppError::message("HF_TOKEN is not valid UTF-8"))?;
            let token = normalize_token(token)?;
            return Ok(Some((token, TokenSource::Environment)));
        }

        let Some(path) = self.token_path.as_ref() else {
            return Ok(None);
        };
        match fs::read_to_string(path) {
            Ok(token) => Ok(Some((normalize_token(token)?, TokenSource::File))),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(AppError::io(
                format!("could not read token from {}", path.display()),
                error,
            )),
        }
    }

    pub fn store(&self, token: &str) -> AppResult<()> {
        let path = self.token_path.as_ref().ok_or_else(|| {
            AppError::message(
                "cannot store a token because neither XDG_CONFIG_HOME nor HOME is available",
            )
        })?;
        let directory = path
            .parent()
            .ok_or_else(|| AppError::message("the token path has no parent directory"))?;
        create_private_directory(directory)?;
        write_private_file(path, token.as_bytes())
    }

    pub fn remove_stored(&self) -> AppResult<bool> {
        let Some(path) = self.token_path.as_ref() else {
            return Ok(false);
        };
        match fs::remove_file(path) {
            Ok(()) => Ok(true),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(AppError::io(
                format!("could not remove stored token at {}", path.display()),
                error,
            )),
        }
    }

    pub fn environment_token_is_set(&self) -> bool {
        self.environment_token.is_some()
    }

    #[cfg(test)]
    pub fn token_path(&self) -> Option<&Path> {
        self.token_path.as_deref()
    }
}

pub fn normalize_token(token: String) -> AppResult<String> {
    let token = token.trim().to_owned();
    if token.is_empty() {
        return Err(AppError::message("Hugging Face token is empty"));
    }
    Ok(token)
}

fn create_private_directory(path: &Path) -> AppResult<()> {
    let mut builder = fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    builder.mode(0o700);
    builder
        .create(path)
        .map_err(|error| AppError::io(format!("could not create {}", path.display()), error))?;
    #[cfg(unix)]
    fs::set_permissions(path, fs::Permissions::from_mode(0o700)).map_err(|error| {
        AppError::io(
            format!("could not secure credential directory {}", path.display()),
            error,
        )
    })?;
    Ok(())
}

fn write_private_file(path: &Path, contents: &[u8]) -> AppResult<()> {
    let directory = path
        .parent()
        .ok_or_else(|| AppError::message("the token path has no parent directory"))?;
    let (temporary_path, mut file) = create_temporary_file(directory, "token")?;
    if let Err(error) = write_and_sync(&mut file, contents) {
        drop(file);
        return Err(cleanup_after_error(&temporary_path, error));
    }
    drop(file);
    replace_file(&temporary_path, path).map_err(|error| cleanup_after_error(&temporary_path, error))
}

fn create_temporary_file(directory: &Path, stem: &str) -> AppResult<(PathBuf, File)> {
    for attempt in 0..100_u16 {
        let path = directory.join(format!(".{stem}-{}-{attempt}.tmp", process::id()));
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        options.mode(0o600);
        match options.open(&path) {
            Ok(file) => return Ok((path, file)),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => {
                return Err(AppError::io(
                    format!(
                        "could not create temporary credential file in {}",
                        directory.display()
                    ),
                    error,
                ));
            }
        }
    }
    Err(AppError::message(format!(
        "could not allocate a temporary credential file in {}",
        directory.display()
    )))
}

fn write_and_sync(file: &mut File, contents: &[u8]) -> AppResult<()> {
    file.write_all(contents)
        .map_err(|error| AppError::io("could not write the temporary credential file", error))?;
    file.write_all(b"\n").map_err(|error| {
        AppError::io("could not terminate the temporary credential file", error)
    })?;
    file.sync_all()
        .map_err(|error| AppError::io("could not sync the temporary credential file", error))
}

#[cfg(unix)]
fn replace_file(source: &Path, destination: &Path) -> AppResult<()> {
    fs::rename(source, destination).map_err(|error| {
        AppError::io(
            format!(
                "could not replace credential file {}",
                destination.display()
            ),
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
                format!(
                    "could not replace credential file {}",
                    destination.display()
                ),
                error,
            ));
        }
    }
    fs::rename(source, destination).map_err(|error| {
        AppError::io(
            format!(
                "could not replace credential file {}",
                destination.display()
            ),
            error,
        )
    })
}

fn cleanup_after_error(path: &Path, original: AppError) -> AppError {
    match fs::remove_file(path) {
        Ok(()) => original,
        Err(cleanup) if cleanup.kind() == io::ErrorKind::NotFound => original,
        Err(cleanup) => AppError::message(format!(
            "{original}; additionally could not remove temporary file {}: {cleanup}",
            path.display()
        )),
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;

    use super::{Credentials, EnvironmentSnapshot, TokenSource};

    fn test_directory(name: &str) -> std::path::PathBuf {
        std::path::PathBuf::from("/tmp/agents").join(format!("xhf-{name}-{}", std::process::id()))
    }

    #[test]
    fn xdg_path_precedes_home() {
        let environment =
            EnvironmentSnapshot::new(Some("/tmp/xdg".into()), Some("/tmp/home".into()), None);
        let credentials = Credentials::from_environment(&environment);
        assert_eq!(
            credentials.token_path().unwrap(),
            std::path::Path::new("/tmp/xdg/xhf/token")
        );
    }

    #[test]
    fn environment_token_precedes_file() {
        let environment =
            EnvironmentSnapshot::new(None, Some("/tmp/home".into()), Some("from-env".to_owned()));
        let credentials = Credentials::from_environment(&environment);
        assert_eq!(
            credentials.load().unwrap(),
            Some(("from-env".to_owned(), TokenSource::Environment))
        );
    }

    #[test]
    fn empty_environment_token_is_ignored() {
        let environment = EnvironmentSnapshot::new(None, None, Some(String::new()));
        let credentials = Credentials::from_environment(&environment);
        assert_eq!(credentials.load().unwrap(), None);
        assert!(!credentials.environment_token_is_set());
    }

    #[test]
    fn stores_and_removes_token() {
        let root = test_directory("credentials");
        if root.exists() {
            fs::remove_dir_all(&root).unwrap();
        }
        let environment = EnvironmentSnapshot::new(Some(root.clone()), None, None);
        let credentials = Credentials::from_environment(&environment);

        credentials.store("secret").unwrap();
        assert_eq!(
            credentials.load().unwrap(),
            Some(("secret".to_owned(), TokenSource::File))
        );
        #[cfg(unix)]
        assert_eq!(
            fs::metadata(credentials.token_path().unwrap())
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        assert!(credentials.remove_stored().unwrap());
        assert!(!credentials.remove_stored().unwrap());
        fs::remove_dir_all(root).unwrap();
    }
}
