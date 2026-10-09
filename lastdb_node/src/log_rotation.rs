//! Best-effort rotation for `lastdbd` launchd stdio logs.
//!
//! Homebrew/launchd owns the actual `StandardOutPath` and
//! `StandardErrorPath` file descriptors. Renaming those paths from a helper
//! would leave the daemon writing to the renamed inode until restart, so the
//! daemon rotates the live descriptors instead: snapshot the current file into
//! numbered copies, truncate the open fd, and keep serving.

#[cfg(target_os = "macos")]
use std::ffi::CStr;
use std::fs;
use std::io;
use std::os::fd::RawFd;
use std::path::{Path, PathBuf};
use std::time::Duration;

const DEFAULT_MAX_BYTES: u64 = 10 * 1024 * 1024;
const DEFAULT_KEEP: usize = 5;
const DEFAULT_CHECK_SECONDS: u64 = 60;

#[derive(Clone, Debug)]
pub struct RotationConfig {
    pub max_bytes: u64,
    pub keep: usize,
    pub check_interval: Duration,
}

impl RotationConfig {
    fn from_env() -> Option<Self> {
        if env_is_false("LASTDBD_STDIO_LOG_ROTATION") {
            return None;
        }

        Some(Self {
            max_bytes: env_flag::var_parsed::<u64>("LASTDBD_STDIO_LOG_MAX_BYTES")
                .unwrap_or(DEFAULT_MAX_BYTES),
            keep: env_flag::var_parsed::<usize>("LASTDBD_STDIO_LOG_KEEP").unwrap_or(DEFAULT_KEEP),
            check_interval: Duration::from_secs(
                env_flag::var_parsed::<u64>("LASTDBD_STDIO_LOG_CHECK_SECONDS")
                    .unwrap_or(DEFAULT_CHECK_SECONDS)
                    .max(1),
            ),
        })
    }
}

/// Start background rotators for stdout and stderr when they point at regular
/// files. Pipes, terminals, and `/dev/null` are ignored.
pub fn spawn_stdio_log_rotators() {
    let Some(config) = RotationConfig::from_env() else {
        return;
    };
    if config.max_bytes == 0 {
        return;
    }

    for fd in [libc::STDOUT_FILENO, libc::STDERR_FILENO] {
        let Some(path) = fd_regular_file_path(fd) else {
            continue;
        };
        let config = config.clone();
        std::thread::spawn(move || loop {
            let _ = rotate_fd_if_needed(fd, &path, &config);
            std::thread::sleep(config.check_interval);
        });
    }
}

pub(crate) fn rotate_fd_if_needed(
    fd: RawFd,
    path: &Path,
    config: &RotationConfig,
) -> io::Result<bool> {
    let metadata = fs::metadata(path)?;
    if !metadata.is_file() || metadata.len() <= config.max_bytes {
        return Ok(false);
    }

    if config.keep > 0 {
        rotate_snapshots(path, config.keep)?;
        fs::copy(path, rotated_path(path, 1))?;
        fs::set_permissions(rotated_path(path, 1), metadata.permissions())?;
    }

    truncate_fd(fd)?;
    Ok(true)
}

fn rotate_snapshots(path: &Path, keep: usize) -> io::Result<()> {
    let oldest = rotated_path(path, keep);
    match fs::remove_file(&oldest) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => return Err(e),
    }

    for index in (2..=keep).rev() {
        let src = rotated_path(path, index - 1);
        if src.exists() {
            fs::rename(src, rotated_path(path, index))?;
        }
    }
    Ok(())
}

fn rotated_path(path: &Path, index: usize) -> PathBuf {
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("lastdbd.log");
    path.with_file_name(format!("{file_name}.{index}"))
}

fn truncate_fd(fd: RawFd) -> io::Result<()> {
    // SAFETY: `fd` is one of this process's stdio descriptors or a test-owned
    // descriptor. `ftruncate` and `lseek` do not take ownership of it.
    let truncated = unsafe { libc::ftruncate(fd, 0) };
    if truncated != 0 {
        return Err(io::Error::last_os_error());
    }

    // Reset the offset for descriptors not opened with O_APPEND. launchd log
    // fds normally append, but tests and manual redirections need this too.
    let seeked = unsafe { libc::lseek(fd, 0, libc::SEEK_SET) };
    if seeked < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

pub(crate) fn fd_regular_file_path(fd: RawFd) -> Option<PathBuf> {
    #[cfg(target_os = "macos")]
    if let Some(path) = fd_path_from_fcntl(fd) {
        if fs::metadata(&path).is_ok_and(|metadata| metadata.is_file()) {
            return Some(path);
        }
    }

    for candidate in [
        PathBuf::from(format!("/dev/fd/{fd}")),
        PathBuf::from(format!("/proc/self/fd/{fd}")),
    ] {
        let Ok(path) = fs::read_link(candidate) else {
            continue;
        };
        if fs::metadata(&path).is_ok_and(|metadata| metadata.is_file()) {
            return Some(path);
        }
    }
    None
}

#[cfg(target_os = "macos")]
fn fd_path_from_fcntl(fd: RawFd) -> Option<PathBuf> {
    let mut buf = [0 as libc::c_char; libc::MAXPATHLEN as usize];
    // SAFETY: `buf` is a writable MAXPATHLEN-sized buffer as required by
    // F_GETPATH. `fcntl` does not take ownership of the descriptor.
    let rc = unsafe { libc::fcntl(fd, libc::F_GETPATH, buf.as_mut_ptr()) };
    if rc != 0 {
        return None;
    }
    let c_str = unsafe { CStr::from_ptr(buf.as_ptr()) };
    Some(PathBuf::from(c_str.to_string_lossy().into_owned()))
}

fn env_is_false(name: &str) -> bool {
    matches!(
        std::env::var(name).ok().as_deref(),
        Some("0" | "false" | "FALSE" | "off" | "OFF")
    )
}
