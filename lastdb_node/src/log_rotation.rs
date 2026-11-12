//! Best-effort in-place rotation for `lastdbd` launchd stdio log descriptors.

#[cfg(target_os = "macos")]
use std::ffi::CStr;
use std::fs;
use std::io;
use std::os::fd::RawFd;
use std::path::{Path, PathBuf};
use std::time::Duration;

/// Start background rotators for stdout and stderr when they point at regular files.
pub fn spawn_stdio_log_rotators() {
    if matches!(
        std::env::var("LASTDBD_STDIO_LOG_ROTATION").ok().as_deref(),
        Some("0" | "false" | "FALSE" | "off" | "OFF")
    ) {
        return;
    }
    let max_bytes = fold_db::env_flag::var_or("LASTDBD_STDIO_LOG_MAX_BYTES", 10 * 1024 * 1024u64);
    if max_bytes == 0 {
        return;
    }
    let keep = fold_db::env_flag::var_or("LASTDBD_STDIO_LOG_KEEP", 5usize);
    let interval = Duration::from_secs(
        fold_db::env_flag::var_or("LASTDBD_STDIO_LOG_CHECK_SECONDS", 60u64).max(1),
    );
    for fd in [libc::STDOUT_FILENO, libc::STDERR_FILENO] {
        if let Some(path) = fd_regular_file_path(fd) {
            std::thread::spawn(move || loop {
                let _ = rotate_fd_if_needed(fd, &path, max_bytes, keep);
                std::thread::sleep(interval);
            });
        }
    }
}

fn rotate_fd_if_needed(fd: RawFd, path: &Path, max_bytes: u64, keep: usize) -> io::Result<()> {
    let metadata = fs::metadata(path)?;
    if !metadata.is_file() || metadata.len() <= max_bytes {
        return Ok(());
    }
    let rotated = |index: usize| {
        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("lastdbd.log");
        path.with_file_name(format!("{name}.{index}"))
    };
    if keep > 0 {
        match fs::remove_file(rotated(keep)) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
        for index in (2..=keep).rev() {
            let src = rotated(index - 1);
            if src.exists() {
                fs::rename(src, rotated(index))?;
            }
        }
        let first = rotated(1);
        fs::copy(path, &first)?;
        fs::set_permissions(first, metadata.permissions())?;
    }
    // SAFETY: `fd` is one of this process's stdio descriptors; `ftruncate` and `lseek` do not consume it.
    if unsafe { libc::ftruncate(fd, 0) } != 0 || unsafe { libc::lseek(fd, 0, libc::SEEK_SET) } < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

fn fd_regular_file_path(fd: RawFd) -> Option<PathBuf> {
    #[cfg(target_os = "macos")]
    {
        let mut buf = [0 as libc::c_char; libc::MAXPATHLEN as usize];
        // SAFETY: `buf` is a writable MAXPATHLEN buffer as required by F_GETPATH.
        if unsafe { libc::fcntl(fd, libc::F_GETPATH, buf.as_mut_ptr()) } == 0 {
            let path = PathBuf::from(
                unsafe { CStr::from_ptr(buf.as_ptr()) }
                    .to_string_lossy()
                    .into_owned(),
            );
            if fs::metadata(&path).is_ok_and(|m| m.is_file()) {
                return Some(path);
            }
        }
    }
    [format!("/dev/fd/{fd}"), format!("/proc/self/fd/{fd}")]
        .into_iter()
        .filter_map(|candidate| fs::read_link(candidate).ok())
        .find(|path| fs::metadata(path).is_ok_and(|m| m.is_file()))
}
