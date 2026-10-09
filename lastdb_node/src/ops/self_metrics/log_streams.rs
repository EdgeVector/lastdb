use super::*;

/// Resolve data-dir disclosure for a status response.
///
/// When `disclose` is true, returns the absolute path string and
/// `data_dir_path_disclosed=true`. When false, omits the path and sets the
/// flag so clients do not invent `$TMPDIR` / host guesses.
/// Where the daemon's own stdout and stderr actually land, resolved from the
/// live file descriptors rather than inferred.
///
/// The supervisor picks the destination and nothing under the node home
/// records the choice, so `~/.lastdb/logs/` holds JSONL sidecars while the
/// tracing stream goes wherever the launchd plist or the brew formula sent it
/// — `/opt/homebrew/var/log/lastdb/lastdbd.err.log` on the primary. An
/// operator who lists the data dir, finds only sidecars, and concludes the
/// tracing stream is not captured is reading a directory that cannot answer
/// the question. One measurement run was abandoned that way; see
/// `papercut-lastdb-primary-stdout-is-in-homebrew-var-log-not-lastdb-logs`,
/// whose own remedy is the `lsof` of fds 1 and 2 that this type automates.
///
/// A `None` path is an answer, not a failure: it says that stream is not a
/// regular file — a terminal in a foreground run, a supervisor pipe,
/// `/dev/null` — which is precisely what an operator hunting for a log file
/// needs to be told. Nothing here ever guesses a path.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogStreamHealth {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stdout_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stderr_path: Option<String>,
}

/// Resolve one descriptor to a path, and only when it names a regular file.
///
/// The `fstat` gate is what keeps a tty out of the status line: `F_GETPATH`
/// happily returns `/dev/ttys004` for a foreground run, and printing that
/// under a `Logs:` heading would send a reader to a device that holds no
/// history. Same reasoning as the papercut's own `lsof` recipe, which filters
/// on `REG`.
pub(super) fn regular_file_path_for_fd(fd: libc::c_int) -> Option<String> {
    // SAFETY: `st` is only read after `fstat` reports success.
    let mut st: libc::stat = unsafe { std::mem::zeroed() };
    if unsafe { libc::fstat(fd, &mut st) } != 0 {
        return None;
    }
    if st.st_mode & libc::S_IFMT != libc::S_IFREG {
        return None;
    }
    fd_path(fd)
}

#[cfg(target_os = "macos")]
pub(super) fn fd_path(fd: libc::c_int) -> Option<String> {
    let mut buf = [0 as libc::c_char; libc::PATH_MAX as usize];
    // SAFETY: `buf` is PATH_MAX bytes, which is what F_GETPATH writes into.
    if unsafe { libc::fcntl(fd, libc::F_GETPATH, buf.as_mut_ptr()) } == -1 {
        return None;
    }
    // SAFETY: F_GETPATH NUL-terminates within the buffer on success.
    let path = unsafe { std::ffi::CStr::from_ptr(buf.as_ptr()) };
    path.to_str().ok().map(str::to_string)
}

#[cfg(not(target_os = "macos"))]
pub(super) fn fd_path(fd: libc::c_int) -> Option<String> {
    std::fs::read_link(format!("/proc/self/fd/{fd}"))
        .ok()
        .map(|path| path.display().to_string())
}

/// Sample both streams. Two `fstat`s and two `fcntl`s — cheap enough for the
/// status path, and deliberately re-read per snapshot so a log rotation that
/// moves the file is visible rather than cached from boot.
pub fn daemon_log_streams() -> LogStreamHealth {
    LogStreamHealth {
        stdout_path: regular_file_path_for_fd(libc::STDOUT_FILENO),
        stderr_path: regular_file_path_for_fd(libc::STDERR_FILENO),
    }
}

/// One line naming both streams, or saying plainly that neither is a file.
///
/// stderr is called out because it is the one an operator actually wants: the
/// tracing INFO stream is on stderr, and a reader given two paths with no
/// steer opens the wrong one first.
pub(super) fn log_streams_line(logs: &LogStreamHealth) -> String {
    match (logs.stdout_path.as_deref(), logs.stderr_path.as_deref()) {
        (Some(out), Some(err)) if out == err => {
            format!("Logs: stdout+stderr -> {out} (tracing stream; not under the node home)")
        }
        (Some(out), Some(err)) => {
            format!("Logs: stderr -> {err} (tracing stream) · stdout -> {out}")
        }
        (None, Some(err)) => {
            format!("Logs: stderr -> {err} (tracing stream) · stdout is not a file")
        }
        (Some(out), None) => {
            format!("Logs: stdout -> {out} · stderr (the tracing stream) is not a file")
        }
        (None, None) => "Logs: neither stream is captured to a file — this daemon's tracing \
             output is going to a terminal or a supervisor pipe, so there is no log file to read"
            .to_string(),
    }
}

pub fn data_dir_path_for_status(data_dir: &Path, disclose: bool) -> (Option<String>, bool) {
    if !disclose {
        return (None, false);
    }
    // Prefer a canonical absolute path when the dir exists; fall back to the
    // configured path display so a pre-create status still carries something
    // usable for clients placing node-relative artifacts.
    let path = std::fs::canonicalize(data_dir)
        .unwrap_or_else(|_| data_dir.to_path_buf())
        .display()
        .to_string();
    (Some(path), true)
}
