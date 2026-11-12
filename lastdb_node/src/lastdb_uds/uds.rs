//! Unix-domain-socket bind, stale-socket cleanup, peer-credential gate, and accept loop.

use std::io;
use std::os::unix::fs::{FileTypeExt, PermissionsExt};
use std::os::unix::io::{AsRawFd, RawFd};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use fold_db::access::peer_cred::read_peer_credential;
use fold_db::access::PeerCredVerdict;

/// File name of the node's Unix-domain control socket within its data dir.
pub const SOCKET_FILE_NAME: &str = "folddb.sock";

const SOCKET_TMP_SUFFIX: &str = ".tmp";
const SOCKET_MODE: u32 = 0o600;
const MAX_SOCKET_PATH_LEN: usize = 104 - 1;
const ACCEPT_POLL_INTERVAL: Duration = Duration::from_millis(50);
const FD_EXHAUSTION_MAX_BACKOFF: Duration = Duration::from_secs(1);
const FD_EXHAUSTION_SUMMARY_INTERVAL: Duration = Duration::from_secs(30);

/// Derive `<data_dir>/folddb.sock`.
pub fn socket_path(data_dir: &Path) -> PathBuf {
    data_dir.join(SOCKET_FILE_NAME)
}

fn check_len(path: &Path) -> io::Result<()> {
    let len = path.as_os_str().len();
    (len <= MAX_SOCKET_PATH_LEN).then_some(()).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "socket path is {len} bytes, exceeds the {MAX_SOCKET_PATH_LEN}-byte sockaddr_un limit"
            ),
        )
    })
}

fn tmp_socket_path(path: &Path) -> PathBuf {
    let name = path
        .file_name()
        .map_or_else(|| SOCKET_FILE_NAME.into(), |n| n.to_string_lossy());
    path.with_file_name(format!("{name}{SOCKET_TMP_SUFFIX}"))
}

/// Reject a data dir whose socket cannot fit `sockaddr_un` before boot work runs.
pub fn preflight_data_dir(data_dir: &Path) -> io::Result<()> {
    let worst = socket_path(data_dir);
    if tmp_socket_path(&worst).as_os_str().len() <= MAX_SOCKET_PATH_LEN {
        return Ok(());
    }
    let max_dir =
        MAX_SOCKET_PATH_LEN.saturating_sub(SOCKET_FILE_NAME.len() + SOCKET_TMP_SUFFIX.len() + 1);
    Err(io::Error::new(
        io::ErrorKind::InvalidInput,
        format!(
            "data dir {} is too deep to host a Unix control socket: its socket path {} is {} bytes, \
             and binding needs {} more for the atomic temp sibling, over the {MAX_SOCKET_PATH_LEN}-byte \
             sockaddr_un limit. Use a data dir at most {max_dir} bytes long.",
            data_dir.display(),
            worst.display(),
            worst.as_os_str().len(),
            SOCKET_TMP_SUFFIX.len(),
        ),
    ))
}

fn remove_stale_socket(path: &Path) -> io::Result<()> {
    match std::fs::symlink_metadata(path) {
        Ok(meta) if meta.file_type().is_socket() => std::fs::remove_file(path),
        Ok(_) => Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            format!(
                "refusing to remove non-socket file at socket path {}",
                path.display()
            ),
        )),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}

/// Bound Unix-domain control socket (`0o600`), unlinked on drop.
#[derive(Debug)]
pub struct UdsSocket {
    path: PathBuf,
    listener: UnixListener,
}

impl UdsSocket {
    /// Bind a control socket at `path` via an owner-only temp sibling and atomic rename.
    pub fn bind_at(path: impl Into<PathBuf>) -> io::Result<Self> {
        let path = path.into();
        let tmp_path = tmp_socket_path(&path);
        check_len(&path)?;
        check_len(&tmp_path)?;
        if UnixStream::connect(&path).is_ok() {
            return Err(io::Error::new(
                io::ErrorKind::AddrInUse,
                format!(
                    "a live node is already listening on {} — refusing to take over its socket",
                    path.display()
                ),
            ));
        }
        remove_stale_socket(&tmp_path)?;
        remove_stale_socket(&path)?;
        let listener = UnixListener::bind(&tmp_path)?;
        if let Err(e) =
            std::fs::set_permissions(&tmp_path, std::fs::Permissions::from_mode(SOCKET_MODE))
                .and_then(|()| std::fs::rename(&tmp_path, &path))
        {
            let _ = std::fs::remove_file(&tmp_path);
            return Err(e);
        }
        Ok(Self { path, listener })
    }

    /// Path of the bound socket file.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Run the control-socket accept loop until `shutdown` is set.
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    pub fn serve<F: FnMut(UnixStream)>(
        &self,
        owner_uid: u32,
        shutdown: &AtomicBool,
        mut on_accept: F,
    ) -> io::Result<()> {
        self.listener.set_nonblocking(true)?;
        let mut fd_episode = FdExhaustionEpisode::default();
        while !shutdown.load(Ordering::Relaxed) {
            let (stream, _) = match self.listener.accept() {
                Ok(conn) => {
                    fd_episode.on_recovered(Instant::now());
                    conn
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    wait_for_pending_connection(self.listener.as_raw_fd(), ACCEPT_POLL_INTERVAL);
                    continue;
                }
                Err(e) if matches!(e.raw_os_error(), Some(libc::EMFILE | libc::ENFILE)) => {
                    std::thread::sleep(fd_episode.on_failure(&e, Instant::now()));
                    continue;
                }
                Err(e) => return Err(e),
            };
            if let Err(e) = stream.set_nonblocking(false) {
                tracing::warn!(error = %e, "dropping control-socket connection: could not restore blocking mode");
                continue;
            }
            match read_peer_credential(stream.as_raw_fd()).map(|c| c.evaluate(owner_uid)) {
                Ok(PeerCredVerdict::SameUser { .. }) => on_accept(stream),
                Ok(PeerCredVerdict::ForeignUser {
                    peer_uid,
                    owner_uid,
                }) => tracing::warn!(
                    peer_uid,
                    owner_uid,
                    "dropping control-socket connection from a foreign OS user"
                ),
                Err(e) => tracing::warn!(
                    error = %e, "dropping control-socket connection: peer-credential read failed"
                ),
            }
        }
        Ok(())
    }
}

impl Drop for UdsSocket {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

fn wait_for_pending_connection(fd: RawFd, timeout: Duration) {
    let mut pending = libc::pollfd {
        fd,
        events: libc::POLLIN,
        revents: 0,
    };
    let timeout_ms = i32::try_from(timeout.as_millis()).unwrap_or(i32::MAX);
    // SAFETY: `pending` is a valid single-element `pollfd` on this stack frame.
    unsafe {
        libc::poll(&raw mut pending, 1, timeout_ms);
    }
}

#[derive(Default)]
struct FdExhaustionEpisode {
    started_at: Option<Instant>,
    last_reported_at: Option<Instant>,
    attempts: u64,
}

impl FdExhaustionEpisode {
    fn on_failure(&mut self, error: &io::Error, now: Instant) -> Duration {
        self.attempts = self.attempts.saturating_add(1);
        let started = *self.started_at.get_or_insert(now);
        let shift = u32::try_from(self.attempts - 1)
            .unwrap_or(u32::MAX)
            .min(u32::BITS - 1);
        let backoff = ACCEPT_POLL_INTERVAL
            .saturating_mul(1_u32 << shift)
            .min(FD_EXHAUSTION_MAX_BACKOFF);
        let backoff_ms = u64::try_from(backoff.as_millis()).unwrap_or(u64::MAX);
        if self.attempts == 1 {
            self.last_reported_at = Some(now);
            tracing::error!(
                error = %error, backoff_ms,
                "control-socket accept hit fd exhaustion; backing off and keeping listener alive"
            );
        } else if self
            .last_reported_at
            .is_none_or(|last| now.duration_since(last) >= FD_EXHAUSTION_SUMMARY_INTERVAL)
        {
            self.last_reported_at = Some(now);
            tracing::warn!(
                error = %error, failed_accepts = self.attempts,
                elapsed_ms = u64::try_from(now.duration_since(started).as_millis()).unwrap_or(u64::MAX),
                backoff_ms, "control-socket accept still out of file descriptors; listener alive"
            );
        }
        backoff
    }

    fn on_recovered(&mut self, now: Instant) {
        if let Some(started) = self.started_at.take() {
            let attempts = std::mem::take(&mut self.attempts);
            self.last_reported_at = None;
            tracing::warn!(
                failed_accepts = attempts,
                elapsed_ms =
                    u64::try_from(now.duration_since(started).as_millis()).unwrap_or(u64::MAX),
                "control-socket accept succeeded again after fd exhaustion"
            );
        }
    }
}
