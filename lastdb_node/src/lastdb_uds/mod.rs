//! Unix-domain-socket transport, bind, peer-credential gate, and routing for `lastdbd`.

use std::io;
use std::os::unix::fs::{FileTypeExt, PermissionsExt};
use std::os::unix::io::AsRawFd;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use fold_db::access::peer_cred::read_peer_credential;
use fold_db::access::PeerCredVerdict;

pub mod uds_http;
pub mod uds_router;
pub mod worker_pool;

pub use worker_pool::UdsWorkerPool;

pub mod uds {
    pub use super::{preflight_data_dir, socket_path, UdsSocket, SOCKET_FILE_NAME};
}

pub mod database_path {
    pub use crate::lastdb_uds::{
        split_database_path as split, valid_selector, DATABASES_PATH, DATABASE_SCOPE_PREFIX,
    };
}

pub const SOCKET_FILE_NAME: &str = "folddb.sock";
pub const DATABASES_PATH: &str = "/api/databases";
pub const DATABASE_SCOPE_PREFIX: &str = "/api/databases/";

const SOCKET_TMP_SUFFIX: &str = ".tmp";
const MAX_SOCKET_PATH_LEN: usize = 103;
const ACCEPT_POLL_INTERVAL: Duration = Duration::from_millis(50);

pub fn valid_selector(selector: &str) -> bool {
    !selector.is_empty()
        && selector.len() <= 64
        && selector
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_'))
}

pub fn split_database_path(target: &str) -> Option<(&str, &str)> {
    target
        .strip_prefix(DATABASE_SCOPE_PREFIX)?
        .split_once('/')
        .filter(|(sel, _)| valid_selector(sel))
}

pub fn socket_path(data_dir: &Path) -> PathBuf {
    data_dir.join(SOCKET_FILE_NAME)
}

fn check_len(path: &Path) -> io::Result<()> {
    let len = path.as_os_str().len();
    (len <= MAX_SOCKET_PATH_LEN).then_some(()).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("socket path is {len} bytes, exceeds the {MAX_SOCKET_PATH_LEN}-byte sockaddr_un limit"),
        )
    })
}

fn tmp_socket_path(path: &Path) -> PathBuf {
    let name = path
        .file_name()
        .map_or_else(|| SOCKET_FILE_NAME.into(), std::ffi::OsStr::to_string_lossy);
    path.with_file_name(format!("{name}{SOCKET_TMP_SUFFIX}"))
}

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
        Ok(m) if m.file_type().is_socket() => std::fs::remove_file(path),
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

#[derive(Debug)]
pub struct UdsSocket {
    path: PathBuf,
    listener: UnixListener,
}

impl UdsSocket {
    pub fn bind_at(path: impl Into<PathBuf>) -> io::Result<Self> {
        let path = path.into();
        let tmp = tmp_socket_path(&path);
        check_len(&path)?;
        check_len(&tmp)?;
        if UnixStream::connect(&path).is_ok() {
            return Err(io::Error::new(
                io::ErrorKind::AddrInUse,
                format!(
                    "a live node is already listening on {} — refusing to take over its socket",
                    path.display()
                ),
            ));
        }
        remove_stale_socket(&tmp)?;
        remove_stale_socket(&path)?;
        let listener = UnixListener::bind(&tmp)?;
        if let Err(e) = std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o600))
            .and_then(|()| std::fs::rename(&tmp, &path))
        {
            let _ = std::fs::remove_file(&tmp);
            return Err(e);
        }
        Ok(Self { path, listener })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    pub fn serve<F: FnMut(UnixStream)>(
        &self,
        owner_uid: u32,
        shutdown: &AtomicBool,
        mut on_accept: F,
    ) -> io::Result<()> {
        self.listener.set_nonblocking(true)?;
        let mut fd_ep: Option<(Instant, Instant, u64)> = None;
        while !shutdown.load(Ordering::Relaxed) {
            let (stream, _) = match self.listener.accept() {
                Ok(conn) => {
                    if let Some((started, _, attempts)) = fd_ep.take() {
                        tracing::warn!(
                            failed_accepts = attempts,
                            elapsed_ms = started.elapsed().as_millis() as u64,
                            "control-socket accept succeeded again after fd exhaustion"
                        );
                    }
                    conn
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    let mut pfd = libc::pollfd {
                        fd: self.listener.as_raw_fd(),
                        events: libc::POLLIN,
                        revents: 0,
                    };
                    // SAFETY: `pfd` is a valid single-element `pollfd` on this stack frame.
                    unsafe { libc::poll(&raw mut pfd, 1, 50) };
                    continue;
                }
                Err(e) if matches!(e.raw_os_error(), Some(libc::EMFILE | libc::ENFILE)) => {
                    std::thread::sleep(record_fd_exhaustion(&mut fd_ep, &e));
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
                }) => {
                    tracing::warn!(
                        peer_uid,
                        owner_uid,
                        "dropping control-socket connection from a foreign OS user"
                    );
                }
                Err(e) => {
                    tracing::warn!(error = %e, "dropping control-socket connection: peer-credential read failed");
                }
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

fn record_fd_exhaustion(
    episode: &mut Option<(Instant, Instant, u64)>,
    error: &io::Error,
) -> Duration {
    let now = Instant::now();
    let (started, last, attempts) = episode.get_or_insert((now, now, 0));
    *attempts = attempts.saturating_add(1);
    let shift = u32::try_from(*attempts - 1).unwrap_or(31).min(31);
    let backoff = ACCEPT_POLL_INTERVAL
        .saturating_mul(1_u32 << shift)
        .min(Duration::from_secs(1));
    let backoff_ms = backoff.as_millis() as u64;
    if *attempts == 1 {
        tracing::error!(error = %error, backoff_ms, "control-socket accept hit fd exhaustion; backing off and keeping listener alive");
    } else if now.duration_since(*last) >= Duration::from_secs(30) {
        *last = now;
        tracing::warn!(error = %error, failed_accepts = *attempts, elapsed_ms = now.duration_since(*started).as_millis() as u64, backoff_ms, "control-socket accept still out of file descriptors; listener alive");
    }
    backoff
}
