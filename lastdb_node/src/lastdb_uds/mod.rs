//! Unix-domain-socket transport, bind, peer-credential gate, and timeout budgets.

use std::io;
use std::os::unix::io::AsRawFd;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use fold_db::access::peer_cred::read_peer_credential;
use fold_db::access::PeerCredVerdict;

pub mod uds_http;
pub mod uds_router;
mod worker_pool;

use uds_router::{bind_socket_listener, record_fd_exhaustion};
pub use uds_router::{
    preflight_data_dir, socket_path, split_database_path, valid_selector, DATABASES_PATH,
    DATABASE_SCOPE_PREFIX, SOCKET_FILE_NAME,
};
pub use worker_pool::{ConnectionWork, SubmitError, UdsPoolSnapshot, UdsWorkerPool};

pub const DEFAULT_CLI_UDS_TIMEOUT_SECS: u64 = 30;

fn secs_override(var: &str) -> Option<Duration> {
    fold_db::env_flag::var_parsed::<u64>(var)
        .filter(|&n| n > 0)
        .map(Duration::from_secs)
}

#[must_use]
pub fn handler_timeout() -> Duration {
    secs_override("LASTDB_UDS_HANDLER_TIMEOUT_SECS").unwrap_or(Duration::from_secs(90))
}

#[must_use]
pub fn admin_handler_timeout_override() -> Option<Duration> {
    secs_override("LASTDB_UDS_ADMIN_TIMEOUT_SECS")
}

#[must_use]
pub fn admin_handler_timeout() -> Duration {
    admin_handler_timeout_override().unwrap_or(Duration::from_secs(600))
}

#[derive(Debug)]
pub struct UdsSocket {
    path: PathBuf,
    listener: UnixListener,
}

impl UdsSocket {
    pub fn bind_at(path: impl Into<PathBuf>) -> io::Result<Self> {
        let (path, listener) = bind_socket_listener(path.into())?;
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
                    if let Some((started, _, n)) = fd_ep.take() {
                        let ms = started.elapsed().as_millis() as u64;
                        tracing::warn!(
                            failed_accepts = n,
                            elapsed_ms = ms,
                            "control-socket accept recovered from fd exhaustion"
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
                    thread::sleep(record_fd_exhaustion(&mut fd_ep, &e));
                    continue;
                }
                Err(e) => return Err(e),
            };
            if let Err(e) = stream.set_nonblocking(false) {
                tracing::warn!(error = %e, "dropping control-socket connection: blocking mode");
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
                        "dropping connection from foreign OS user"
                    );
                }
                Err(e) => tracing::warn!(error = %e, "dropping connection: peer-credential failed"),
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
