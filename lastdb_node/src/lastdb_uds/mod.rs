//! Unix-domain-socket transport, bind, peer-credential gate, and bounded LIFO worker pool.

use std::collections::VecDeque;
use std::io;
use std::os::unix::io::AsRawFd;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::thread;
use std::time::{Duration, Instant};

use fold_db::access::peer_cred::read_peer_credential;
use fold_db::access::PeerCredVerdict;

pub mod uds_http;
pub mod uds_router;

use uds_router::{bind_socket_listener, record_fd_exhaustion};
pub use uds_router::{
    preflight_data_dir, socket_path, split_database_path, valid_selector, DATABASES_PATH,
    DATABASE_SCOPE_PREFIX, SOCKET_FILE_NAME,
};

pub mod uds {
    pub use crate::lastdb_uds::{preflight_data_dir, socket_path, UdsSocket, SOCKET_FILE_NAME};
}

pub mod database_path {
    pub use crate::lastdb_uds::uds_router::{
        split_database_path as split, valid_selector, DATABASES_PATH, DATABASE_SCOPE_PREFIX,
    };
}

pub mod worker_pool {
    pub use crate::lastdb_uds::{ConnectionWork, SubmitError, UdsPoolSnapshot, UdsWorkerPool};
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

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubmitError {
    QueueFull,
    ShutDown,
}

type WorkFn = Box<dyn FnOnce(UnixStream, ConnectionWork) + Send + 'static>;
type ConnJob = (UnixStream, WorkFn, InFlightGuard);

pub struct ConnectionWork(InFlightGuard);

impl ConnectionWork {
    pub fn resume<F: FnOnce(UnixStream, Self) + Send + 'static>(self, stream: UnixStream, work: F) {
        let pool = Arc::clone(&self.0 .0);
        if pool
            .enqueue((stream, Box::new(work), self.0), true)
            .is_err()
        {
            tracing::error!("accepted socket response dispatcher closed");
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub struct UdsPoolSnapshot {
    pub workers: usize,
    pub queue_capacity: usize,
    pub in_flight: usize,
    pub submitted: u64,
    pub queue_full_rejects: u64,
}

#[derive(Clone)]
pub struct UdsWorkerPool {
    inner: Arc<PoolInner>,
}

struct PoolInner {
    workers: usize,
    queue_capacity: usize,
    submitted: AtomicU64,
    queue_full_rejects: AtomicU64,
    state: Mutex<PoolState>,
    drained: Condvar,
}

#[derive(Default)]
struct PoolState {
    queue: VecDeque<ConnJob>,
    idle: Vec<Arc<Slot>>,
    in_flight: usize,
    accepting_closed: bool,
    workers_closed: bool,
}

struct Slot {
    hand: Mutex<Hand>,
    ready: Condvar,
}

enum Hand {
    Empty,
    Job(ConnJob),
    Closed,
}

impl Drop for PoolInner {
    fn drop(&mut self) {
        let (idle, queued) = {
            let mut s = lock(&self.state);
            s.workers_closed = true;
            (std::mem::take(&mut s.idle), std::mem::take(&mut s.queue))
        };
        drop(queued);
        for slot in idle {
            *lock(&slot.hand) = Hand::Closed;
            slot.ready.notify_one();
        }
    }
}

struct InFlightGuard(Arc<PoolInner>);

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        let mut s = lock(&self.0.state);
        s.in_flight -= 1;
        if s.in_flight == 0 {
            self.0.drained.notify_all();
        }
    }
}

impl PoolInner {
    fn enqueue(&self, job: ConnJob, front: bool) -> Result<(), (ConnJob, SubmitError)> {
        let slot = {
            let mut s = lock(&self.state);
            if s.workers_closed {
                return Err((job, SubmitError::ShutDown));
            }
            let Some(slot) = s.idle.pop() else {
                if front {
                    s.queue.push_front(job);
                } else if s.queue.len() < self.queue_capacity {
                    s.queue.push_back(job);
                } else {
                    return Err((job, SubmitError::QueueFull));
                }
                return Ok(());
            };
            slot
        };
        *lock(&slot.hand) = Hand::Job(job);
        slot.ready.notify_one();
        Ok(())
    }

    fn next(&self, slot: &Arc<Slot>) -> Option<ConnJob> {
        {
            let mut s = lock(&self.state);
            if let Some(job) = s.queue.pop_front() {
                return Some(job);
            }
            if s.workers_closed {
                return None;
            }
            s.idle.push(Arc::clone(slot));
        }
        let mut hand = lock(&slot.hand);
        loop {
            match std::mem::replace(&mut *hand, Hand::Empty) {
                Hand::Job(job) => return Some(job),
                Hand::Closed => return None,
                Hand::Empty => {
                    hand = slot
                        .ready
                        .wait(hand)
                        .unwrap_or_else(PoisonError::into_inner);
                }
            }
        }
    }
}

impl UdsWorkerPool {
    #[must_use]
    pub fn from_env() -> Self {
        let cpus = thread::available_parallelism().map_or(4, std::num::NonZeroUsize::get);
        let workers =
            fold_db::env_flag::var_or("LASTDB_UDS_WORKERS", cpus.saturating_mul(2).clamp(8, 64))
                .max(1);
        let queue_capacity = fold_db::env_flag::var_or("LASTDB_UDS_QUEUE", 256usize).max(1);
        let inner = Arc::new(PoolInner {
            workers,
            queue_capacity,
            submitted: AtomicU64::new(0),
            queue_full_rejects: AtomicU64::new(0),
            state: Mutex::new(PoolState::default()),
            drained: Condvar::new(),
        });
        let stack = fold_db::env_flag::var_or("RUST_MIN_STACK", 0usize).max(8 * 1024 * 1024);
        for i in 0..workers {
            let pool = Arc::clone(&inner);
            let b = thread::Builder::new()
                .name(format!("lastdb-uds-worker-{i}"))
                .stack_size(stack);
            b.spawn(move || {
                let slot = Arc::new(Slot {
                    hand: Mutex::new(Hand::Empty),
                    ready: Condvar::new(),
                });
                while let Some((stream, work, guard)) = pool.next(&slot) {
                    work(stream, ConnectionWork(guard));
                }
            })
            .unwrap_or_else(|e| panic!("failed to spawn UDS worker {i}: {e}"));
        }
        Self { inner }
    }

    pub fn try_submit_owned_connection<F: FnOnce(UnixStream, ConnectionWork) + Send + 'static>(
        &self,
        stream: UnixStream,
        work: F,
    ) -> Result<(), SubmitError> {
        let guard = {
            let mut s = lock(&self.inner.state);
            if s.accepting_closed {
                return self.reject(stream, SubmitError::ShutDown);
            }
            if s.in_flight >= self.inner.workers.saturating_add(self.inner.queue_capacity) {
                return self.reject(stream, SubmitError::QueueFull);
            }
            s.in_flight += 1;
            InFlightGuard(Arc::clone(&self.inner))
        };
        match self.inner.enqueue((stream, Box::new(work), guard), false) {
            Ok(()) => {
                self.inner.submitted.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            Err(((stream, _, _), err)) => self.reject(stream, err),
        }
    }

    fn reject(&self, mut stream: UnixStream, err: SubmitError) -> Result<(), SubmitError> {
        if matches!(err, SubmitError::QueueFull) {
            self.inner
                .queue_full_rejects
                .fetch_add(1, Ordering::Relaxed);
        }
        let _ = uds_http::write_busy_and_close(&mut stream);
        Err(err)
    }

    pub fn close_and_drain(&self, timeout: Duration) -> Result<(), String> {
        let started = Instant::now();
        let mut s = lock(&self.inner.state);
        s.accepting_closed = true;
        while s.in_flight != 0 {
            let rem = timeout.saturating_sub(started.elapsed());
            if rem.is_zero() {
                return Err(format!(
                    "UDS worker pool drain timed out with {} active",
                    s.in_flight
                ));
            }
            s = self
                .inner
                .drained
                .wait_timeout(s, rem)
                .unwrap_or_else(PoisonError::into_inner)
                .0;
        }
        Ok(())
    }

    #[must_use]
    pub fn snapshot(&self) -> UdsPoolSnapshot {
        UdsPoolSnapshot {
            workers: self.inner.workers,
            queue_capacity: self.inner.queue_capacity,
            in_flight: lock(&self.inner.state).in_flight,
            submitted: self.inner.submitted.load(Ordering::Relaxed),
            queue_full_rejects: self.inner.queue_full_rejects.load(Ordering::Relaxed),
        }
    }
}
