//! Bounded **worker pool + queue** for control-socket connection handling.
//!
//! # Why this is the primary concurrency control
//!
//! Mini used to treat “max concurrent connections” as the product limit. That is
//! a poor limiter: idle apps cost nothing, and hung handlers pin slots at 0% CPU
//! while real work waits. The right shape is:
//!
//! 1. **Accept** is cheap (one FD until a worker starts).
//! 2. **Bounded workers** do handler work (CPU-shaped concurrency).
//! 3. **Bounded queue** absorbs bursts; when full → immediate **backpressure**
//!    (HTTP 503) instead of spawning unbounded threads.
//!
//! FD exhaustion (EMFILE) is still bounded because in-flight streams cannot
//! exceed roughly `workers + queue_capacity` — without pretending “connections”
//! are the product resource.
//!
//! # Env
//!
//! - `LASTDB_UDS_WORKERS` — worker threads (default: `2 * available_parallelism`,
//!   clamped to **8..=64**).
//! - `LASTDB_UDS_QUEUE` — pending accepted jobs waiting for a free worker
//!   (default **256**).
//! - `RUST_MIN_STACK` — raises the worker stack above
//!   [`DEFAULT_WORKER_STACK_BYTES`]; never lowers it.

use std::cell::Cell;
use std::os::unix::net::UnixStream;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender, TrySendError};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use crate::uds_http;

/// Default queue depth when `LASTDB_UDS_QUEUE` is unset.
const DEFAULT_QUEUE_CAPACITY: usize = 256;

/// Floor / ceiling for the auto worker count.
const MIN_WORKERS: usize = 8;
const MAX_WORKERS: usize = 64;

/// Explicit stack size for every UDS worker thread.
///
/// Workers used to spawn with a bare `thread::Builder`, inheriting the
/// platform default (2 MiB). Handler frames in an **unoptimized** build exceed
/// that, so a debug-built `lastdbd` aborted on the very first `/api/status` —
/// on a fresh empty home, nothing to do with the data:
///
/// ```text
/// thread 'lastdb-uds-worker-1' has overflowed its stack
/// fatal runtime error: stack overflow, aborting
/// ```
///
/// Release builds were unaffected, which is exactly what made this expensive:
/// the crash appeared only when someone was debugging, and every integration
/// test that boots a `Host` inherited it. Brain:
/// `papercut-lastdbd-uds-worker-stack-overflow-on-debug-status`.
///
/// 8 MiB matches the main thread's default. Thread stacks are reserved address
/// space committed lazily page by page, so this costs virtual mappings, not
/// resident memory — a 64-worker pool reserves 512 MiB of address space and
/// still touches only the pages it uses.
const DEFAULT_WORKER_STACK_BYTES: usize = 8 * 1024 * 1024;

/// Why [`UdsWorkerPool::try_submit_connection`] refused the job.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubmitError {
    /// Queue is full — backpressure 503 was (or should be) written to the peer.
    QueueFull,
    /// Admission is closed, or all workers exited / the channel disconnected.
    ShutDown,
}

/// Configuration for a [`UdsWorkerPool`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct WorkerPoolConfig {
    workers: usize,
    queue_capacity: usize,
}

impl Default for WorkerPoolConfig {
    fn default() -> Self {
        Self {
            workers: default_worker_count(),
            queue_capacity: DEFAULT_QUEUE_CAPACITY,
        }
    }
}

impl WorkerPoolConfig {
    /// Build from env, falling back to [`Self::default`] for unset/invalid knobs.
    #[must_use]
    fn from_env() -> Self {
        let defaults = Self::default();
        let workers =
            env_flag::var_parsed::<usize>("LASTDB_UDS_WORKERS").unwrap_or(defaults.workers);
        let queue_capacity =
            env_flag::var_parsed::<usize>("LASTDB_UDS_QUEUE").unwrap_or(defaults.queue_capacity);
        Self {
            workers: workers.max(1),
            queue_capacity: queue_capacity.max(1),
        }
    }
}

/// Stack size for UDS workers: [`DEFAULT_WORKER_STACK_BYTES`], raised to
/// `RUST_MIN_STACK` when that is larger.
///
/// Never returns less than the floor, so the debug-build overflow cannot come
/// back via a small `RUST_MIN_STACK`.
#[must_use]
fn worker_stack_bytes() -> usize {
    env_flag::var_parsed::<usize>("RUST_MIN_STACK")
        .unwrap_or(0)
        .max(DEFAULT_WORKER_STACK_BYTES)
}

/// CPU-shaped default worker count: `2 * available_parallelism`, clamped.
#[must_use]
fn default_worker_count() -> usize {
    let cpus = std::thread::available_parallelism().map_or(4, std::num::NonZeroUsize::get);
    (cpus.saturating_mul(2)).clamp(MIN_WORKERS, MAX_WORKERS)
}

/// One accepted connection plus the work to run on a worker thread.
struct ConnJob {
    stream: UnixStream,
    /// When the accept loop submitted this job — the start of its queue wait.
    /// One request per connection on the control socket, so connection queue
    /// wait IS the request's queue wait.
    enqueued_at: Instant,
    /// Consumes the stream (handler owns it until done).
    work: Box<dyn FnOnce(UnixStream) + Send + 'static>,
    /// Admission owns one count until this job's handler finishes.
    in_flight_guard: InFlightGuard,
}

thread_local! {
    /// Queue wait of the job THIS worker thread is currently running.
    ///
    /// The worker thread runs the entire blocking handler (including any
    /// `Handle::block_on` future, which is polled on the calling thread), so
    /// a thread-local is a sound handoff that changes no public handler
    /// signature. Set just before a job's work runs, cleared right after.
    static CURRENT_QUEUE_WAIT: Cell<Option<Duration>> = const { Cell::new(None) };
}

/// Queue wait of the connection job the current worker thread is running, or
/// `None` off a worker thread (tests, direct calls, other runtimes).
///
/// Telemetry recorders call this from inside the request handler to attribute
/// socket-queue wait separately from handler work.
#[must_use]
pub fn current_queue_wait() -> Option<Duration> {
    CURRENT_QUEUE_WAIT.with(Cell::get)
}

/// Point-in-time occupancy for `/api/status` and self-metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UdsPoolSnapshot {
    pub workers: usize,
    pub queue_capacity: usize,
    /// Jobs queued or running (not yet finished).
    pub in_flight: usize,
    pub submitted: u64,
    pub queue_full_rejects: u64,
}

/// Shared handle: accept loops `try_submit_connection`; workers drain the queue.
#[derive(Clone)]
pub struct UdsWorkerPool {
    inner: Arc<PoolInner>,
}

struct PoolInner {
    tx: SyncSender<ConnJob>,
    workers: usize,
    queue_capacity: usize,
    submitted: AtomicU64,
    queue_full_rejects: AtomicU64,
    /// One lock closes admission and counts every accepted job. A close cannot
    /// observe zero between a submit's open check and its count increment.
    admission: Mutex<AdmissionState>,
    drained: Condvar,
}

#[derive(Default)]
struct AdmissionState {
    closed: bool,
    in_flight: usize,
}

struct InFlightGuard(Arc<PoolInner>);

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        let mut state = self
            .0
            .admission
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.in_flight -= 1;
        if state.in_flight == 0 {
            self.0.drained.notify_all();
        }
    }
}

impl UdsWorkerPool {
    /// Spawn `config.workers` threads that pull from a bounded channel of size
    /// `config.queue_capacity`.
    #[must_use]
    fn new(config: WorkerPoolConfig) -> Self {
        let workers = config.workers.max(1);
        let queue_capacity = config.queue_capacity.max(1);
        let (tx, rx) = sync_channel::<ConnJob>(queue_capacity);
        let inner = Arc::new(PoolInner {
            tx,
            workers,
            queue_capacity,
            submitted: AtomicU64::new(0),
            queue_full_rejects: AtomicU64::new(0),
            admission: Mutex::new(AdmissionState::default()),
            drained: Condvar::new(),
        });

        // `std::sync::mpsc::Receiver` is not `Sync`; share it behind a mutex so
        // workers can compete for jobs.
        let rx = Arc::new(std::sync::Mutex::new(rx));
        // Take the LARGER of our floor and any `RUST_MIN_STACK` the operator
        // set: setting `stack_size` explicitly makes `std::thread` stop
        // consulting that variable, so a naive floor would silently REMOVE the
        // documented workaround from anyone already relying on it.
        let stack_bytes = worker_stack_bytes();
        for i in 0..workers {
            let rx = Arc::clone(&rx);
            // Two independent reproductions of the same 2 MiB default being too
            // small, both on 2026-07-31: serializing a real-home /api/schemas
            // catalog (~2 MiB JSON, 1k+ schemas) overflowed on the first
            // list-schemas after identity-ready in a CoW boot proof, and a
            // debug-built node aborted on its very first /api/status. See
            // [`DEFAULT_WORKER_STACK_BYTES`].
            thread::Builder::new()
                .name(format!("lastdb-uds-worker-{i}"))
                .stack_size(stack_bytes)
                .spawn(move || worker_loop(&rx))
                .unwrap_or_else(|e| panic!("failed to spawn UDS worker {i}: {e}"));
        }

        Self { inner }
    }

    /// Config from environment ([`WorkerPoolConfig::from_env`]).
    #[must_use]
    pub fn from_env() -> Self {
        Self::new(WorkerPoolConfig::from_env())
    }

    /// Queue an accepted connection for a worker.
    ///
    /// On [`SubmitError::QueueFull`], writes HTTP 503
    /// (`uds_worker_queue_full`) to `stream` and closes it — the caller must
    /// not use `stream` again.
    pub fn try_submit_connection<F>(&self, stream: UnixStream, work: F) -> Result<(), SubmitError>
    where
        F: FnOnce(UnixStream) + Send + 'static,
    {
        let in_flight_guard = {
            let mut state = self
                .inner
                .admission
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if state.closed {
                None
            } else {
                state.in_flight += 1;
                Some(InFlightGuard(Arc::clone(&self.inner)))
            }
        };
        let Some(in_flight_guard) = in_flight_guard else {
            let mut stream = stream;
            if let Err(e) = uds_http::write_busy_and_close(&mut stream) {
                tracing::debug!(error = %e, "busy response write failed");
            }
            return Err(SubmitError::ShutDown);
        };
        let job = ConnJob {
            stream,
            enqueued_at: Instant::now(),
            work: Box::new(work),
            in_flight_guard,
        };
        match self.inner.tx.try_send(job) {
            Ok(()) => {
                self.inner.submitted.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            Err(TrySendError::Full(job)) => {
                self.inner
                    .queue_full_rejects
                    .fetch_add(1, Ordering::Relaxed);
                let mut stream = job.stream;
                if let Err(e) = uds_http::write_busy_and_close(&mut stream) {
                    tracing::debug!(error = %e, "busy response write failed");
                }
                Err(SubmitError::QueueFull)
            }
            Err(TrySendError::Disconnected(job)) => {
                let mut stream = job.stream;
                if let Err(e) = uds_http::write_busy_and_close(&mut stream) {
                    tracing::debug!(error = %e, "busy response write failed");
                }
                Err(SubmitError::ShutDown)
            }
        }
    }

    #[must_use]
    pub fn workers(&self) -> usize {
        self.inner.workers
    }

    #[must_use]
    pub fn queue_capacity(&self) -> usize {
        self.inner.queue_capacity
    }

    #[must_use]
    pub fn queue_full_rejects(&self) -> u64 {
        self.inner.queue_full_rejects.load(Ordering::Relaxed)
    }

    #[must_use]
    fn submitted(&self) -> u64 {
        self.inner.submitted.load(Ordering::Relaxed)
    }

    /// Jobs accepted into the pool and not yet finished (queued or running).
    #[must_use]
    pub fn in_flight(&self) -> usize {
        self.inner
            .admission
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .in_flight
    }

    /// Refuse new connections, then wait for queued and active handlers.
    ///
    /// Admission stays closed after a timeout. The caller must not record a
    /// clean shutdown when this method returns an error.
    pub fn close_and_drain(&self, timeout: Duration) -> Result<(), String> {
        let started = Instant::now();
        let mut state = self
            .inner
            .admission
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.closed = true;
        while state.in_flight != 0 {
            let remaining = timeout.saturating_sub(started.elapsed());
            if remaining.is_zero() {
                break;
            }
            let (next, _) = self
                .inner
                .drained
                .wait_timeout(state, remaining)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state = next;
        }
        if state.in_flight == 0 {
            Ok(())
        } else {
            Err(format!(
                "UDS worker pool drain timed out with {} accepted connection(s) still active",
                state.in_flight
            ))
        }
    }

    /// Occupancy + reject counters for status surfaces.
    #[must_use]
    pub fn snapshot(&self) -> UdsPoolSnapshot {
        UdsPoolSnapshot {
            workers: self.workers(),
            queue_capacity: self.queue_capacity(),
            in_flight: self.in_flight(),
            submitted: self.submitted(),
            queue_full_rejects: self.queue_full_rejects(),
        }
    }
}

fn worker_loop(rx: &Arc<std::sync::Mutex<Receiver<ConnJob>>>) {
    loop {
        let job = {
            let guard = match rx.lock() {
                Ok(g) => g,
                Err(poisoned) => poisoned.into_inner(),
            };
            match guard.recv() {
                Ok(job) => job,
                Err(_) => return, // sender dropped
            }
        };
        let ConnJob {
            stream,
            enqueued_at,
            work,
            in_flight_guard,
        } = job;
        // Measured at pickup: elapsed since submit is exactly the time the
        // job sat in the bounded queue behind busy workers.
        let queue_wait = enqueued_at.elapsed();
        CURRENT_QUEUE_WAIT.with(|cell| cell.set(Some(queue_wait)));
        work(stream);
        CURRENT_QUEUE_WAIT.with(|cell| cell.set(None));
        drop(in_flight_guard);
    }
}
