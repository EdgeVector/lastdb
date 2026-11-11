//! Bounded worker pool + queue for control-socket connection handling.
//!
//! - `LASTDB_UDS_WORKERS` — worker threads (default: `2 * available_parallelism`, clamped to `8..=64`).
//! - `LASTDB_UDS_QUEUE` — pending accepted jobs waiting for a free worker (default `256`).
//! - `RUST_MIN_STACK` — raises the worker stack above [`DEFAULT_WORKER_STACK_BYTES`]; never lowers it.

use std::os::unix::net::UnixStream;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use crate::lastdb_uds::uds_http;

mod dispatch;
use dispatch::{Dispatch, Rejected, Slot};

const DEFAULT_QUEUE_CAPACITY: usize = 256;
const MIN_WORKERS: usize = 8;
const MAX_WORKERS: usize = 64;
const DEFAULT_WORKER_STACK_BYTES: usize = 8 * 1024 * 1024;

/// Why [`UdsWorkerPool::try_submit_owned_connection`] refused the job.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubmitError {
    /// Queue is full — backpressure 503 was written to the peer.
    QueueFull,
    /// Admission is closed, or all workers exited.
    ShutDown,
}

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
    #[must_use]
    fn from_env() -> Self {
        let defaults = Self::default();
        let workers = fold_db::env_flag::var_parsed::<usize>("LASTDB_UDS_WORKERS")
            .unwrap_or(defaults.workers);
        let queue_capacity = fold_db::env_flag::var_parsed::<usize>("LASTDB_UDS_QUEUE")
            .unwrap_or(defaults.queue_capacity);
        Self {
            workers: workers.max(1),
            queue_capacity: queue_capacity.max(1),
        }
    }
}

#[must_use]
fn worker_stack_bytes() -> usize {
    fold_db::env_flag::var_parsed::<usize>("RUST_MIN_STACK")
        .unwrap_or(0)
        .max(DEFAULT_WORKER_STACK_BYTES)
}

#[must_use]
fn default_worker_count() -> usize {
    let cpus = std::thread::available_parallelism().map_or(4, std::num::NonZeroUsize::get);
    (cpus.saturating_mul(2)).clamp(MIN_WORKERS, MAX_WORKERS)
}

struct ConnJob {
    stream: UnixStream,
    work: Box<dyn FnOnce(UnixStream, ConnectionWork) + Send + 'static>,
    in_flight_guard: InFlightGuard,
}

/// Admission kept for one accepted connection across async continuation.
pub struct ConnectionWork {
    guard: InFlightGuard,
}

impl ConnectionWork {
    /// Continue an accepted request on the same bounded worker pool, retaining its original admission.
    pub fn resume<F>(self, stream: UnixStream, work: F)
    where
        F: FnOnce(UnixStream, Self) + Send + 'static,
    {
        let dispatch = Arc::clone(&self.guard.0.dispatch);
        let job = ConnJob {
            stream,
            work: Box::new(work),
            in_flight_guard: self.guard,
        };
        if dispatch.resume(job).is_err() {
            tracing::error!("accepted socket response dispatcher closed");
        }
    }
}

/// Point-in-time occupancy for `/api/status`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UdsPoolSnapshot {
    /// Worker threads configured for this pool.
    pub workers: usize,
    /// How many accepted jobs may wait for a worker.
    pub queue_capacity: usize,
    /// Jobs queued or running (not yet finished).
    pub in_flight: usize,
    /// Jobs the pool has accepted since it started.
    pub submitted: u64,
    /// Jobs refused because the queue was full.
    pub queue_full_rejects: u64,
}

/// Shared handle: accept loops `try_submit_owned_connection`; workers drain the queue.
#[derive(Clone)]
pub struct UdsWorkerPool {
    inner: Arc<PoolInner>,
}

struct PoolInner {
    dispatch: Arc<Dispatch<ConnJob>>,
    workers: usize,
    queue_capacity: usize,
    submitted: AtomicU64,
    queue_full_rejects: AtomicU64,
    admission: Mutex<AdmissionState>,
    drained: Condvar,
}

#[derive(Default)]
struct AdmissionState {
    closed: bool,
    in_flight: usize,
}

impl Drop for PoolInner {
    fn drop(&mut self) {
        self.dispatch.close();
    }
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
    #[must_use]
    fn new(config: WorkerPoolConfig) -> Self {
        let workers = config.workers.max(1);
        let queue_capacity = config.queue_capacity.max(1);
        let dispatch = Dispatch::new(queue_capacity);
        let inner = Arc::new(PoolInner {
            dispatch: Arc::clone(&dispatch),
            workers,
            queue_capacity,
            submitted: AtomicU64::new(0),
            queue_full_rejects: AtomicU64::new(0),
            admission: Mutex::new(AdmissionState::default()),
            drained: Condvar::new(),
        });

        let stack_bytes = worker_stack_bytes();
        for i in 0..workers {
            let dispatch = Arc::clone(&dispatch);
            thread::Builder::new()
                .name(format!("lastdb-uds-worker-{i}"))
                .stack_size(stack_bytes)
                .spawn(move || worker_loop(&dispatch))
                .unwrap_or_else(|e| panic!("failed to spawn UDS worker {i}: {e}"));
        }

        Self { inner }
    }

    /// Build a worker pool from environment configuration.
    #[must_use]
    pub fn from_env() -> Self {
        Self::new(WorkerPoolConfig::from_env())
    }

    /// Transfer completion ownership when a handler awaits outside the pool.
    pub fn try_submit_owned_connection<F>(
        &self,
        stream: UnixStream,
        work: F,
    ) -> Result<(), SubmitError>
    where
        F: FnOnce(UnixStream, ConnectionWork) + Send + 'static,
    {
        let in_flight_guard = match self.admit() {
            Ok(guard) => guard,
            Err(error) => return self.reject(stream, error),
        };
        let job = ConnJob {
            stream,
            work: Box::new(work),
            in_flight_guard,
        };
        match self.inner.dispatch.submit(job) {
            Ok(()) => {
                self.inner.submitted.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            Err(Rejected::Full(job)) => self.reject(job.stream, SubmitError::QueueFull),
            Err(Rejected::Closed(job)) => self.reject(job.stream, SubmitError::ShutDown),
        }
    }

    fn admit(&self) -> Result<InFlightGuard, SubmitError> {
        let mut state = self
            .inner
            .admission
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.closed {
            return Err(SubmitError::ShutDown);
        }
        if state.in_flight >= self.inner.workers.saturating_add(self.inner.queue_capacity) {
            return Err(SubmitError::QueueFull);
        }
        state.in_flight += 1;
        Ok(InFlightGuard(Arc::clone(&self.inner)))
    }

    fn reject(&self, mut stream: UnixStream, error: SubmitError) -> Result<(), SubmitError> {
        if matches!(error, SubmitError::QueueFull) {
            self.inner
                .queue_full_rejects
                .fetch_add(1, Ordering::Relaxed);
        }
        if let Err(error) = uds_http::write_busy_and_close(&mut stream) {
            tracing::debug!(error = %error, "busy response write failed");
        }
        Err(error)
    }

    /// Worker threads configured for this pool.
    #[must_use]
    pub fn workers(&self) -> usize {
        self.inner.workers
    }

    /// How many accepted jobs may wait for a worker.
    #[must_use]
    pub fn queue_capacity(&self) -> usize {
        self.inner.queue_capacity
    }

    /// Jobs refused because the queue was full.
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

fn worker_loop(dispatch: &Dispatch<ConnJob>) {
    let slot = Slot::new();
    while let Some(job) = dispatch.next(&slot) {
        (job.work)(
            job.stream,
            ConnectionWork {
                guard: job.in_flight_guard,
            },
        );
    }
}
