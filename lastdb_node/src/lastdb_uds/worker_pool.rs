//! Bounded LIFO worker pool and FIFO queue for control-socket connection handling.

use std::collections::VecDeque;
use std::os::unix::net::UnixStream;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::thread;
use std::time::{Duration, Instant};

use crate::lastdb_uds::uds_http;

const DEFAULT_QUEUE_CAPACITY: usize = 256;
const MIN_WORKERS: usize = 8;
const MAX_WORKERS: usize = 64;
const DEFAULT_WORKER_STACK_BYTES: usize = 8 * 1024 * 1024;

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Why [`UdsWorkerPool::try_submit_owned_connection`] refused the job.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubmitError {
    /// Queue is full — backpressure 503 was written to the peer.
    QueueFull,
    /// Admission is closed, or all workers exited.
    ShutDown,
}

struct ConnJob {
    stream: UnixStream,
    work: Box<dyn FnOnce(UnixStream, ConnectionWork) + Send + 'static>,
    guard: InFlightGuard,
}

/// Admission kept for one accepted connection across async continuation.
pub struct ConnectionWork {
    guard: InFlightGuard,
}

impl ConnectionWork {
    /// Continue an accepted request on the same bounded worker pool, retaining its admission.
    pub fn resume<F: FnOnce(UnixStream, Self) + Send + 'static>(self, stream: UnixStream, work: F) {
        let dispatch = Arc::clone(&self.guard.0.dispatch);
        let job = ConnJob {
            stream,
            work: Box::new(work),
            guard: self.guard,
        };
        if dispatch.enqueue(job, true).is_err() {
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

/// Bounded worker pool handle shared by accept loops and `/api/status`.
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
        let mut state = lock(&self.0.admission);
        state.in_flight -= 1;
        if state.in_flight == 0 {
            self.0.drained.notify_all();
        }
    }
}

impl UdsWorkerPool {
    /// Build a worker pool from environment configuration.
    #[must_use]
    pub fn from_env() -> Self {
        let cpus = thread::available_parallelism().map_or(4, std::num::NonZeroUsize::get);
        let default_workers = cpus.saturating_mul(2).clamp(MIN_WORKERS, MAX_WORKERS);
        let workers = fold_db::env_flag::var_or("LASTDB_UDS_WORKERS", default_workers).max(1);
        let queue_capacity =
            fold_db::env_flag::var_or("LASTDB_UDS_QUEUE", DEFAULT_QUEUE_CAPACITY).max(1);
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
        let stack =
            fold_db::env_flag::var_or("RUST_MIN_STACK", 0usize).max(DEFAULT_WORKER_STACK_BYTES);
        for i in 0..workers {
            let dispatch = Arc::clone(&dispatch);
            thread::Builder::new()
                .name(format!("lastdb-uds-worker-{i}"))
                .stack_size(stack)
                .spawn(move || {
                    let slot = Arc::new(Slot {
                        hand: Mutex::new(Hand::Empty),
                        ready: Condvar::new(),
                    });
                    while let Some(job) = dispatch.next(&slot) {
                        (job.work)(job.stream, ConnectionWork { guard: job.guard });
                    }
                })
                .unwrap_or_else(|e| panic!("failed to spawn UDS worker {i}: {e}"));
        }
        Self { inner }
    }

    /// Submit an accepted connection for handling on the bounded worker pool.
    pub fn try_submit_owned_connection<F: FnOnce(UnixStream, ConnectionWork) + Send + 'static>(
        &self,
        stream: UnixStream,
        work: F,
    ) -> Result<(), SubmitError> {
        let guard = {
            let mut state = lock(&self.inner.admission);
            if state.closed {
                return self.reject(stream, SubmitError::ShutDown);
            }
            if state.in_flight >= self.inner.workers.saturating_add(self.inner.queue_capacity) {
                return self.reject(stream, SubmitError::QueueFull);
            }
            state.in_flight += 1;
            InFlightGuard(Arc::clone(&self.inner))
        };
        let job = ConnJob {
            stream,
            work: Box::new(work),
            guard,
        };
        match self.inner.dispatch.enqueue(job, false) {
            Ok(()) => {
                self.inner.submitted.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            Err((job, err)) => self.reject(job.stream, err),
        }
    }

    fn reject(&self, mut stream: UnixStream, error: SubmitError) -> Result<(), SubmitError> {
        if matches!(error, SubmitError::QueueFull) {
            self.inner
                .queue_full_rejects
                .fetch_add(1, Ordering::Relaxed);
        }
        if let Err(err) = uds_http::write_busy_and_close(&mut stream) {
            tracing::debug!(error = %err, "busy response write failed");
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

    /// Refuse new connections, then wait up to `timeout` for queued and active handlers.
    pub fn close_and_drain(&self, timeout: Duration) -> Result<(), String> {
        let started = Instant::now();
        let mut state = lock(&self.inner.admission);
        state.closed = true;
        while state.in_flight != 0 {
            let remaining = timeout.saturating_sub(started.elapsed());
            if remaining.is_zero() {
                return Err(format!(
                    "UDS worker pool drain timed out with {} accepted connection(s) still active",
                    state.in_flight
                ));
            }
            state = self
                .inner
                .drained
                .wait_timeout(state, remaining)
                .unwrap_or_else(PoisonError::into_inner)
                .0;
        }
        Ok(())
    }

    /// Occupancy and reject counters for `/api/status`.
    #[must_use]
    pub fn snapshot(&self) -> UdsPoolSnapshot {
        UdsPoolSnapshot {
            workers: self.inner.workers,
            queue_capacity: self.inner.queue_capacity,
            in_flight: lock(&self.inner.admission).in_flight,
            submitted: self.inner.submitted.load(Ordering::Relaxed),
            queue_full_rejects: self.inner.queue_full_rejects.load(Ordering::Relaxed),
        }
    }
}

struct Dispatch<J> {
    state: Mutex<DispatchState<J>>,
}

struct DispatchState<J> {
    queue: VecDeque<J>,
    capacity: usize,
    idle: Vec<Arc<Slot<J>>>,
    closed: bool,
}

struct Slot<J> {
    hand: Mutex<Hand<J>>,
    ready: Condvar,
}

enum Hand<J> {
    Empty,
    Job(J),
    Closed,
}

impl<J> Dispatch<J> {
    fn new(capacity: usize) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(DispatchState {
                queue: VecDeque::new(),
                capacity,
                idle: Vec::new(),
                closed: false,
            }),
        })
    }

    fn enqueue(&self, job: J, resume_front: bool) -> Result<(), (J, SubmitError)> {
        let slot = {
            let mut s = lock(&self.state);
            if s.closed {
                return Err((job, SubmitError::ShutDown));
            }
            let Some(slot) = s.idle.pop() else {
                if resume_front {
                    s.queue.push_front(job);
                } else if s.queue.len() < s.capacity {
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

    fn close(&self) {
        let idle = {
            let mut s = lock(&self.state);
            s.closed = true;
            std::mem::take(&mut s.idle)
        };
        for slot in idle {
            *lock(&slot.hand) = Hand::Closed;
            slot.ready.notify_one();
        }
    }

    fn next(&self, slot: &Arc<Slot<J>>) -> Option<J> {
        {
            let mut s = lock(&self.state);
            if let Some(job) = s.queue.pop_front() {
                return Some(job);
            }
            if s.closed {
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
