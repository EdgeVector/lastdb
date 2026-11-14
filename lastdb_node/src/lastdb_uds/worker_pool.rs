//! Bounded LIFO Unix-domain-socket worker pool and async connection handoff.

use std::collections::VecDeque;
use std::os::unix::net::UnixStream;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::thread;
use std::time::{Duration, Instant};

use super::uds_http;

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
