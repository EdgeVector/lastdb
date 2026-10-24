//! Hand accepted jobs to idle workers, most recently idle first.
//!
//! Waiting jobs stay FIFO. Idle workers form a stack, so a new job goes to the
//! worker that went idle last. Under light load the same few threads serve
//! every request and the rest of the pool stays cold.
//!
//! Why: each thread's allocator heap keeps its own partly used pages. A shared
//! receiver rotated requests through every worker, so a node with 28 workers
//! pinned about one page set per worker even for one sequential client
//! (measured 2026-10-19: 13 MB live, ~140 MB footprint, plateau ~280 MB with
//! 256-key reads). With LIFO hand-off, retained pages follow real concurrency
//! instead of pool size.

use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};

pub(super) struct Dispatch<J> {
    state: Mutex<State<J>>,
}

struct State<J> {
    queue: VecDeque<J>,
    capacity: usize,
    /// Invariant: non-empty only while `queue` is empty.
    idle: Vec<Arc<Slot<J>>>,
    closed: bool,
}

/// One worker's private hand-off cell.
pub(super) struct Slot<J> {
    hand: Mutex<Hand<J>>,
    ready: Condvar,
}

enum Hand<J> {
    Empty,
    Job(J),
    Closed,
}

pub(super) enum Rejected<J> {
    Full(J),
    Closed(J),
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

impl<J> Slot<J> {
    pub(super) fn new() -> Arc<Self> {
        Arc::new(Self {
            hand: Mutex::new(Hand::Empty),
            ready: Condvar::new(),
        })
    }

    fn give(&self, hand: Hand<J>) {
        *lock(&self.hand) = hand;
        self.ready.notify_one();
    }
}

impl<J> Dispatch<J> {
    pub(super) fn new(capacity: usize) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State {
                queue: VecDeque::new(),
                capacity,
                idle: Vec::new(),
                closed: false,
            }),
        })
    }

    /// Give `job` to the most recently idle worker, or queue it behind busy
    /// workers. A full queue or a closed pool returns the job to the caller.
    pub(super) fn submit(&self, job: J) -> Result<(), Rejected<J>> {
        let slot = {
            let mut state = lock(&self.state);
            if state.closed {
                return Err(Rejected::Closed(job));
            }
            match state.idle.pop() {
                Some(slot) => slot,
                None if state.queue.len() >= state.capacity => return Err(Rejected::Full(job)),
                None => {
                    state.queue.push_back(job);
                    return Ok(());
                }
            }
        };
        slot.give(Hand::Job(job));
        Ok(())
    }

    /// Refuse new jobs. Idle workers exit now; busy workers drain the queue
    /// first and then exit.
    pub(super) fn close(&self) {
        let idle = {
            let mut state = lock(&self.state);
            state.closed = true;
            std::mem::take(&mut state.idle)
        };
        for slot in idle {
            slot.give(Hand::Closed);
        }
    }

    /// Requeue the response of an already admitted request. Global admission
    /// counts this same request throughout, so responses plus new jobs remain
    /// bounded by workers + queue capacity. Responses release those admissions.
    pub(super) fn resume(&self, job: J) -> Result<(), J> {
        let slot = {
            let mut state = lock(&self.state);
            if state.closed {
                return Err(job);
            }
            if let Some(slot) = state.idle.pop() {
                slot
            } else {
                state.queue.push_front(job);
                return Ok(());
            }
        };
        slot.give(Hand::Job(job));
        Ok(())
    }

    /// The next job for the worker that owns `slot`, or `None` once the pool
    /// is closed and its queue is empty.
    pub(super) fn next(&self, slot: &Arc<Slot<J>>) -> Option<J> {
        {
            let mut state = lock(&self.state);
            if let Some(job) = state.queue.pop_front() {
                return Some(job);
            }
            if state.closed {
                return None;
            }
            state.idle.push(Arc::clone(slot));
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
