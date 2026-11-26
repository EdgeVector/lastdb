//! Time that committed `POST /v1/write` requests spent in each phase of the handler, summed
//! since the node started. Counters only: a write takes no lock. The ops history sample takes
//! the change since the previous sample (see `sample`).
//!
//! The phases follow one another, so for one write they add up to `handler`:
//! - `parse`: reading the JSON body;
//! - `prepare`: checks, schema names, the old record of a delete, field validation, and the
//!   wait for a shared database's write lock;
//! - `lock_wait`: waiting for the write gates of the store;
//! - `read_and_stage`: under the gate, reading the old records, checking conditions, signing
//!   the spool entry and staging the batch;
//! - `commit`: the commit log write and its ordered flush, until the commit completes;
//! - `after_commit`: after the commit, before the reply (waking the sync loop, or signing a
//!   shared database's entries).
//!
//! `handler` is the whole handler. A request's time in `lastdb ops` also holds the wait for a
//! socket worker and the reply, which no phase holds. `slowest_ns` is the slowest single value
//! of that phase, not of one write.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, PoisonError};
use std::time::Duration;

use serde_json::{json, Value};

const NAMES: [&str; 7] = [
    "parse",
    "prepare",
    "lock_wait",
    "read_and_stage",
    "commit",
    "after_commit",
    "handler",
];

/// The phases of one committed write.
#[derive(Default, Clone, Copy)]
pub(crate) struct WriteTimes {
    pub(crate) parse: Duration,
    pub(crate) prepare: Duration,
    pub(crate) lock_wait: Duration,
    pub(crate) read_and_stage: Duration,
    pub(crate) commit: Duration,
    pub(crate) after_commit: Duration,
    pub(crate) handler: Duration,
}

impl WriteTimes {
    fn nanos(&self) -> [u64; 7] {
        [
            self.parse,
            self.prepare,
            self.lock_wait,
            self.read_and_stage,
            self.commit,
            self.after_commit,
            self.handler,
        ]
        .map(|phase| u64::try_from(phase.as_nanos()).unwrap_or(u64::MAX))
    }
}

pub(super) struct PhaseTotals {
    writes: AtomicU64,
    total_ns: [AtomicU64; 7],
    slowest_ns: [AtomicU64; 7],
    /// The slowest value of each phase since the last history sample.
    recent_slowest_ns: [AtomicU64; 7],
    /// `writes`, then each `total_ns`, at the last history sample.
    mark: Mutex<[u64; 8]>,
}

impl Default for PhaseTotals {
    fn default() -> Self {
        Self {
            writes: AtomicU64::new(0),
            total_ns: std::array::from_fn(|_| AtomicU64::new(0)),
            slowest_ns: std::array::from_fn(|_| AtomicU64::new(0)),
            recent_slowest_ns: std::array::from_fn(|_| AtomicU64::new(0)),
            mark: Mutex::new([0; 8]),
        }
    }
}

impl PhaseTotals {
    pub(super) fn add(&self, times: &WriteTimes) {
        self.writes.fetch_add(1, Ordering::Relaxed);
        for (index, ns) in times.nanos().into_iter().enumerate() {
            self.total_ns[index].fetch_add(ns, Ordering::Relaxed);
            self.slowest_ns[index].fetch_max(ns, Ordering::Relaxed);
            self.recent_slowest_ns[index].fetch_max(ns, Ordering::Relaxed);
        }
    }

    /// The writes and phase times since the previous sample, in the shape of [`Self::value`],
    /// with the slowest value since then. Null when no write committed since then. A write
    /// that commits while the sample runs may fall partly in the next sample.
    pub(super) fn sample(&self) -> Value {
        let mut mark = self.mark.lock().unwrap_or_else(PoisonError::into_inner);
        let writes = self.writes.load(Ordering::Relaxed);
        let added = writes.saturating_sub(mark[0]);
        if added == 0 {
            return Value::Null;
        }
        mark[0] = writes;
        let phases: Vec<Value> = NAMES
            .iter()
            .enumerate()
            .map(|(index, name)| {
                let total = self.total_ns[index].load(Ordering::Relaxed);
                let value = json!({
                    "phase": name,
                    "total_ns": total.saturating_sub(mark[index + 1]),
                    "slowest_ns": self.recent_slowest_ns[index].swap(0, Ordering::Relaxed),
                });
                mark[index + 1] = total;
                value
            })
            .collect();
        json!({ "writes": added, "phases": phases })
    }

    pub(super) fn value(&self) -> Value {
        let phases: Vec<Value> = NAMES
            .iter()
            .enumerate()
            .map(|(index, name)| {
                json!({
                    "phase": name,
                    "total_ns": self.total_ns[index].load(Ordering::Relaxed),
                    "slowest_ns": self.slowest_ns[index].load(Ordering::Relaxed),
                })
            })
            .collect();
        json!({
            "writes": self.writes.load(Ordering::Relaxed),
            "phases": phases,
        })
    }
}
