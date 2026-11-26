//! Time that committed `POST /v1/write` requests spent in each phase of the handler, summed
//! since the node started. Counters only: no lock, nothing on disk.
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
}

impl Default for PhaseTotals {
    fn default() -> Self {
        Self {
            writes: AtomicU64::new(0),
            total_ns: std::array::from_fn(|_| AtomicU64::new(0)),
            slowest_ns: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }
}

impl PhaseTotals {
    pub(super) fn add(&self, times: &WriteTimes) {
        self.writes.fetch_add(1, Ordering::Relaxed);
        for (index, ns) in times.nanos().into_iter().enumerate() {
            self.total_ns[index].fetch_add(ns, Ordering::Relaxed);
            self.slowest_ns[index].fetch_max(ns, Ordering::Relaxed);
        }
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
