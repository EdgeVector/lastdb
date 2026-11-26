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
//!
//! A phase with a dotted name is a part of the phase before the dot, and is not added again to
//! `handler`. The parts of a phase need not add up to it; the rest is work no part names.
//! - `read_and_stage.capture`: taking the read view of the write on a blocking thread;
//! - `read_and_stage.old_records`: reading the old records and checking the conditions;
//! - `read_and_stage.stamps`: the local replay head, the author clocks and the field stamps;
//! - `read_and_stage.spool_entry`: encoding and signing the spool entry of a captured write;
//! - `read_and_stage.stage`: staging the new records and their alternate entries;
//! - `commit.queue`: the space and queue checks, and joining the commit queue;
//! - `commit.turn_wait`: until the queue turn that commits the write holds both gates (another
//!   commit, such as a spool confirmation of the sync loop, runs first here);
//! - `commit.spool_records`: the spool records of the captured writes;
//! - `commit.log_flush`: the commit log append and its ordered flush;
//! - `commit.install`: publishing the spool head, the read view and the metadata;
//! - `commit.deliver`: post-commit work of the writes in the turn;
//! - `commit.wake`: from the end of the turn until the write runs again.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, PoisonError};
use std::time::Duration;

use fold_db::db_operations::WritePhases;
use serde_json::{json, Value};

const PHASES: usize = 19;

const NAMES: [&str; PHASES] = [
    "parse",
    "prepare",
    "lock_wait",
    "read_and_stage",
    "read_and_stage.capture",
    "read_and_stage.old_records",
    "read_and_stage.stamps",
    "read_and_stage.spool_entry",
    "read_and_stage.stage",
    "commit",
    "commit.queue",
    "commit.turn_wait",
    "commit.spool_records",
    "commit.log_flush",
    "commit.install",
    "commit.deliver",
    "commit.wake",
    "after_commit",
    "handler",
];

/// The phases of one committed write. `core` holds the phases of the write scope and their
/// parts.
#[derive(Default, Clone, Copy)]
pub(crate) struct WriteTimes {
    pub(crate) parse: Duration,
    pub(crate) prepare: Duration,
    pub(crate) core: WritePhases,
    pub(crate) after_commit: Duration,
    pub(crate) handler: Duration,
}

impl WriteTimes {
    fn nanos(&self) -> [u64; PHASES] {
        let core = &self.core;
        [
            self.parse,
            self.prepare,
            core.lock_wait,
            core.read_and_stage,
            core.capture,
            core.old_records,
            core.stamps,
            core.spool_entry,
            core.stage,
            core.commit,
            core.queue,
            core.turn_wait,
            core.spool_records,
            core.log_flush,
            core.install,
            core.deliver,
            core.wake,
            self.after_commit,
            self.handler,
        ]
        .map(|phase| u64::try_from(phase.as_nanos()).unwrap_or(u64::MAX))
    }
}

pub(super) struct PhaseTotals {
    writes: AtomicU64,
    total_ns: [AtomicU64; PHASES],
    slowest_ns: [AtomicU64; PHASES],
    /// The slowest value of each phase since the last history sample.
    recent_slowest_ns: [AtomicU64; PHASES],
    /// `writes`, then each `total_ns`, at the last history sample.
    mark: Mutex<[u64; PHASES + 1]>,
}

impl Default for PhaseTotals {
    fn default() -> Self {
        Self {
            writes: AtomicU64::new(0),
            total_ns: std::array::from_fn(|_| AtomicU64::new(0)),
            slowest_ns: std::array::from_fn(|_| AtomicU64::new(0)),
            recent_slowest_ns: std::array::from_fn(|_| AtomicU64::new(0)),
            mark: Mutex::new([0; PHASES + 1]),
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
