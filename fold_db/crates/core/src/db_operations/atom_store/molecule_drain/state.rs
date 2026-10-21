//! What the drain reports: totals since the node started, and the molecules it
//! refused. Memory only; the durable state of the drain is the candidate list.

use super::judge::Refusal;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// Refused molecules the report names, at most.
const REFUSED_SAMPLE: usize = 8;

/// A candidate that the drain refused, for the report.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RefusedMolecule {
    /// The storage scope, empty for the personal scope.
    pub scope: String,
    pub molecule: String,
    /// `header_suspect`, `bindings_suspect`, `counters_unset` or
    /// `binding_record_without_counter`.
    pub reason: String,
}

/// The report of the molecule drain, for `lastdb status` and the drain route.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MoleculeDrainReport {
    /// The wake interval of the drain task in milliseconds, 0 when it is off.
    pub interval_ms: u64,
    pub runs: u64,
    pub last_run_unix_ms: u64,
    /// Candidates the drain holds after its last pass: not finished, not parked.
    pub pending: u64,
    /// Candidates the drain parked: they wait for kept conflict records or were
    /// refused.
    pub parked: u64,
    /// Parked candidates that wait for kept conflict records to end.
    pub waiting_records: u64,
    /// Parked candidates that a counter or a record keeps from a drain.
    pub refused: u64,
    /// Molecules whose family the drain removed since the node started.
    pub drained: u64,
    /// Tip records the drain erased through the planner.
    pub tips_erased: u64,
    /// Other rows the drain deleted (generation, delete barrier, header).
    pub rows_removed: u64,
    /// Atom bodies deleted by the counter when the drain lowered it to zero.
    pub atoms_deleted: u64,
    /// Batches the drain wrote.
    pub batches: u64,
    /// Candidates the drain dropped because the molecule was bound again.
    pub rebound: u64,
    pub consecutive_failures: u64,
    pub next_retry_unix_ms: u64,
    pub last_error: Option<String>,
    /// The first refused molecules.
    pub refused_sample: Vec<RefusedMolecule>,
}

/// The shared state of one store's drain: counters, the wake signal, and the
/// samples. Cloned by reference through `Arc`.
#[derive(Debug, Default)]
pub(crate) struct DrainState {
    interval_ms: AtomicU64,
    runs: AtomicU64,
    last_run_unix_ms: AtomicU64,
    pending: AtomicU64,
    waiting_records: AtomicU64,
    refused: AtomicU64,
    drained: AtomicU64,
    tips_erased: AtomicU64,
    rows_removed: AtomicU64,
    atoms_deleted: AtomicU64,
    batches: AtomicU64,
    rebound: AtomicU64,
    consecutive_failures: AtomicU64,
    next_retry_unix_ms: AtomicU64,
    last_error: Mutex<Option<String>>,
    refused_sample: Mutex<Vec<RefusedMolecule>>,
    wake: Arc<tokio::sync::Notify>,
}

impl DrainState {
    /// The signal that tells the drain task to look at the candidate list now.
    pub(crate) fn wake_signal(&self) -> Arc<tokio::sync::Notify> {
        Arc::clone(&self.wake)
    }

    pub(crate) fn set_interval_ms(&self, interval_ms: u64) {
        self.interval_ms.store(interval_ms, Ordering::Relaxed);
    }

    /// One batch of the drain.
    pub(crate) fn note_batch(&self, tips: u64, rows: u64, atoms_deleted: u64) {
        self.batches.fetch_add(1, Ordering::Relaxed);
        self.tips_erased.fetch_add(tips, Ordering::Relaxed);
        self.rows_removed.fetch_add(rows, Ordering::Relaxed);
        self.atoms_deleted
            .fetch_add(atoms_deleted, Ordering::Relaxed);
    }

    pub(crate) fn note_drained(&self) {
        self.drained.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn note_rebound(&self) {
        self.rebound.fetch_add(1, Ordering::Relaxed);
    }

    /// The numbers of the candidates the drain holds after a pass.
    pub(crate) fn set_gauges(&self, pending: u64, waiting_records: u64, refused: u64) {
        self.pending.store(pending, Ordering::Relaxed);
        self.waiting_records
            .store(waiting_records, Ordering::Relaxed);
        self.refused.store(refused, Ordering::Relaxed);
    }

    /// Keep the first refused molecules for the report.
    pub(crate) fn set_refused_sample(&self, sample: Vec<(String, String, Refusal)>) {
        let sample = sample
            .into_iter()
            .take(REFUSED_SAMPLE)
            .map(|(scope, molecule, why)| RefusedMolecule {
                scope,
                molecule,
                reason: why.name().to_string(),
            })
            .collect();
        *self.refused_sample.lock().expect("drain refused sample") = sample;
    }

    /// One wake of the task ended.
    pub(crate) fn note_run(&self, error: Option<String>, failures: u64, retry_in_ms: Option<u64>) {
        self.runs.fetch_add(1, Ordering::Relaxed);
        self.last_run_unix_ms
            .store(crate::clock::unix_millis(), Ordering::Relaxed);
        self.consecutive_failures.store(failures, Ordering::Relaxed);
        self.next_retry_unix_ms.store(
            retry_in_ms.map_or(0, |ms| crate::clock::unix_millis().saturating_add(ms)),
            Ordering::Relaxed,
        );
        *self.last_error.lock().expect("drain last error") = error;
    }

    /// The report now.
    pub(crate) fn snapshot(&self) -> MoleculeDrainReport {
        let waiting = self.waiting_records.load(Ordering::Relaxed);
        let refused = self.refused.load(Ordering::Relaxed);
        MoleculeDrainReport {
            interval_ms: self.interval_ms.load(Ordering::Relaxed),
            runs: self.runs.load(Ordering::Relaxed),
            last_run_unix_ms: self.last_run_unix_ms.load(Ordering::Relaxed),
            pending: self.pending.load(Ordering::Relaxed),
            parked: waiting + refused,
            waiting_records: waiting,
            refused,
            drained: self.drained.load(Ordering::Relaxed),
            tips_erased: self.tips_erased.load(Ordering::Relaxed),
            rows_removed: self.rows_removed.load(Ordering::Relaxed),
            atoms_deleted: self.atoms_deleted.load(Ordering::Relaxed),
            batches: self.batches.load(Ordering::Relaxed),
            rebound: self.rebound.load(Ordering::Relaxed),
            consecutive_failures: self.consecutive_failures.load(Ordering::Relaxed),
            next_retry_unix_ms: self.next_retry_unix_ms.load(Ordering::Relaxed),
            last_error: self.last_error.lock().expect("drain last error").clone(),
            refused_sample: self
                .refused_sample
                .lock()
                .expect("drain refused sample")
                .clone(),
        }
    }
}
