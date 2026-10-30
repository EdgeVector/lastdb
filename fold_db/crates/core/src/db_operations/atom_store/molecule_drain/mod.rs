//! The molecule drain: the resumable removal of a dead molecule (step S7).
//!
//! A molecule is **dead** in a storage scope when nothing names it: the
//! `bindings` counter is zero and no binding record exists, and no counter is
//! `suspect` or missing. The counter is the only authority. A dead molecule may
//! still hold tip records (its schema was dropped) and kept conflict records. The
//! drain erases the tips through the atom counter planner, waits for the kept
//! records to end, and then removes the rows the molecule owns.
//!
//! - **Trigger.** The batch that lowers `bindings` to zero also writes a
//!   candidate row (`candidate.rs`). A crash cannot split the two. The candidate
//!   list is one range under one hash, so a restart finds every candidate
//!   without a scan.
//! - **Steps.** One step is one batch (`step.rs`). The tip phase erases a bounded
//!   page of `mk:` keys. The row phase deletes bounded pages of the rows that
//!   carry no count. The final batch deletes the header and the candidate row.
//! - **Gate.** Each batch reads the header, the `bindings` counter and the
//!   binding records again inside the write gate (`judge.rs`) and stops if
//!   anything names the molecule. A molecule that is `suspect`, has no counters,
//!   or has a binding record with no counter is never drained. It stays a
//!   candidate and is reported.
//! - **No raw writer.** Every write goes through the planner (`commit_tip_changes`).
//!   This module holds no write verb of its own.
//! - **Owner.** [`MoleculeDrain`] owns the memory state of the drain: the report
//!   and the wake signal of its task. `AtomStore` holds one owner by composition
//!   and hands it out; no `impl AtomStore` block lives in this module, and no
//!   other code reads or writes the state. The drain works on a store that the
//!   caller names (`&AtomStore`) and uses only the store interface: the write
//!   scopes, the bounded range reads, the binding reads, the release plan of the
//!   retention module, and the planner. Each batch of the drain is a
//!   `TipChangeSet` value that the planner writes as one batch.
//! - **Rows and wake from other writers.** The unbind that takes `bindings` to
//!   zero adds the candidate row to its own batch as a value (`Candidate::row`).
//!   It then asks the owner to wake the task after that batch commits
//!   ([`MoleculeDrain::wake_after_commit`]). The owner subscribes its wake signal
//!   to the commit of the current request through
//!   `AtomStore::notify_after_commit`, which knows nothing about the drain.
//!
//! Design: `docs/design-lastdb-counter-lifecycle.md` section 12.
//! Contract: `docs/lastdb-molecule-drain.md`.

mod batch;
mod candidate;
mod cursor;
mod enqueue;
mod family;
mod judge;
mod state;
mod step;

pub(crate) use candidate::{valid_candidate_key, Candidate};
pub(crate) use cursor::DrainCursor;
pub use enqueue::MoleculeDrainVerdict;
pub use state::{MoleculeDrainReport, RefusedMolecule};
pub(crate) use step::{DrainLimits, OutcomeKind};

use super::AtomStore;
use state::DrainState;
use std::sync::Arc;

/// The owner of the molecule drain.
///
/// It holds the memory state of the drain: the totals and gauges of the report,
/// the refused sample, and the wake signal of the task. The state is private to
/// this module. Clones share one state through `Arc`, so every clone of the
/// store that holds the owner sees the same report and wakes the same task.
#[derive(Debug, Clone, Default)]
pub struct MoleculeDrain {
    state: Arc<DrainState>,
}

impl MoleculeDrain {
    /// The drain report now.
    #[must_use]
    pub fn report(&self) -> MoleculeDrainReport {
        self.state.snapshot()
    }

    /// The signal that the drain task waits on.
    pub(crate) fn wake_signal(&self) -> Arc<tokio::sync::Notify> {
        self.state.wake_signal()
    }

    /// Wake the drain task when the batch of the current request on `store`
    /// commits. Outside a native request scope the task wakes at once.
    pub(crate) fn wake_after_commit(&self, store: &AtomStore) {
        store.notify_after_commit(&self.state.wake_signal());
    }

    /// True when no drain task runs: the interval was never set.
    pub(crate) fn task_is_off(&self) -> bool {
        self.state.task_is_off()
    }

    /// The interval of the drain task, set when the task starts.
    pub(crate) fn set_interval_ms(&self, interval_ms: u64) {
        self.state.set_interval_ms(interval_ms);
    }

    /// One wake of the task ended.
    pub(crate) fn note_run(&self, error: Option<String>, failures: u64, retry_in_ms: Option<u64>) {
        self.state.note_run(error, failures, retry_in_ms);
    }

    /// Put the gauges and the refused molecules of the task cursor in the report.
    pub(crate) fn publish(&self, cursor: &DrainCursor) {
        let (pending, waiting, refused) = cursor.gauges();
        self.state.set_gauges(pending, waiting, refused);
        self.state.set_refused_sample(cursor.refused());
    }
}
