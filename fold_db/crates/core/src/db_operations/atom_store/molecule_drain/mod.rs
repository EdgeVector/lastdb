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
pub(crate) use enqueue::DrainingMolecules;
pub use enqueue::MoleculeDrainVerdict;
pub(crate) use state::DrainState;
pub use state::{MoleculeDrainReport, RefusedMolecule};
pub(crate) use step::{DrainLimits, DrainOutcome, OutcomeKind};

use super::ports::{
    BindingReads, CommitTips, DrainStates, MainRows, NativeRuntime, RangeWalks, RetentionReleases,
    WriteScopes,
};
use std::sync::Arc;

/// The ports the drain uses. It never names the store type.
pub(crate) trait DrainPorts:
    BindingReads
    + CommitTips
    + DrainStates
    + MainRows
    + NativeRuntime
    + RangeWalks
    + RetentionReleases
    + WriteScopes
{
}

impl<T> DrainPorts for T where
    T: BindingReads
        + CommitTips
        + DrainStates
        + MainRows
        + NativeRuntime
        + RangeWalks
        + RetentionReleases
        + WriteScopes
{
}

/// The molecule drain over a store that implements [`DrainPorts`].
pub(crate) struct MoleculeDrain<'s, S> {
    store: &'s S,
}

impl<'s, S: DrainPorts> MoleculeDrain<'s, S> {
    pub(crate) fn new(store: &'s S) -> Self {
        Self { store }
    }

    /// Wake the drain task when the batch of the current request is committed.
    /// Outside a native request scope the task wakes at once.
    pub(crate) fn wake_drain_after_commit(&self) {
        let signal: Arc<tokio::sync::Notify> = self.store.drain_state().wake_signal();
        let runtime = self.store.native_runtime();
        let Some(runtime) = runtime else {
            signal.notify_one();
            return;
        };
        let later = Arc::clone(&signal);
        let queued = crate::storage::disk_pack::transaction::after_commit(&runtime, async move {
            later.notify_one();
            Ok::<(), crate::storage::StorageError>(())
        });
        if queued.is_err() {
            signal.notify_one();
        }
    }
}
