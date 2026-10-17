//! One atomic disk root and immutable accepted-write overlay.

use super::*;

/// A read-only operational boundary; this method never wakes a flush worker.
#[derive(serde::Serialize)]
pub struct NativeDiskStatus {
    pub generation: u64,
    pub frontier: u64,
    pub accepted_batch: bool,
    pub accepted_batches: usize,
    pub accepted_sequence: u64,
    pub completed_sequence: u64,
    pub writer_failed: bool,
}

#[derive(Clone)]
pub(super) struct NativeReadView {
    pub(super) current: Current,
    pub(super) accepted: Arc<Vec<Arc<transaction::PendingBatch>>>,
    pub(super) revision: u64,
}

impl PackRuntime {
    pub fn status_native(&self) -> NativeDiskStatus {
        let queue = self.queue.lock().expect("native accepted queue");
        let view = self.view.read().expect("native disk status view");
        NativeDiskStatus {
            generation: view.current.generation,
            frontier: view.current.frontier,
            accepted_batch: !view.accepted.is_empty(),
            accepted_batches: view.accepted.len(),
            accepted_sequence: queue.sequence(),
            completed_sequence: *self.completed.borrow(),
            writer_failed: self.failed.load(Ordering::Acquire),
        }
    }

    /// The caller owns the write gate and complete physical/logical reservations.
    /// The batch and its protection stay immutable until the last reader releases it.
    pub(super) fn install_accepted(
        &self,
        batch: Arc<transaction::PendingBatch>,
    ) -> StorageResult<()> {
        let changes = batch.changed_coordinates();
        let mut view = self.view.write().expect("native complete read view");
        if self.failed.load(Ordering::Acquire) {
            return Err(StorageError::BackendError(
                "native accepted view requires a healthy writer".into(),
            ));
        }
        let revision = next_revision(&view)?;
        self.validity
            .lock()
            .expect("native dependency validity")
            .record(revision, changes);
        let mut accepted = view.accepted.as_ref().clone();
        accepted.push(batch);
        view.accepted = Arc::new(accepted);
        view.revision = revision;
        Ok(())
    }

    /// Publication removes only this runtime's overlay; older calls keep their Arc.
    pub(super) fn install_committed(
        &self,
        current: Current,
        hidden_changes: Vec<validity::ChangedCoordinate>,
    ) -> StorageResult<()> {
        let mut view = self.view.write().expect("native complete read view");
        if !hidden_changes.is_empty() {
            let revision = next_revision(&view)?;
            self.validity
                .lock()
                .expect("native dependency validity")
                .record(revision, hidden_changes);
            view.revision = revision;
        }
        self.published.store(current.frontier, Ordering::Release);
        view.current = current;
        view.accepted = Arc::new(Vec::new());
        Ok(())
    }
}

fn next_revision(view: &NativeReadView) -> StorageResult<u64> {
    view.revision
        .checked_add(1)
        .ok_or_else(|| StorageError::BackendError("native accepted revision exhausted".into()))
}
