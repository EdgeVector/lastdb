//! One atomic disk root and immutable accepted-write overlay.

use super::*;

/// A read-only operational boundary; this method never wakes a flush worker.
#[derive(serde::Serialize)]
pub struct NativeDiskStatus {
    pub generation: u64,
    pub frontier: u64,
    pub accepted_batch: bool,
    pub writer_failed: bool,
}

#[derive(Clone)]
pub(super) struct NativeReadView {
    pub(super) current: Current,
    pub(super) accepted: Option<Arc<transaction::PendingBatch>>,
}

impl PackRuntime {
    pub fn status_native(&self) -> NativeDiskStatus {
        let view = self.view.read().expect("native disk status view");
        NativeDiskStatus {
            generation: view.current.generation,
            frontier: view.current.frontier,
            accepted_batch: view.accepted.is_some(),
            writer_failed: self.failed.load(Ordering::Acquire),
        }
    }

    /// The caller owns the write gate and complete physical/logical reservations.
    /// The batch and its protection stay immutable until the last reader releases it.
    pub(super) fn install_accepted(
        &self,
        batch: Arc<transaction::PendingBatch>,
    ) -> StorageResult<()> {
        let mut view = self.view.write().expect("native complete read view");
        if self.failed.load(Ordering::Acquire) || view.accepted.is_some() {
            return Err(StorageError::BackendError(
                "native accepted view requires a healthy writer and no older accepted batch".into(),
            ));
        }
        view.accepted = Some(batch);
        Ok(())
    }

    /// Publication removes only this runtime's overlay; older calls keep their Arc.
    pub(super) fn install_committed(&self, current: Current) {
        let mut view = self.view.write().expect("native complete read view");
        self.published.store(current.frontier, Ordering::Release);
        *view = NativeReadView {
            current,
            accepted: None,
        };
    }
}
