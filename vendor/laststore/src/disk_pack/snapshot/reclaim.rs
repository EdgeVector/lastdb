//! Delete exact obsolete paths below a captured installed root without a writer mutex.

use super::SnapshotWriter;
use crate::disk_pack::maintenance::{self, MaintenanceReceipt, SpacePolicy};
use crate::disk_pack::{write_lock::WriteLock, Current};
use crate::Result;
use std::sync::Arc;

/// An installed deletion floor; newer output is never part of its deletion plan.
pub struct ReclaimPlan {
    lock: Arc<WriteLock>,
    current: Current,
    _hold: crate::disk_pack::lifetime::GenerationHold,
}

impl SnapshotWriter {
    /// The node captures this root under its view boundary and serializes
    /// maintenance callers. Later ordinary snapshot publications may proceed.
    pub fn capture_reclaim(&self) -> Result<ReclaimPlan> {
        self.check_admission()?;
        if self
            .lock
            .compaction_active
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(crate::Error::WriteCapacity(
                "compaction owns the maintenance lease".into(),
            ));
        }
        Ok(ReclaimPlan {
            lock: Arc::clone(&self.lock),
            current: self.current.clone(),
            _hold: crate::disk_pack::lifetime::GenerationHold::from_writer(
                &self.read_lease(),
                &self.root,
                &self.current,
            )?,
        })
    }
}

impl ReclaimPlan {
    /// Plan and remove only obsolete older paths while new calls can continue.
    pub fn execute(self, policy: &SpacePolicy, orphans: bool) -> Result<MaintenanceReceipt> {
        if orphans {
            maintenance::sweep_orphans_at(&self.lock, policy, self.current, true)
        } else {
            maintenance::reclaim_at(&self.lock, policy, self.current, true)
        }
    }
}
