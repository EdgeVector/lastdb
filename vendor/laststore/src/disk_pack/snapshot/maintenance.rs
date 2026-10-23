use super::SnapshotWriter;
use crate::disk_pack::maintenance::{MaintenanceReceipt, SpacePolicy, SpaceReservation};
use crate::Result;

impl SnapshotWriter {
    /// Retain this writer's lease and its read/reclaim gate, without file access.
    pub fn read_lease(&self) -> crate::disk_pack::lifetime::WriterReadLease {
        crate::disk_pack::lifetime::WriterReadLease::new(std::sync::Arc::clone(&self.lock))
    }

    /// Pack live stored records through the same publication lease, then reclaim safely.
    pub fn compact(&mut self, policy: &SpacePolicy) -> Result<MaintenanceReceipt> {
        let mut receipt = self.pack(policy)?;
        let reclaim = crate::disk_pack::maintenance::reclaim_locked(&self.lock, policy)?;
        receipt.files_removed = reclaim.files_removed;
        receipt.bytes_removed = reclaim.bytes_removed;
        receipt.reclaim_deferred = reclaim.reclaim_deferred;
        Ok(receipt)
    }

    /// Publish replacements without deletion. A node installs this root in its
    /// read view before it calls `reclaim` under that view's exclusive lock.
    pub fn pack(&mut self, policy: &SpacePolicy) -> Result<MaintenanceReceipt> {
        self.check_admission()?;
        if self
            .lock
            .compaction_active
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(crate::Error::WriteCapacity(
                "compaction already active".into(),
            ));
        }
        self.poisoned = true;
        let packed = crate::disk_pack::maintenance::compact_locked(&self.lock, policy);
        let (current, receipt) = match packed {
            Ok(packed) => packed,
            Err(error) => {
                // One atomic rename publishes the replacement, after every output
                // barrier. A CURRENT that still names this root means the failed
                // output is unreferenced, so the writer keeps admitting work.
                let disk = crate::disk_pack::artifact::current(&self.root);
                self.poisoned = !disk.is_ok_and(|disk| disk == self.current);
                return Err(error);
            }
        };
        self.compaction.completed(&receipt);
        self.current = current;
        self.dependencies = receipt.parts_after;
        self.poisoned = false;
        Ok(receipt)
    }

    /// Release only obsolete unreferenced parts; never implicitly perform a rewrite.
    pub fn reclaim(&mut self, policy: &SpacePolicy) -> Result<MaintenanceReceipt> {
        self.check_admission()?;
        if self
            .lock
            .compaction_active
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return Ok(MaintenanceReceipt {
                generation: self.current.generation,
                reclaim_deferred: true,
                ..Default::default()
            });
        }
        crate::disk_pack::maintenance::reclaim_locked(&self.lock, policy)
    }

    /// Remove part files that no receipt names, such as the output of a publication
    /// or a compaction that a crash interrupted. Files younger than `policy.grace_ms` stay.
    pub fn reclaim_orphans(&mut self, policy: &SpacePolicy) -> Result<MaintenanceReceipt> {
        self.check_admission()?;
        crate::disk_pack::maintenance::sweep_orphans_locked(&self.lock, policy)
    }

    /// Reserve all native output bytes while this handle owns the writer lease.
    pub fn reserve_space(&self, bytes: u64, policy: &SpacePolicy) -> Result<SpaceReservation> {
        self.check_admission()?;
        crate::disk_pack::maintenance::reserve_space(&self.root, policy, bytes)
    }
}

impl SnapshotWriter {
    /// Capture a source without retaining the writer mutex during the copy.
    pub fn prepare_pack(
        &self,
        full_copy: bool,
    ) -> Result<crate::disk_pack::maintenance::CompactionPlan> {
        self.check_admission()?;
        crate::disk_pack::maintenance::CompactionPlan::capture(
            std::sync::Arc::clone(&self.lock),
            &self.current,
            if full_copy {
                Default::default()
            } else {
                self.compaction
            },
        )
    }

    /// Capture a protected current root for an off-gate catch-up round.
    pub fn capture_pack_root(&self) -> Result<crate::disk_pack::lifetime::GenerationHold> {
        self.check_admission()?;
        crate::disk_pack::lifetime::GenerationHold::from_writer(
            &self.read_lease(),
            &self.root,
            &self.current,
        )
    }

    /// The caller owns the write boundary. Only a bounded final delta runs here.
    pub fn finish_pack(
        &mut self,
        mut output: crate::disk_pack::maintenance::PreparedCompaction,
    ) -> Result<MaintenanceReceipt> {
        self.check_admission()?;
        output.catch_up_bounded(self.capture_pack_root()?, 4096)?;
        self.poisoned = true;
        let result =
            crate::durability::complete_native_commit(&self.root, || output.finish(&self.lock));
        let (current, receipt) = match result {
            Ok(result) => result,
            Err(error) => {
                self.poisoned = !crate::disk_pack::artifact::current(&self.root)
                    .is_ok_and(|disk| disk == self.current);
                return Err(error);
            }
        };
        self.compaction.completed(&receipt);
        self.current = current;
        self.dependencies = receipt.parts_after;
        self.poisoned = false;
        Ok(receipt)
    }
}
