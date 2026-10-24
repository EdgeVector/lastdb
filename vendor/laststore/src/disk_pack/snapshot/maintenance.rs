use super::SnapshotWriter;
use crate::disk_pack::maintenance::{MaintenanceReceipt, SpacePolicy, SpaceReservation};
use crate::Result;

impl SnapshotWriter {
    /// Caller checks the queue under its publication boundary. A stale root or
    /// numeric receipt collision requests off-gate reprepare without any copy.
    pub fn try_finish_pack(
        &mut self,
        output: &mut crate::disk_pack::maintenance::PreparedCompaction,
        staged: &mut crate::disk_pack::maintenance::CompactionPublication,
    ) -> Result<Option<MaintenanceReceipt>> {
        self.check_admission()?;
        output.ensure_usable()?;
        if !output.belongs_to(&self.lock) || !staged.belongs_to(output) {
            return Err(crate::disk_pack::corrupt(
                "compaction belongs to another writer lease",
            ));
        }
        if self.current != staged.expected {
            return Ok(None);
        }
        let previous = self.current.clone();
        self.poisoned = true;
        let installed =
            crate::durability::complete_native_commit(&self.root, || staged.install(&self.lock));
        let installed = match installed {
            Ok(installed) => installed,
            Err(error) => {
                self.poisoned = !crate::disk_pack::artifact::current(&self.root)
                    .is_ok_and(|disk| disk == previous);
                return Err(error);
            }
        };
        if !installed {
            self.poisoned = false;
            return Ok(None);
        }
        if let Err(error) = self.lock.remember_checked(&staged.current) {
            self.poisoned = true;
            return Err(error);
        }
        self.current = staged.current.clone();
        self.changes.rewritten(&previous, &self.current);
        self.dependencies = staged.parts;
        self.legacy_live = None;
        let mut receipt = output.receipt();
        receipt.generation = self.current.generation;
        receipt.parts_after = self.dependencies;
        self.compaction.completed(&receipt);
        self.poisoned = false;
        Ok(Some(receipt))
    }

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
        let previous = self.current.clone();
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
        self.changes.rewritten(&previous, &self.current);
        self.dependencies = receipt.parts_after;
        self.legacy_live = None;
        self.poisoned = false;
        Ok(receipt)
    }

    /// Release only obsolete unreferenced parts; never implicitly perform a rewrite.
    pub fn reclaim(&mut self, policy: &SpacePolicy) -> Result<MaintenanceReceipt> {
        self.check_admission()?;
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

    /// Register exact changes at the same cut used by off-gate preparation.
    pub fn capture_pack_handoff(
        &self,
        output: &mut crate::disk_pack::maintenance::PreparedCompaction,
    ) -> Result<(
        crate::disk_pack::lifetime::GenerationHold,
        super::RetiredCaptures,
    )> {
        self.check_admission()?;
        if !output.belongs_to(&self.lock) {
            return Err(crate::disk_pack::corrupt(
                "change capture belongs to another writer",
            ));
        }
        output.require_no_merge_capture()?;
        let cut = self.capture_pack_root()?;
        let (capture, retired) = self.changes.register(&self.current)?;
        output.set_merge_capture(capture);
        Ok((cut, retired))
    }

    /// The node retires abandoned capture arrays outside its write gate.
    pub fn capture_cleanup(&self) -> super::CaptureCleanup {
        self.changes.cleanup_handle()
    }

    /// Synchronous fallback: replay a bounded final delta, seed complete live
    /// counts, and publish while the caller owns the write boundary. The node's
    /// staged path seeds before it takes the final publication boundary.
    pub fn finish_pack(
        &mut self,
        mut output: crate::disk_pack::maintenance::PreparedCompaction,
    ) -> Result<MaintenanceReceipt> {
        self.check_admission()?;
        output.catch_up_bounded(self.capture_pack_root()?, 4096)?;
        let previous = self.current.clone();
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
        self.changes.rewritten(&previous, &self.current);
        self.dependencies = receipt.parts_after;
        self.legacy_live = None;
        self.poisoned = false;
        Ok(receipt)
    }
}
