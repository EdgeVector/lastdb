use super::HomeSnapshot;
use crate::disk_pack::maintenance::{MaintenanceReceipt, SpacePolicy};
use crate::Result;

impl HomeSnapshot {
    /// Conditional publication performs no catch-up or receipt construction.
    pub fn try_finish_pack(
        &mut self,
        output: &mut crate::disk_pack::maintenance::PreparedCompaction,
        staged: &mut crate::disk_pack::maintenance::CompactionPublication,
    ) -> Result<Option<MaintenanceReceipt>> {
        self.writer.try_finish_pack(output, staged)
    }

    /// Capture the installed deletion floor before the node releases its write gate.
    pub fn capture_reclaim(&self) -> Result<crate::disk_pack::snapshot::ReclaimPlan> {
        self.writer.capture_reclaim()
    }

    /// Read calls keep this typed writer lease through their last file access.
    pub fn read_lease(&self) -> crate::disk_pack::lifetime::WriterReadLease {
        self.writer.read_lease()
    }

    /// Publish packed replacements and retain every source file. The node installs
    /// the new view before a separate reclaim call can release old generations.
    pub fn pack(&mut self, policy: &SpacePolicy) -> Result<MaintenanceReceipt> {
        self.writer.pack(policy)
    }
}

impl HomeSnapshot {
    /// Capture a protected source for concurrent compaction.
    pub fn prepare_pack(
        &self,
        full_copy: bool,
    ) -> Result<crate::disk_pack::maintenance::CompactionPlan> {
        self.writer.prepare_pack(full_copy)
    }
    /// Capture a protected current root for a catch-up round.
    pub fn capture_pack_root(&self) -> Result<crate::disk_pack::lifetime::GenerationHold> {
        self.writer.capture_pack_root()
    }
    /// Publish completed private output after a bounded final catch-up.
    pub fn finish_pack(
        &mut self,
        output: crate::disk_pack::maintenance::PreparedCompaction,
    ) -> Result<MaintenanceReceipt> {
        self.writer.finish_pack(output)
    }
}
