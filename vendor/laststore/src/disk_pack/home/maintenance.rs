use super::HomeJournal;
use crate::disk_pack::maintenance::{MaintenanceReceipt, SpacePolicy};
use crate::Result;

impl HomeJournal {
    /// Capture the installed deletion floor before the node releases its write gate.
    pub fn capture_reclaim(&self) -> Result<crate::disk_pack::journal::ReclaimPlan> {
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
