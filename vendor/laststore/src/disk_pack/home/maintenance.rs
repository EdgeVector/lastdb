use super::HomeJournal;
use crate::disk_pack::maintenance::{MaintenanceReceipt, SpacePolicy};
use crate::Result;

impl HomeJournal {
    /// Complete file count from the verified current receipt.
    pub fn dependency_parts(&self) -> Result<u64> {
        self.writer.dependency_parts()
    }

    /// Replace metadata only, retaining the original payload bytes.
    pub fn repack_lookups(&mut self, policy: &SpacePolicy) -> Result<MaintenanceReceipt> {
        self.writer.repack_lookups(policy)
    }
    /// True only when a publication requires recovery before admission.
    pub fn is_poisoned(&self) -> bool {
        self.writer.is_poisoned()
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
