use super::JournalWriter;
use crate::disk_pack::maintenance::{MaintenanceReceipt, SpacePolicy, SpaceReservation};
use crate::Result;

impl JournalWriter {
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
        self.publish()?;
        self.poisoned = true;
        let (current, receipt) = crate::disk_pack::maintenance::compact_locked(&self.lock, policy)?;
        self.current = current;
        self.poisoned = false;
        Ok(receipt)
    }

    /// Release only obsolete unreferenced parts; never implicitly perform a rewrite.
    pub fn reclaim(&mut self, policy: &SpacePolicy) -> Result<MaintenanceReceipt> {
        self.publish()?;
        crate::disk_pack::maintenance::reclaim_locked(&self.lock, policy)
    }

    /// Reserve all native output bytes while this handle owns the writer lease.
    pub fn reserve_space(&self, bytes: u64, policy: &SpacePolicy) -> Result<SpaceReservation> {
        self.check_admission()?;
        crate::disk_pack::maintenance::reserve_space(&self.root, policy, bytes)
    }
}
