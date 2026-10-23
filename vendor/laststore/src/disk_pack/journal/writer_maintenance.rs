use super::JournalWriter;
use crate::disk_pack::maintenance::{MaintenanceReceipt, SpacePolicy, SpaceReservation};
use crate::Result;

impl JournalWriter {
    /// Current complete retained file count, without a filesystem walk.
    pub fn dependency_parts(&self) -> Result<u64> {
        Ok(crate::disk_pack::publish::ensure_generation(&self.lock, &self.current)?.dependencies)
    }

    /// Replace only lookup pages. Payload objects and their versions stay intact.
    pub fn repack_lookups(&mut self, policy: &SpacePolicy) -> Result<MaintenanceReceipt> {
        self.publish()?;
        self.poisoned = true;
        let (current, receipt) =
            match crate::disk_pack::maintenance::repack_locked(&self.lock, policy) {
                Ok(packed) => packed,
                Err(error) => {
                    self.poisoned = !crate::disk_pack::artifact::current(&self.root)
                        .is_ok_and(|disk| disk == self.current);
                    return Err(error);
                }
            };
        self.current = current;
        self.poisoned = false;
        Ok(receipt)
    }
    /// An uncertain publication requires a reopen before more writes.
    pub fn is_poisoned(&self) -> bool {
        self.poisoned
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
        self.publish()?;
        self.poisoned = true;
        let (current, receipt) =
            match crate::disk_pack::maintenance::compact_locked(&self.lock, policy) {
                Ok(packed) => packed,
                Err(error) => {
                    let disk = crate::disk_pack::artifact::current(&self.root);
                    self.poisoned = !disk.is_ok_and(|disk| disk == self.current);
                    return Err(error);
                }
            };
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
