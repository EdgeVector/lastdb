use super::HomeJournal;
use crate::disk_pack::maintenance::{MaintenanceReceipt, SpacePolicy};
use crate::Result;

impl HomeJournal {
    /// Publish packed replacements and retain every source file. The node installs
    /// the new view before a separate reclaim call can release old generations.
    pub fn pack(&mut self, policy: &SpacePolicy) -> Result<MaintenanceReceipt> {
        self.writer.pack(policy)
    }
}
