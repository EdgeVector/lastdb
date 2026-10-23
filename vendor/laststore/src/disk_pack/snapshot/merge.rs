//! A physical rewrite joins a normal snapshot's single durability boundary.
use super::{SnapshotMutation, SnapshotReservation, SnapshotWriter};
use crate::disk_pack::maintenance::{MaintenanceReceipt, MergeDelta, PreparedCompaction};
use crate::disk_pack::{corrupt, PublishedGeneration};
use crate::Result;

pub(super) struct MergePublication<'a> {
    pub(super) output: &'a mut PreparedCompaction,
    pub(super) delta: MergeDelta,
}

impl SnapshotWriter {
    pub(in crate::disk_pack) fn commit_reserved_merged(
        &mut self,
        mutations: Vec<SnapshotMutation>,
        reservation: SnapshotReservation,
        output: &mut PreparedCompaction,
        delta: MergeDelta,
    ) -> Result<(PublishedGeneration, MaintenanceReceipt)> {
        if !output.belongs_to(&self.lock)
            || !output.has_identity(delta.identity)
            || delta.expected != self.current
        {
            return Err(corrupt(
                "compaction merge no longer matches its writer root",
            ));
        }
        let changed = (delta.mutations.len() as u64).saturating_add(mutations.len() as u64);
        crate::commit_timing::count("compaction_merge_locations", delta.mutations.len() as u64);
        let publication = self.commit_reserved_base(
            mutations,
            reservation,
            Some(MergePublication { output, delta }),
        )?;
        let mut receipt = output.receipt();
        receipt.catch_up_records = receipt.catch_up_records.saturating_add(changed);
        receipt.generation = publication.current.generation;
        receipt.parts_after = publication.dependency_parts;
        self.legacy_live = None;
        self.compaction.completed(&receipt);
        Ok((publication, receipt))
    }
}
