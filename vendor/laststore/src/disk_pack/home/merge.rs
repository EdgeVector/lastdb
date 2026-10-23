//! Admit an optional compacted base without consuming an accepted client ticket.
use super::{HomeMutation, HomeSnapshot, MutationQuote, MutationReservation};
use crate::disk_pack::maintenance::{MaintenanceReceipt, MergeDelta, PreparedCompaction};
use crate::disk_pack::{corrupt, PublishedGeneration, MAX_DEPENDENCY_PARTS};
use crate::Result;

/// Keeps the exact latest source alive through the fused normal publication.
pub struct PackMerge {
    pub(in crate::disk_pack) delta: MergeDelta,
    reservation: u64,
    quote: MutationQuote,
}

impl HomeSnapshot {
    /// Only bounded metadata reads and extra quota occur here. A refusal leaves
    /// the original client operations and reservation untouched.
    pub fn prepare_pack_merge(
        &self,
        output: &mut PreparedCompaction,
        reservation: &MutationReservation,
    ) -> Result<PackMerge> {
        if !output.belongs_to(&self.writer.lock) {
            return Err(corrupt("compaction merge belongs to another writer lease"));
        }
        let incoming = reservation.quote.payload().operations();
        let limit = self
            .current()
            .batch_operation_limit()
            .saturating_sub(incoming);
        let delta = output.merge_delta(self.writer.capture_pack_root()?, limit)?;
        let (parts, bytes) = if delta.mutations.is_empty() {
            (0, 0)
        } else {
            let quote = MutationQuote::locator_delta(
                &delta.mutations,
                self.current().batch_operation_limit(),
            )?;
            (quote.dependency_parts(), quote.physical_bytes())
        };
        // A delta can import payloads and block maps from the latest source.
        // Counting that complete source is conservative and avoids a new walk.
        let bound = output
            .dependency_parts()
            .saturating_add(self.dependency_parts())
            .saturating_add(parts)
            .saturating_add(reservation.quote.dependency_parts());
        if bound > MAX_DEPENDENCY_PARTS {
            return Err(crate::Error::WriteCapacity(
                "compaction_behind: merged publication exceeds dependency headroom".into(),
            ));
        }
        output.reserve_merge(bytes)?;
        Ok(PackMerge {
            delta,
            reservation: reservation.payload.identity(),
            quote: reservation.quote,
        })
    }

    /// Publish the client's new mutation frontier and the physical rewrite once.
    pub fn commit_mutation_merged(
        &mut self,
        operations: Vec<HomeMutation>,
        reservation: MutationReservation,
        output: &mut PreparedCompaction,
        merge: PackMerge,
    ) -> Result<(PublishedGeneration, MaintenanceReceipt)> {
        if merge.reservation != reservation.payload.identity() || merge.quote != reservation.quote {
            return Err(corrupt(
                "compaction merge reservation identity or dimensions changed",
            ));
        }
        self.validate_mutation(&operations, &reservation)?;
        self.writer.commit_reserved_merged(
            self.address(operations)?,
            reservation.payload,
            output,
            merge.delta,
        )
    }
}
