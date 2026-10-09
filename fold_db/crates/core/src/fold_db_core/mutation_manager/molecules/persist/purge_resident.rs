//! Purge finalize replay and resident-graph turn completion for purge jobs.

use super::*;

impl MutationManager {
    pub(super) async fn replay_purge_finalize(&self, schema_name: &str) -> Result<(), SchemaError> {
        let started = std::time::Instant::now();
        let result = async {
            let mut schema = self
                .schema_manager
                .get_schema_metadata(schema_name)?
                .ok_or_else(|| {
                    SchemaError::InvalidData(format!(
                        "Schema '{schema_name}' not found for purge finalization replay"
                    ))
                })?;
            schema.sync_molecule_uuids();
            self.db_ops.store_schema(schema_name, &schema).await?;
            self.schema_manager.load_schema_internal(schema).await?;
            self.db_ops.flush().await.map_err(|error| {
                SchemaError::InvalidData(format!(
                    "Flush failed after purge finalization replay: {error}"
                ))
            })
        }
        .await;
        crate::request_phases::add_phase(
            crate::request_phases::RequestPhase::PurgeFinalize,
            started.elapsed(),
        );
        result
    }

    pub(super) async fn finish_purge_resident_turn(
        &self,
        job: &PurgeEnvelope,
        slot_revisions: &[crate::resident::PersistSlotRevision],
    ) -> Result<(), SchemaError> {
        // Cleanup can remove a completed slot control. Serialize it with the
        // entire apply transaction, not only its revision increment: resetting
        // a control between predecessor capture and apply loses the new turn.
        // Match molecule_lock_keys_for / sibling_molecule_lock_keys, which use
        // the physical molecule identity without a storage-prefix component.
        // All durable work finished before these short memory-only gates.
        let lock_keys = job
            .slots
            .iter()
            .map(|slot| {
                Self::molecule_persist_lock_key(
                    &slot.molecule_uuid,
                    Some(&slot.hash),
                    Some(&slot.range),
                    None,
                )
            })
            .chain(slot_revisions.iter().map(|ticket| {
                Self::molecule_persist_lock_key(
                    &ticket.molecule_uuid,
                    Some(&ticket.disk_hash),
                    Some(&ticket.disk_range),
                    None,
                )
            }))
            .collect();
        let _guards = self.acquire_molecule_write_lock_keys(lock_keys).await;
        self.complete_resident_persist_turn(slot_revisions)?;
        Self::evict_purged_resident(self.db_ops.resident(), slot_revisions);
        Self::clear_purge_overlays(self.db_ops.resident(), job);
        Ok(())
    }

    /// Drop T0 tips for this envelope before the overlay is forgotten.
    ///
    /// Apply already removes the tip when it stamps the overlay, but a
    /// concurrent rehydrate can put it back while the overlay still hides
    /// reads. Live Delete converges disk through `converge_delete_tips`,
    /// which never calls `purge_tip`. Forgetting the overlay first then
    /// leaves `resolve_tip` serving the deleted row.
    pub(super) fn evict_purged_resident(
        resident: &crate::resident::ResidentGraph,
        slot_revisions: &[crate::resident::PersistSlotRevision],
    ) {
        for ticket in slot_revisions {
            resident.purge_tip_after_persist(ticket);
        }
    }

    pub(super) fn clear_purge_overlays(
        resident: &crate::resident::ResidentGraph,
        job: &PurgeEnvelope,
    ) {
        for (schema, hash, range) in &job.schema_tombstones {
            resident.forget_schema_key_tombstone_if_current(schema, hash, range, job.tombstone_id);
        }
        for slot in &job.slots {
            resident.forget_key_tombstone_if_current(
                &slot.molecule_uuid,
                &slot.hash,
                &slot.range,
                job.tombstone_id,
            );
        }
    }
}
