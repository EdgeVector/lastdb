//! Post-publish persist enqueue for a prepared schema delta.

use super::*;

impl MutationManager {
    pub(super) fn enqueue_post_publish_persist(
        &self,
        delta: &mut super::super::molecules::PreparedSchemaDelta,
        sibling_updates: Vec<(
            String,
            crate::db_operations::MoleculeData,
            HashSet<crate::db_operations::ChangedKey>,
        )>,
        gate_outcome: &super::super::molecules::ApplyGateOutcome,
        batch_dirty_keys: Vec<crate::resident::DirtyKey>,
        completion: Option<tokio::sync::oneshot::Sender<crate::request_phases::RequestCounts>>,
        author_clock_barrier: Option<super::super::author_clock::AuthorClockPersistBarrier>,
    ) {
        let job = super::super::molecules::DeferredPersistJob {
            author_clock_barrier,
            deferred_atoms: delta.deferred_atoms.take(),
            storage_prefix: delta
                .schema
                .runtime_fields
                .values()
                .find_map(|field| field.common().storage_prefix())
                .map(str::to_string),
            search_batch: delta.search_batch.take(),
            share_prefixes: std::mem::take(&mut delta.share_prefixes),
            schema: delta.schema.clone(),
            modified_fields: gate_outcome.modified_fields.clone(),
            mutation_events: gate_outcome.mutation_events.clone(),
            idempotency_entries: std::mem::take(&mut delta.idempotency_entries),
            retention_write: delta.tracks_retention_age.then(|| {
                (
                    delta.mutation_key_values.clone(),
                    crate::clock::unix_millis() / 1_000,
                )
            }),
            retention_hash_partitions: delta
                .tracks_retention_hash_partitions
                .then(|| delta.mutation_key_values.clone()),
            schema_name: delta.schema_name.clone(),
            sibling_updates,
            molecule_write_guards: Vec::new(),
            batch_dirty_keys,
            defer_reservation: delta.defer_reservation.take(),
            slot_revisions: gate_outcome.slot_revisions.clone(),
            // The lane is another task. The request task-local does not reach it.
            batch_log: crate::durable_flush::current(),
        };
        let reservation = delta
            .lane_reservation
            .take()
            .expect("every prepared schema delta reserves its persist lane before apply");
        let pending_persist_task = delta
            .pending_persist_task
            .take()
            .expect("every prepared schema delta registers before atom creation");
        self.fill_reserved_persist(job, reservation, completion, pending_persist_task);
    }
}
