//! One native request owns every schema's stored bytes before publication.

use super::super::molecules::{DeferredPersistJob, PreparedSchemaDelta};
use super::*;
use crate::db_operations::search_index::{IndexSink, SearchOutboxSink};
use crate::storage::disk_pack::{transaction, PackRuntime};
use std::sync::Arc;

pub(super) struct NativeRequest<'a> {
    pub runtime: Arc<PackRuntime>,
    pub mutations: Vec<Mutation>,
    pub storage_prefix: Option<&'a str>,
    pub author_clock: Option<super::super::author_clock::AuthorClockPersistBarrier>,
}

impl MutationManager {
    pub(super) async fn commit_native_request(
        &self,
        request: NativeRequest<'_>,
        state: &mut super::create_update_batch::BatchState,
    ) -> Result<Vec<String>, SchemaError> {
        // The clock queue uses native storage too. Complete its barrier before
        // taking the request's exclusive storage gate, never while owning it.
        if let Some(clock) = request.author_clock {
            clock.wait().await?;
        }
        let memory_ack = self.resident_mode.acks_on_resident()
            && !state.force_durable
            && !crate::fold_db_core::mutation_sync_flush_enabled();
        let ids =
            transaction::request_scope(Some(Arc::clone(&request.runtime)), memory_ack, async {
                request
                    .runtime
                    .record_read_stage("request-start", request.mutations.len());
                // Recheck inside the exclusive native gate. A prior native request
                // can commit while this request waits to enter preparation.
                let ids: Vec<_> = request
                    .mutations
                    .iter()
                    .map(|mutation| mutation.uuid.clone())
                    .collect();
                let (_, mutations, hashes) = self
                    .filter_idempotent_mutations(request.mutations, request.storage_prefix)
                    .await?;
                request
                    .runtime
                    .record_read_stage("idempotency-done", mutations.len());
                self.check_batch_preconditions(&mutations, request.storage_prefix)
                    .await?;
                request
                    .runtime
                    .record_read_stage("conditions-done", mutations.len());
                let mut deltas = self
                    .prepare_grouped_deltas(
                        mutations,
                        hashes,
                        request.storage_prefix,
                        state.force_durable,
                        &mut state.timing_breakdown,
                        &mut state.stages,
                    )
                    .await?;
                request
                    .runtime
                    .record_read_stage("private-prepare-done", deltas.len());
                self.commit_native_deltas(&mut deltas, &request.runtime)
                    .await?;
                request
                    .runtime
                    .record_read_stage("stored-stages-done", deltas.len());
                if memory_ack {
                    let bytes = transaction::memory_quote(&request.runtime)?;
                    if let Some(reservation) = self.try_reserve_defer(bytes) {
                        transaction::retain_raw_memory_reservation(&request.runtime, reservation)?;
                    } else {
                        transaction::require_durable(&request.runtime)?;
                    }
                }
                Ok::<_, SchemaError>(ids)
            })
            .await?;
        crate::sync::capture::mark_logical_commit_published();
        self.finalize_batch(&mut state.timing_breakdown, state.force_durable)
            .await?;
        Ok(ids)
    }

    pub(super) async fn commit_native_deltas(
        &self,
        deltas: &mut [PreparedSchemaDelta],
        runtime: &Arc<PackRuntime>,
    ) -> Result<Vec<String>, SchemaError> {
        let mut ids = Vec::new();
        let mut heads = super::super::molecules::batch_rebase::BatchSlotHeads::default();
        for delta in deltas {
            let (mutation_events, modified_fields) =
                self.rebase_delta_for_publish(delta, &mut heads);
            let mut job = private_job(
                delta,
                delta.schema.clone(),
                mutation_events,
                modified_fields,
            );
            self.stage_native_job(&mut job).await?;
            self.stage_native_paths(&job, runtime).await?;
            heads.record(delta);
            runtime.record_read_stage("logical-paths-done", job.modified_fields.len());
            if let Some(batch) = job.search_batch.take() {
                let inbox = self.search_outbox_inbox.clone();
                transaction::after_commit(runtime, async move {
                    let sink = inbox
                        .map(SearchOutboxSink::new)
                        .or_else(SearchOutboxSink::from_env);
                    if let Some(sink) = sink {
                        sink.apply_change_batch(batch).await.map_err(|error| {
                            crate::storage::StorageError::BackendError(error.to_string())
                        })?;
                    }
                    Ok(())
                })?;
            }
            transaction::retain(runtime, delta.pending_persist_task.take())?;
            transaction::retain(runtime, delta.defer_reservation.take())?;
            // Native requests use the one complete storage owner, not separate
            // schema lanes. An unused lane ticket returns its capacity here.
            drop(delta.lane_reservation.take());
            ids.extend(
                delta
                    .schema_mutations
                    .iter()
                    .map(|mutation| mutation.uuid.clone()),
            );
        }
        Ok(ids)
    }
}

fn private_job(
    delta: &PreparedSchemaDelta,
    schema: crate::schema::types::Schema,
    mutation_events: Vec<crate::atom::MutationEvent>,
    modified_fields: super::super::helpers::ModifiedFieldKeys,
) -> DeferredPersistJob {
    DeferredPersistJob {
        author_clock_barrier: None,
        deferred_atoms: delta.deferred_atoms.clone(),
        storage_prefix: schema
            .runtime_fields
            .values()
            .find_map(|field| field.common().storage_prefix())
            .map(str::to_string),
        search_batch: delta.search_batch.clone(),
        share_prefixes: delta.share_prefixes.clone(),
        schema,
        modified_fields,
        mutation_events,
        idempotency_entries: delta.idempotency_entries.clone(),
        retention_write: delta.tracks_retention_age.then(|| {
            (
                delta.mutation_key_values.clone(),
                crate::clock::unix_millis() / 1000,
            )
        }),
        retention_hash_partitions: delta
            .tracks_retention_hash_partitions
            .then(|| delta.mutation_key_values.clone()),
        schema_name: delta.schema_name.clone(),
        sibling_updates: delta.sibling_updates.clone(),
        molecule_write_guards: Vec::new(),
        batch_dirty_keys: Vec::new(),
        defer_reservation: None,
        slot_revisions: Vec::new(),
        batch_log: crate::durable_flush::current(),
    }
}
