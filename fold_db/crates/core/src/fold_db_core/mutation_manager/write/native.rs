//! One native request owns every schema's stored bytes before publication.

use super::super::molecules::{DeferredPersistJob, PreparedSchemaDelta};
use super::*;
use crate::storage::disk_pack::{transaction, PackRuntime};
use crate::write_preparation::measure;
use futures::FutureExt;
use std::sync::Arc;

pub(super) struct NativeRequest<'a> {
    pub runtime: Arc<PackRuntime>,
    pub mutations: Vec<Mutation>,
    pub storage_prefix: Option<&'a str>,
    pub author_clock: Option<super::super::author_clock::AuthorClockPersistBarrier>,
}

struct NativeCommit {
    already_seen_ids: Vec<String>,
    mutation_ids: Vec<String>,
}

impl MutationManager {
    pub(super) async fn write_native_batch(
        &self,
        request: NativeRequest<'_>,
        operations: ResidentCommitOperations,
        start: std::time::Instant,
        mut state: super::create_update_batch::BatchState,
    ) -> Result<ResidentCommitReceipt, SchemaError> {
        let committed = self.commit_native_request(request, &mut state).await?;
        if committed.mutation_ids.is_empty() {
            return self
                .finish_all_duplicate_batch(
                    committed.already_seen_ids,
                    state.force_durable,
                    &state.timing_breakdown,
                    start,
                    state.stages,
                )
                .await;
        }
        Ok(Self::build_commit_receipt(
            committed.already_seen_ids,
            &committed.mutation_ids,
            operations,
            start,
            state,
        ))
    }

    async fn commit_native_request(
        &self,
        request: NativeRequest<'_>,
        state: &mut super::create_update_batch::BatchState,
    ) -> Result<NativeCommit, SchemaError> {
        self.require_native_logical_admission(&request.mutations)?;
        // The clock queue uses native storage too. Complete its barrier before
        // taking the request's exclusive storage gate, never while owning it.
        if let Some(clock) = request.author_clock {
            clock.wait().await?;
        }
        // Owned inputs and the startup-frozen floor need no write gate.
        // Offline maintenance retains its exclusive no-writer contract.
        self.require_replay_cutoff(&request.mutations).await?;
        state.force_durable = true;
        let committed = transaction::request_scope(Some(Arc::clone(&request.runtime)), async {
            let notifications =
                crate::db_operations::ChangeFeedStore::native_events(&request.mutations);
            request
                .runtime
                .record_read_stage("request-start", request.mutations.len());
            // Compare once inside the exclusive native gate. A prior
            // request can commit while this request waits to enter it.
            let started = std::time::Instant::now();
            let super::super::cas_idempotency::FilteredMutations {
                already_seen_ids,
                mutations,
                hashes,
            } = self
                .filter_idempotent_mutations(request.mutations, request.storage_prefix)
                .await?;
            Self::add_timing(
                &mut state.timing_breakdown,
                "idempotency_check",
                started.elapsed(),
            );
            let mutation_ids = mutations
                .iter()
                .map(|mutation| mutation.uuid.clone())
                .collect();
            request
                .runtime
                .record_read_stage("idempotency-done", mutations.len());
            if mutations.is_empty() {
                self.db_ops
                    .change_feed()
                    .stage_native_events(notifications)
                    .await?;
                return Ok::<_, SchemaError>(NativeCommit {
                    already_seen_ids,
                    mutation_ids,
                });
            }
            self.check_batch_preconditions(&mutations, request.storage_prefix)
                .await?;
            request
                .runtime
                .record_read_stage("conditions-done", mutations.len());
            // Keep the private multi-schema stage behind one future
            // allocation instead of expanding it in every host caller.
            let (delta_count, logical_candidates) = super::super::logical_capture::collect(
                &request.runtime,
                self.stage_grouped_deltas(
                    mutations,
                    hashes,
                    request.storage_prefix,
                    &mut state.timing_breakdown,
                    &mut state.stages,
                    &request.runtime,
                )
                .boxed(),
            )
            .await?;
            request
                .runtime
                .record_read_stage("stored-stages-done", delta_count);
            self.finish_logical_capture(&request.runtime, &mutation_ids, logical_candidates)
                .await?;
            measure("author_clock_us", self.stage_native_author_clock()).await?;
            measure(
                "change_feed_us",
                self.db_ops.change_feed().stage_native_events(notifications),
            )
            .await?;
            measure("memory_reserve_us", async {
                self.reserve_native_core_memory(&request.runtime)
            })
            .await?;
            Ok::<_, SchemaError>(NativeCommit {
                already_seen_ids,
                mutation_ids,
            })
        })
        .await?;
        Ok(committed)
    }

    pub(in crate::fold_db_core::mutation_manager) async fn stage_native_author_clock(
        &self,
    ) -> Result<(), SchemaError> {
        let key = crate::schema::types::author_clock::mutation_author_clock_key(
            &self.signer.public_key_base64(),
        );
        let state = *self
            .author_clock_state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut durable = self
            .db_ops
            .metadata()
            .get_typed::<crate::schema::types::MutationAuthorClockState>(&key)
            .await?
            .unwrap_or_default();
        durable.observe_remote(state.physical_nanos, state.logical_counter);
        self.db_ops.metadata().put_typed(&key, &durable).await?;
        Ok(())
    }

    pub(super) async fn stage_native_delta(
        &self,
        mut delta: PreparedSchemaDelta,
        runtime: &Arc<PackRuntime>,
        heads: &mut super::super::molecules::batch_rebase::BatchSlotHeads,
    ) -> Result<std::time::Duration, SchemaError> {
        // Membership must precede the protein reads that select sibling tips.
        // This also upgrades a reopened home on its first ordinary mutation.
        measure(
            "schema_coherence_us",
            self.schema_manager
                .apply_field_hash_coherence_on_load(&delta.schema_name),
        )
        .await?;
        let (modified_fields, fold_elapsed) = measure(
            "rebase_us",
            self.rebase_delta_for_publish(&mut delta, heads),
        )
        .await?;
        heads.record(&delta);
        self.collect_logical_delta(&delta, runtime)?;
        let pending_persist_task = delta.pending_persist_task.take();
        let mut job = into_native_job(delta, modified_fields);
        let alternate_keys: Vec<_> = if job.schema.alternate_keys.is_empty() {
            Vec::new()
        } else {
            job.modified_fields
                .values()
                .flatten()
                .map(|key| crate::schema::types::KeyValue::new(key.hash.clone(), key.range.clone()))
                .collect::<std::collections::HashSet<_>>()
                .into_iter()
                .collect()
        };
        let alternate_before = if alternate_keys.is_empty() {
            Vec::new()
        } else {
            self.db_ops
                .atoms()
                .alternate_fields(&job.schema, &alternate_keys, job.storage_prefix.as_deref())
                .await?
        };
        measure("store_job_us", self.stage_native_job(&mut job)).await?;
        self.db_ops
            .atoms()
            .stage_alternate_changes(
                &job.schema,
                &alternate_keys,
                &alternate_before,
                job.storage_prefix.as_deref(),
            )
            .await?;
        measure("logical_paths_us", self.stage_native_paths(&job, runtime)).await?;
        runtime.record_read_stage("logical-paths-done", job.modified_fields.len());
        if let Some(batch) = job.search_batch.take() {
            let inbox = self.search_outbox_inbox.clone();
            transaction::after_commit(runtime, async move {
                Self::deliver_search_outbox(inbox, batch).await
            })?;
        }
        transaction::retain(runtime, pending_persist_task)?;
        Ok(fold_elapsed)
    }
}

fn into_native_job(
    delta: PreparedSchemaDelta,
    modified_fields: super::super::helpers::ModifiedFieldKeys,
) -> DeferredPersistJob {
    let storage_prefix = delta
        .schema
        .runtime_fields
        .values()
        .find_map(|field| field.common().storage_prefix())
        .map(str::to_string);
    let retention_hash_partitions = delta
        .tracks_retention_hash_partitions
        .then(|| delta.mutation_key_values.clone());
    let retention_write = delta
        .tracks_retention_age
        .then_some(delta.mutation_key_values);
    DeferredPersistJob {
        deferred_atoms: delta.deferred_atoms,
        storage_prefix,
        search_batch: delta.search_batch,
        share_prefixes: delta.share_prefixes,
        schema: delta.schema,
        modified_fields,
        idempotency_entries: delta.idempotency_entries,
        retention_write,
        retention_hash_partitions,
        schema_name: delta.schema_name,
        sibling_updates: delta.sibling_updates,
        sibling_moves: delta.sibling_moves,
    }
}
