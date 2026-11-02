//! One native request owns every schema's stored bytes before publication.

use super::*;
use crate::storage::disk_pack::{transaction, PackRuntime};
use crate::write_preparation::measure;
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
        // The clock queue uses native storage too. Complete its barrier before
        // taking the request's exclusive storage gate, never while owning it.
        if let Some(clock) = request.author_clock {
            clock.wait().await?;
        }
        // Owned inputs and the startup-frozen floor need no write gate.
        // Offline maintenance retains its exclusive no-writer contract.
        self.require_replay_cutoff(&request.mutations).await?;
        state.force_durable = true;
        let committed = transaction::request_scope(Arc::clone(&request.runtime), async {
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
            let group_count = self
                .stage_record_groups(
                    mutations,
                    hashes,
                    request.storage_prefix,
                    &mut state.timing_breakdown,
                    &mut state.stages,
                    &request.runtime,
                )
                .await?;
            request
                .runtime
                .record_read_stage("stored-stages-done", group_count);
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

    /// Charge the request's staged core memory to the defer window, or make
    /// the request wait for disk when the window is full.
    pub(in crate::fold_db_core::mutation_manager) fn reserve_native_core_memory(
        &self,
        runtime: &PackRuntime,
    ) -> Result<(), SchemaError> {
        let bytes = transaction::memory_quote(runtime)?;
        if let Some(reservation) = self.try_reserve_defer(bytes) {
            transaction::retain_raw_memory_reservation(runtime, reservation)?;
        } else {
            transaction::require_durable(runtime)?;
        }
        Ok(())
    }
}
