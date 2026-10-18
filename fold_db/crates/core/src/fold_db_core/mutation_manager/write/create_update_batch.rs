//! Create / Update batch pipeline, one helper per phase.

use super::*;

mod prepare;

/// Mutable state threaded through the phases of one batch write.
pub(super) struct BatchState {
    pub(super) force_durable: bool,
    pub(super) timing_breakdown: std::collections::HashMap<&'static str, std::time::Duration>,
    pub(super) stages: ResidentCommitStages,
}

impl BatchState {
    fn new(force_durable: bool) -> Self {
        Self {
            force_durable,
            timing_breakdown: std::collections::HashMap::new(),
            stages: ResidentCommitStages::default(),
        }
    }
}

impl MutationManager {
    pub(super) async fn write_create_update_batch_in_log(
        &self,
        mutations: Vec<Mutation>,
        storage_prefix: Option<&str>,
        require_durable: bool,
        author_clock_barrier: Option<super::super::author_clock::AuthorClockPersistBarrier>,
    ) -> Result<ResidentCommitReceipt, SchemaError> {
        if mutations.is_empty() {
            return Ok(ResidentCommitReceipt::empty());
        }
        // Replay and synchronous requests wait for complete durability.
        // Each backend owns its publication and failure boundary.
        let force_durable = require_durable
            || mutations
                .iter()
                .any(|mutation| mutation.synchronous == Some(true));
        let operations = Self::count_requested_operations(&mutations);

        self.reject_derived_and_blocked(&mutations)?;

        // Keep each compare-and-set lock through idempotency, validation,
        // and commit. A same-key writer cannot change the compared state.
        let lock_wait_start = std::time::Instant::now();
        let _cas_guards = self.acquire_cas_locks(&mutations).await;
        crate::request_phases::add_phase(
            crate::request_phases::RequestPhase::LockWait,
            lock_wait_start.elapsed(),
        );

        Self::log_batch_start(mutations.len());

        let start_time = std::time::Instant::now();
        let state = BatchState::new(force_durable);
        let runtime = self
            .db_ops
            .namespaced_store()
            .disk_pack_runtime()
            .ok_or_else(|| {
                SchemaError::InvalidData("logical writes require native disk storage".into())
            })?;
        self.write_native_batch(
            super::native::NativeRequest {
                runtime,
                mutations,
                storage_prefix,
                author_clock: author_clock_barrier,
            },
            operations,
            start_time,
            state,
        )
        .await
    }

    fn log_batch_start(batch_len: usize) {
        tracing::info!(
            "write_mutations_batch_async: Starting batch of {} mutations",
            batch_len
        );
        tracing::debug!(
            "DEBUG: MutationManager::write_mutations_batch_async started for {} mutations",
            batch_len
        );
    }

    /// Phase -1: reject historical derived writes and blocked targets, then
    /// record the purge-barrier phase as zero.
    fn reject_derived_and_blocked(&self, mutations: &[Mutation]) -> Result<(), SchemaError> {
        // Phase -1: Reject historical derived writes before anything else
        // touches them. No live writer is allowed to mint
        // `Provenance::Derived`; keeping the check here prevents forged or
        // stale derived writes from polluting atoms. User mutations
        // (`Provenance::User` or `None`) are untouched.
        for mutation in mutations {
            validate_derived_provenance(mutation, &self.schema_manager)?;
        }

        self.reject_blocked_mutation_targets(mutations)?;

        // Create/update do not wait on the schema-wide purge barrier. Janitor
        // purge is later (mixed batches enqueue; this path never runs
        // process_hard_erasures). Last writer wins. Compliance Purge keeps
        // exclusive on its own request. Recording the phase as ~0 keeps
        // request-ops comparable.
        crate::request_phases::add_phase(
            crate::request_phases::RequestPhase::PurgeBarrier,
            std::time::Duration::ZERO,
        );
        Ok(())
    }

    /// Stamp the final stage timings, log the phase totals, and build the
    /// receipt that merges already-seen ids with the newly written ones.
    pub(super) fn build_commit_receipt(
        already_seen_ids: Vec<String>,
        mutation_ids: &[String],
        mut operations: ResidentCommitOperations,
        start_time: std::time::Instant,
        state: BatchState,
    ) -> ResidentCommitReceipt {
        let BatchState {
            force_durable,
            timing_breakdown,
            mut stages,
            ..
        } = state;
        let total_time = start_time.elapsed();
        stages.gate_wait_us = 0;
        stages.ack_us = crate::request_phases::duration_us(total_time);

        // Combine already-seen ids with newly processed mutation ids
        let mut all_ids = already_seen_ids;
        all_ids.extend(mutation_ids.iter().cloned());
        // Duplicates the filter removed. The per-verb counts above describe
        // the requested batch, so this is additive, not a correction.
        let duplicates = (all_ids.len() - mutation_ids.len()) as u64;
        operations.no_op = operations.no_op.saturating_add(duplicates);

        tracing::info!(
            "write_mutations_batch_async: Completed {} mutations ({} new, {} cached) in {:.2}ms",
            all_ids.len(),
            mutation_ids.len(),
            all_ids.len() - mutation_ids.len(),
            total_time.as_millis()
        );

        Self::log_timing_breakdown(&timing_breakdown, total_time);
        Self::report_phase_totals(&timing_breakdown, total_time, std::time::Duration::ZERO);
        Self::report_resident_commit_counts(&operations);

        ResidentCommitReceipt {
            mutation_ids: all_ids,
            // Stamped by the caller batch entry point, which is the only
            // scope that knows this pipeline run is one logical operation —
            // a mixed batch calls this function once and peels erasures
            // around it.
            revision: None,
            operations,
            // Queued permits a later flush. Explicit durability waits for disk;
            // admission can also choose disk-first when it cannot retain bytes.
            durability: Self::commit_durability(force_durable),
            cloud: None,
            stages,
            touched_group_ids: crate::durable_flush::current_touched_groups(),
        }
    }

    fn commit_durability(force_durable: bool) -> ResidentDurability {
        if force_durable {
            ResidentDurability::Durable
        } else {
            ResidentDurability::Queued
        }
    }

    /// Per-class counts are taken from the verbs the CALLER sent, before the
    /// idempotency filter runs, so `created + updated` describes the batch
    /// that was asked for. The duplicates it removes are reported separately
    /// as `no_op` rather than silently shrinking these.
    fn count_requested_operations(mutations: &[Mutation]) -> ResidentCommitOperations {
        let mut operations = ResidentCommitOperations::default();
        for mutation in mutations {
            match mutation.mutation_type {
                crate::schema::types::MutationType::Create => operations.created += 1,
                crate::schema::types::MutationType::Update => operations.updated += 1,
                crate::schema::types::MutationType::Delete
                | crate::schema::types::MutationType::Purge => operations.deleted += 1,
            }
        }
        operations
    }

    /// Pipeline for Create / Update mutations. Hard-erasure verbs
    /// (`Purge` / `Delete`) are split off in the public entrypoint above
    /// before reaching this method so the existing flow (atom creation,
    /// molecule advancement, inline indexing, trigger dispatch) never
    /// sees them.
    pub(in crate::fold_db_core::mutation_manager) async fn write_create_update_batch_async(
        &self,
        mutations: Vec<Mutation>,
        storage_prefix: Option<&str>,
        require_durable: bool,
        author_clock_barrier: Option<super::super::author_clock::AuthorClockPersistBarrier>,
    ) -> Result<ResidentCommitReceipt, SchemaError> {
        // One log for the request and the persist lane. A nested call keeps it.
        if crate::durable_flush::current().is_some() {
            return self
                .write_create_update_batch_in_log(
                    mutations,
                    storage_prefix,
                    require_durable,
                    author_clock_barrier,
                )
                .await;
        }
        let log = crate::durable_flush::BatchPlacementLog::shared();
        crate::durable_flush::scope(
            log,
            self.write_create_update_batch_in_log(
                mutations,
                storage_prefix,
                require_durable,
                author_clock_barrier,
            ),
        )
        .await
    }
}
