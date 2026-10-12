//! Create / Update batch pipeline, one helper per phase.

use super::*;

mod apply_gate;
mod prepare;

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
        // Replay and an explicit synchronous request wait for the complete
        // schema-lane envelope. Ordinary writes keep the accepted resident-ack
        // contract; LastStore restores a failed multi-op durable attempt.
        let force_durable = require_durable
            || mutations
                .iter()
                .any(|mutation| mutation.synchronous == Some(true));
        let operations = Self::count_requested_operations(&mutations);
        let mut stages = ResidentCommitStages::default();

        self.reject_derived_and_blocked(&mutations)?;

        // Phase 0.5a: Compare-and-set lock acquisition (same-node atomicity).
        //
        // For every mutation carrying a `CasExpectation`, take the per-key CAS
        // lock before either the idempotency filter or the precondition check
        // below runs. The `_cas_guards` bindings hold those locks until this
        // function returns, so no other same-key writer can read-modify-write
        // in between our compare and our commit — that window is exactly
        // where a naive check-then-set silently loses a concurrent writer. A
        // batch with no CAS mutations acquires nothing and is byte-for-byte
        // unchanged.
        let lock_wait_start = std::time::Instant::now();
        let _cas_guards = self.acquire_cas_locks(&mutations).await;
        crate::request_phases::add_phase(
            crate::request_phases::RequestPhase::LockWait,
            lock_wait_start.elapsed(),
        );

        let batch_len = mutations.len();
        tracing::info!(
            "write_mutations_batch_async: Starting batch of {} mutations",
            batch_len
        );
        tracing::debug!(
            "DEBUG: MutationManager::write_mutations_batch_async started for {} mutations",
            batch_len
        );

        let start_time = std::time::Instant::now();
        let mut timing_breakdown = std::collections::HashMap::new();
        // Accumulated across schema groups; reported once as `LockWait` and
        // excluded from the residual `apply` bucket. See the acquisition site.
        let mut molecule_lock_wait = std::time::Duration::ZERO;

        // Phase 1: Filter out already-processed mutations BEFORE the CAS
        // precondition check below. A byte-identical retry of an applied
        // CAS-guarded mutation must be recognized as a safe no-op here — the
        // CAS check compares against the head the first attempt already
        // advanced, so running it before this filter rejects the retry as a
        // conflict for exactly the write type ("same-node atomicity") whose
        // contract depends on safe retries. This filter and the CAS check
        // below both still run while `_cas_guards` is held, so the
        // same-node atomicity invariant is unchanged for every mutation that
        // survives the filter.
        let idem_start = std::time::Instant::now();
        let (already_seen_ids, new_mutations, new_hashes) = self
            .filter_idempotent_mutations(mutations, storage_prefix)
            .await?;
        Self::add_timing(
            &mut timing_breakdown,
            "idempotency_check",
            idem_start.elapsed(),
        );

        if new_mutations.is_empty() {
            return self
                .finish_all_duplicate_batch(
                    already_seen_ids,
                    force_durable,
                    &timing_breakdown,
                    start_time,
                    stages,
                )
                .await;
        }

        self.check_batch_preconditions(&new_mutations, storage_prefix)
            .await?;

        tracing::debug!(
            "Idempotency: {} new, {} duplicates",
            new_mutations.len(),
            already_seen_ids.len()
        );

        let mut prepared_deltas = self
            .prepare_grouped_deltas(
                new_mutations,
                new_hashes,
                storage_prefix,
                force_durable,
                &mut timing_breakdown,
                &mut stages,
            )
            .await?;

        let persist_completions = self
            .apply_prepared_deltas(
                &mut prepared_deltas,
                storage_prefix,
                require_durable,
                author_clock_barrier,
                &mut timing_breakdown,
                &mut molecule_lock_wait,
                &mut stages,
            )
            .await?;

        let mut mutation_ids = Vec::new();
        for delta in &prepared_deltas {
            for mutation in &delta.schema_mutations {
                mutation_ids.push(mutation.uuid.clone());
            }
        }
        Self::await_persist_completions(persist_completions).await?;

        // Phase 8: apply the optional caller durability barrier. A failed
        // barrier cannot produce a Durable receipt.
        self.finalize_batch(&mut timing_breakdown, force_durable)
            .await?;

        Ok(Self::build_commit_receipt(
            already_seen_ids,
            &mutation_ids,
            operations,
            stages,
            force_durable,
            &timing_breakdown,
            start_time,
            molecule_lock_wait,
        ))
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
    #[allow(clippy::too_many_arguments)]
    fn build_commit_receipt(
        already_seen_ids: Vec<String>,
        mutation_ids: &[String],
        mut operations: ResidentCommitOperations,
        mut stages: ResidentCommitStages,
        force_durable: bool,
        timing_breakdown: &std::collections::HashMap<&str, std::time::Duration>,
        start_time: std::time::Instant,
        molecule_lock_wait: std::time::Duration,
    ) -> ResidentCommitReceipt {
        let total_time = start_time.elapsed();
        stages.gate_wait_us = crate::request_phases::duration_us(molecule_lock_wait);
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

        Self::log_timing_breakdown(timing_breakdown, total_time);
        Self::report_phase_totals(timing_breakdown, total_time, molecule_lock_wait);
        Self::report_resident_commit_counts(&operations);

        ResidentCommitReceipt {
            mutation_ids: all_ids,
            // Stamped by the caller batch entry point, which is the only
            // scope that knows this pipeline run is one logical operation —
            // a mixed batch calls this function once and peels erasures
            // around it.
            revision: None,
            operations,
            // The default path returns after the resident commit; the
            // per-schema FIFO lane writes the envelope afterwards. Anything
            // stronger would be a claim about disk this function did not wait
            // for.
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
