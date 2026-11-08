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
    /// Prepare one native Create/Update batch and await its durable commit.
    pub(in crate::fold_db_core::mutation_manager) async fn write_create_update_batch_async(
        &self,
        mutations: Vec<Mutation>,
        author_clock_barrier: Option<super::super::author_clock::AuthorClockPersistBarrier>,
    ) -> Result<ResidentCommitReceipt, SchemaError> {
        if mutations.is_empty() {
            return Ok(ResidentCommitReceipt::empty());
        }
        // Synchronous requests wait for complete durability.
        let force_durable = mutations
            .iter()
            .any(|mutation| mutation.synchronous == Some(true));

        self.reject_blocked_mutation_targets(&mutations)?;

        // The native transaction gate covers every condition and its complete
        // publication.
        Self::log_batch_start(mutations.len());

        let start_time = std::time::Instant::now();
        let state = BatchState::new(force_durable);
        self.write_native_batch(
            super::native::NativeRequest {
                runtime: self.db_ops.native_runtime(),
                mutations,
                author_clock: author_clock_barrier,
            },
            start_time,
            state,
        )
        .await
    }

    fn log_batch_start(batch_len: usize) {
        tracing::info!(
            "write_create_update_batch_async: Starting batch of {} mutations",
            batch_len
        );
        tracing::debug!(
            "DEBUG: MutationManager::write_create_update_batch_async started for {} mutations",
            batch_len
        );
    }

    /// Report one accepted batch and its server-generated record identities.
    pub(super) fn build_commit_receipt(
        mutation_ids: Vec<String>,
        record_revisions: std::collections::BTreeMap<String, String>,
        operations: ResidentCommitOperations,
        start_time: std::time::Instant,
        state: BatchState,
    ) -> ResidentCommitReceipt {
        let BatchState {
            force_durable,
            timing_breakdown,
            mut stages,
        } = state;
        let total_time = start_time.elapsed();
        stages.ack_us = crate::request_phases::duration_us(total_time);
        Self::log_timing_breakdown(&timing_breakdown, total_time);
        Self::report_phase_totals(&timing_breakdown, total_time);
        Self::report_resident_commit_counts(&operations);
        ResidentCommitReceipt {
            mutation_ids,
            record_revisions,
            revision: None,
            operations,
            durability: Self::commit_durability(force_durable),
            stages,
        }
    }

    fn commit_durability(force_durable: bool) -> ResidentDurability {
        if force_durable {
            ResidentDurability::Durable
        } else {
            ResidentDurability::Queued
        }
    }

    /// Count the complete writes accepted by the native transaction.
    pub(super) fn count_applied_operations(mutations: &[Mutation]) -> ResidentCommitOperations {
        let mut operations = ResidentCommitOperations::default();
        for mutation in mutations {
            match mutation.mutation_type {
                crate::schema::types::MutationType::Create => operations.created += 1,
                crate::schema::types::MutationType::Replace => operations.updated += 1,
                crate::schema::types::MutationType::Delete
                | crate::schema::types::MutationType::Purge => operations.deleted += 1,
            }
        }
        operations
    }
}
