//! Persist-lane storage-slot purge job.

use super::*;
use futures::FutureExt;

impl MutationManager {
    /// Run one legacy storage-slot drain page through its schema lane.
    ///
    /// A retry rebuilds the schema snapshot because a failed core attempt can
    /// reload the already-purged schema before its flush fails. The evidence
    /// checkpoint stays in the envelope and therefore survives that rebuild.
    pub(in crate::fold_db_core::mutation_manager) async fn run_storage_slot_purge_job(
        &self,
        job: &mut StorageSlotPurgeEnvelope,
    ) -> Result<(), SchemaError> {
        let native = self.db_ops.namespaced_store().disk_pack_runtime();
        let original = native.as_ref().map(|_| {
            (
                job.evidence_checkpoint.clone(),
                job.search_batch.clone(),
                job.durable_complete,
                job.retry_requires_finalize,
            )
        });
        let attempt = async {
            self.attempt_storage_slot_purge_stages(job).await?;
            if let Some(runtime) = &native {
                self.stage_storage_slot_core_paths(job, runtime)
                    .boxed()
                    .await?;
                self.reserve_native_core_memory(runtime)?;
            }
            Ok::<_, SchemaError>(())
        };

        let attempt = async {
            let _kv_suppress = self
                .capture_router()
                .map(|router| router.enter_kv_suppress());
            crate::sync::capture::with_capture_suppressed(
                crate::storage::disk_pack::transaction::scope(native.clone(), attempt),
            )
            .await
        };
        let (result, phases, _) = crate::request_phases::run_with_phases(attempt).await;
        if result.is_err() {
            if let Some((evidence, search, complete, finalize)) = original {
                job.evidence_checkpoint = evidence;
                job.search_batch = search;
                job.durable_complete = complete;
                job.retry_requires_finalize = finalize;
            }
        }
        result?;
        if job.completion.is_some() {
            job.pending_completion = Some(StorageSlotPurgeCompletion {
                evidence: std::mem::take(&mut job.evidence_checkpoint),
                phases,
            });
        }
        Ok(())
    }

    /// Drain every target chunk under the schema purge barrier, then deliver
    /// the Search tombstones that the drain produced.
    async fn attempt_storage_slot_purge_stages(
        &self,
        job: &mut StorageSlotPurgeEnvelope,
    ) -> Result<(), SchemaError> {
        if !job.durable_complete {
            self.drain_storage_slot_targets(job).await?;
        }

        if job.search_batch.is_none() && !job.evidence_checkpoint.is_empty() {
            let schema = self
                .schema_manager
                .get_schema_metadata(&job.schema_name)?
                .ok_or_else(|| {
                    SchemaError::InvalidData(format!(
                        "Schema '{}' disappeared after storage-slot purge",
                        job.schema_name
                    ))
                })?;
            job.search_batch = Self::build_storage_slot_tombstones(
                &job.schema_name,
                &schema,
                &job.evidence_checkpoint,
            );
        }
        if let Some(batch) = &job.search_batch {
            self.deliver_index_change_batch(batch).await?;
        }
        Ok(())
    }

    async fn drain_storage_slot_targets(
        &self,
        job: &mut StorageSlotPurgeEnvelope,
    ) -> Result<(), SchemaError> {
        const CHUNK: usize = 64;

        let prep = crate::fold_db_core::purge::prepare_storage_slot_purge(
            &self.db_ops,
            &self.schema_manager,
            &job.schema_name,
        )
        .await?;

        for chunk in job.targets.chunks(CHUNK) {
            let barrier = self.schema_purge_barrier(&job.schema_name);
            let acquire_start = std::time::Instant::now();
            let (result, exclusive_hold) = {
                let _purge_guard = barrier.write_owned().await;
                crate::request_phases::add_phase(
                    crate::request_phases::RequestPhase::PurgeBarrier,
                    acquire_start.elapsed(),
                );
                self.record_schema_barrier_acquisition(&job.schema_name);
                let storage_keys: Vec<String> = chunk
                    .iter()
                    .map(crate::fold_db_core::purge::StorageSlotPurgeTarget::storage_record_key)
                    .collect();
                // Match ordinary Delete lock order. These exact guards cover
                // fresh tip/atom validation through the destructive commit.
                let _tip_guards = self.db_ops.atoms().lock_tip_commits(&storage_keys).await;
                let _publication_guards = self
                    .db_ops
                    .atoms()
                    .lock_tip_publications(&storage_keys)
                    .await;
                let held_from = std::time::Instant::now();
                let mut acct = crate::fold_db_core::purge::PurgeCommitAccounting::default();
                let result = match crate::fold_db_core::purge::purge_storage_slots_bulk(
                    crate::fold_db_core::purge::PurgeScope {
                        db_ops: &self.db_ops,
                        schema_manager: &self.schema_manager,
                        schema_name: &job.schema_name,
                    },
                    chunk,
                    crate::fold_db_core::purge::PurgeMissingPolicy::Skip,
                    &prep,
                    &mut acct,
                    &mut job.evidence_checkpoint,
                )
                .await
                {
                    Ok(result) => result,
                    Err(error) => {
                        // The core can remove rows and reload the
                        // shrunken schema before its flush fails.
                        job.retry_requires_finalize = true;
                        return Err(error);
                    }
                };
                let held = held_from.elapsed();
                crate::request_phases::add_phase(
                    crate::request_phases::RequestPhase::PurgeCommit,
                    held.saturating_sub(acct.named()),
                );
                (result, held)
            };
            self.record_purge(
                &job.schema_name,
                u64::try_from(result.report.records_purged).unwrap_or(u64::MAX),
                exclusive_hold,
            );
            self.record_purge_path(
                &job.schema_name,
                result.report.target_slots,
                result.report.candidate_atoms,
                result.report.reverse_edge_reads,
            );
        }

        if job.retry_requires_finalize {
            self.replay_purge_finalize(&job.schema_name).await?;
            job.retry_requires_finalize = false;
        }
        job.durable_complete = true;
        Ok(())
    }
}
