//! Persist-lane hard-erasure purge job.

use super::*;

impl MutationManager {
    pub(in crate::fold_db_core::mutation_manager) async fn run_purge_job(
        &self,
        job: &mut PurgeEnvelope,
        slot_revisions: &[crate::resident::PersistSlotRevision],
    ) -> Result<(), SchemaError> {
        let has_completion = job.completion.is_some();
        let native = self.db_ops.namespaced_store().disk_pack_runtime();
        let original_flags = native
            .as_ref()
            .map(|_| (job.validated_present, job.retry_requires_finalize));
        let attempt = self.attempt_purge_stages(job);
        let attempt = async {
            // The lane task no longer has the request's logical-commit scope,
            // and LastStore can move its writes onto spawn_blocking. Hold both
            // suppress guards so tip and catalog deletes do not re-enter the
            // mutation log as leftover physical operations. Product requests
            // still stage MutationIntent while router depth is nonzero.
            let _kv_suppress = self
                .capture_router()
                .map(|router| router.enter_kv_suppress());
            crate::sync::capture::with_capture_suppressed(
                crate::storage::disk_pack::transaction::scope(native, attempt),
            )
            .await
        };
        let (result, phases) = if has_completion {
            let (result, phases, _) = crate::request_phases::run_with_phases(attempt).await;
            (result, phases)
        } else {
            (
                attempt.await,
                crate::request_phases::RequestPhaseTotals::default(),
            )
        };
        if result.is_err() {
            if let Some((validated, finalize)) = original_flags {
                job.validated_present = validated;
                job.retry_requires_finalize = finalize;
            }
        }
        match result {
            Ok(result) => {
                self.finish_purge_resident_turn(job, slot_revisions).await?;
                if job.completion.is_some() {
                    job.pending_completion = Some(PurgeCompletion {
                        result: Ok(result),
                        phases,
                    });
                }
                Ok(())
            }
            Err(PurgeAttemptError::Missing(error)) => {
                self.finish_purge_resident_turn(job, slot_revisions).await?;
                if job.completion.is_some() {
                    job.pending_completion = Some(PurgeCompletion {
                        result: Err(error),
                        phases,
                    });
                } else {
                    tracing::warn!(error = %error, "terminal persist-lane purge target absent");
                    self.db_ops
                        .resident()
                        .metrics()
                        .record_persist_lane_failure();
                }
                Ok(())
            }
            Err(PurgeAttemptError::Retry(error)) => Err(error),
        }
    }

    /// Validate targets, run the durable erasure core, then the schema final
    /// stages (finalize replay, retention keys, Search outbox).
    async fn attempt_purge_stages(
        &self,
        job: &mut PurgeEnvelope,
    ) -> Result<Vec<String>, PurgeAttemptError> {
        self.validate_purge_job_targets(job).await?;

        let result = match self
            .process_hard_erasures_durable(
                job.erasures.clone(),
                super::super::super::write::HardErasureOptions {
                    storage_prefix: job.storage_prefix.as_deref(),
                    missing: crate::fold_db_core::purge::PurgeMissingPolicy::Skip,
                    verb: job.verb,
                    // This envelope owns a revision, not every future
                    // value of the key. Complete its turn before the
                    // revision-conditional resident cleanup below.
                    evict_resident: false,
                    precomputed_retained: job.guarded_complement_retained.clone(),
                    emit_derived_events: false,
                },
            )
            .await
        {
            Ok(result) => result,
            Err(error) => {
                // The core can fail after its destructive store step. Keep
                // this obligation until one later attempt completes every
                // schema final stage.
                job.retry_requires_finalize = true;
                return Err(PurgeAttemptError::Retry(error));
            }
        };

        let schema_name = job
            .erasures
            .first()
            .map_or("", |mutation| mutation.schema_name.as_str());
        if job.retry_requires_finalize {
            self.replay_purge_finalize(schema_name)
                .await
                .map_err(PurgeAttemptError::Retry)?;
            job.retry_requires_finalize = false;
        }

        if let Some(keys) = &job.retention_keys {
            // A key that the erase spared by tip write time keeps its age row.
            let spared_from_nanos = job
                .erasures
                .iter()
                .filter_map(|mutation| mutation.erase_tips_before_nanos)
                .min();
            let schemas = self.db_ops.schemas();
            match spared_from_nanos {
                Some(nanos) => {
                    schemas
                        .remove_schema_retention_keys_older_than(
                            schema_name,
                            keys,
                            nanos / 1_000_000_000,
                        )
                        .await
                }
                None => {
                    schemas
                        .remove_schema_retention_keys(schema_name, keys)
                        .await
                }
            }
            .map_err(PurgeAttemptError::Retry)?;
        }

        if let Some(batch) = &job.search_batch {
            self.deliver_index_change_batch(batch)
                .await
                .map_err(PurgeAttemptError::Retry)?;
        }

        Ok(result)
    }

    /// Refuse-policy jobs check once that every target is present.
    async fn validate_purge_job_targets(
        &self,
        job: &mut PurgeEnvelope,
    ) -> Result<(), PurgeAttemptError> {
        if job.missing != crate::fold_db_core::purge::PurgeMissingPolicy::Refuse
            || job.validated_present
        {
            return Ok(());
        }
        let schema_name = job
            .erasures
            .first()
            .map_or("", |mutation| mutation.schema_name.as_str());
        let keys: Vec<_> = job
            .erasures
            .iter()
            .map(|mutation| mutation.key_value.clone())
            .collect();
        match crate::fold_db_core::purge::validate_purge_targets_present(
            &self.db_ops,
            &self.schema_manager,
            schema_name,
            &keys,
            job.verb,
        )
        .await
        .map_err(PurgeAttemptError::Retry)?
        {
            crate::fold_db_core::purge::PurgeTargetPresence::Present => {
                job.validated_present = true;
                Ok(())
            }
            crate::fold_db_core::purge::PurgeTargetPresence::Missing(error) => {
                Err(PurgeAttemptError::Missing(error))
            }
        }
    }
}
