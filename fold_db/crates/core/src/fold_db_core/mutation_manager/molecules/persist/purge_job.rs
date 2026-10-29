//! Persist-lane hard-erasure purge job.

use super::*;
use crate::db_operations::search_index::IndexChangeBatch;

impl MutationManager {
    pub(in crate::fold_db_core::mutation_manager) async fn run_purge_job(
        &self,
        job: &mut PurgeEnvelope,
    ) -> Result<(), SchemaError> {
        let has_completion = job.completion.is_some();
        let notification_capture = job.notification_capture.clone();
        let native = self.db_ops.namespaced_store().disk_pack_runtime();
        let original_flags = native
            .as_ref()
            .map(|_| (job.validated_present, job.retry_requires_finalize));
        let attempt = async {
            self.require_replay_cutoff(&job.erasures)
                .await
                .map_err(PurgeAttemptError::Rejected)?;
            // Both erasure verbs accept caller conditions. Compare before any
            // selected tip or body is removed, under the native write gate.
            self.check_cas_preconditions(&job.erasures, job.storage_prefix.as_deref())
                .await
                .map_err(PurgeAttemptError::Rejected)?;
            let result = match native.as_ref() {
                Some(runtime) => self.attempt_current_erasure_stages(job, runtime).await?,
                None => self.attempt_purge_stages(job).await?,
            };
            if let Some(runtime) = &native {
                let events = crate::db_operations::ChangeFeedStore::native_events(&job.erasures);
                self.db_ops
                    .change_feed()
                    .stage_native_events(events)
                    .await
                    .map_err(PurgeAttemptError::Rejected)?;
                self.reserve_native_core_memory(runtime)
                    .map_err(PurgeAttemptError::Retry)?;
            }
            Ok(result)
        };
        let attempt = crate::storage::disk_pack::transaction::scope(native.clone(), attempt);
        let attempt = crate::db_operations::ChangeFeedStore::with_capture_context(
            notification_capture,
            attempt,
        );
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
                if job.completion.is_some() {
                    job.pending_completion = Some(PurgeCompletion {
                        result: Ok(result),
                        phases,
                    });
                }
                Ok(())
            }
            Err(PurgeAttemptError::Rejected(error)) => {
                let uuids: Vec<_> = job
                    .erasures
                    .iter()
                    .map(|mutation| mutation.uuid.as_str())
                    .collect();
                self.db_ops.atoms().clear_pending_delete_barriers_of(&uuids);
                if job.completion.is_some() {
                    job.pending_completion = Some(PurgeCompletion {
                        result: Err(error),
                        phases,
                    });
                } else {
                    tracing::warn!(error = %error, "terminal persist-lane purge target absent");
                    self.db_ops.persist_metrics().record_failure();
                }
                Ok(())
            }
            Err(PurgeAttemptError::Retry(error)) => Err(error),
        }
    }

    /// Validate targets, run the durable erasure core, then the schema final
    /// stages (finalize replay, retention keys, Search outbox).
    pub(super) async fn attempt_purge_stages(
        &self,
        job: &mut PurgeEnvelope,
    ) -> Result<Vec<String>, PurgeAttemptError> {
        self.validate_purge_job_targets(job).await?;

        let outcome = match self
            .process_hard_erasures_durable(
                job.erasures.clone(),
                super::super::super::write::HardErasureOptions {
                    storage_prefix: job.storage_prefix.as_deref(),
                    missing: crate::fold_db_core::purge::PurgeMissingPolicy::Skip,
                    verb: job.verb,
                },
            )
            .await
        {
            Ok(result) => result,
            Err(
                error @ (SchemaError::ReadResourceLimit { .. }
                | SchemaError::WriteResourceLimit { .. }),
            ) if crate::storage::disk_pack::transaction::publication::active(
                self.db_ops.namespaced_store().disk_pack_runtime(),
            ) =>
            {
                // The complete native scope still owns every private erase
                // stage. A deterministic cap refusal drops that scope and
                // completes the caller; retry cannot create more capacity.
                return Err(PurgeAttemptError::Rejected(error));
            }
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
            // The exact tip verdict controls cleanup. An older Delete can
            // spare a current tip even without a TTL cutoff in its envelope.
            let spared: HashSet<_> = outcome.spared.iter().collect();
            let erased: Vec<_> = keys
                .iter()
                .filter(|key| !spared.contains(key))
                .cloned()
                .collect();
            self.db_ops
                .schemas()
                .remove_schema_retention_keys(schema_name, &erased)
                .await
                .map_err(PurgeAttemptError::Retry)?;
        }

        if let Some(batch) = self
            .erased_search_batch(schema_name, job, &outcome.spared)
            .map_err(PurgeAttemptError::Retry)?
        {
            self.publish_purge_search(&batch)
                .await
                .map_err(PurgeAttemptError::Retry)?;
        }

        Ok(outcome.ids)
    }

    /// The Search tombstones of the erasures that the erase did not spare. A
    /// key that the erase spared still has a newer tip, so Search keeps it.
    fn erased_search_batch(
        &self,
        schema_name: &str,
        job: &PurgeEnvelope,
        spared: &[crate::schema::types::KeyValue],
    ) -> Result<Option<IndexChangeBatch>, SchemaError> {
        let Some(schema) = self.schema_manager.get_schema_metadata(schema_name)? else {
            return Ok(None);
        };
        let spared: HashSet<String> = spared
            .iter()
            .map(crate::schema::types::KeyValue::to_storage_key)
            .collect();
        let erasures: Vec<_> = job
            .erasures
            .iter()
            .filter(|mutation| !spared.contains(&mutation.key_value.to_storage_key()))
            .cloned()
            .collect();
        let keys: Vec<_> = erasures
            .iter()
            .map(|mutation| mutation.key_value.clone())
            .collect();
        Ok(Self::build_index_change_batch(
            schema_name,
            &schema,
            &erasures,
            &keys,
        ))
    }

    /// Search sees a tombstone only after the entire native deletion commits.
    async fn publish_purge_search(&self, batch: &IndexChangeBatch) -> Result<(), SchemaError> {
        let Some(runtime) = self.db_ops.namespaced_store().disk_pack_runtime() else {
            return self.deliver_index_change_batch(batch).await;
        };
        let inbox = self.search_outbox_inbox.clone();
        let batch = batch.clone();
        crate::storage::disk_pack::transaction::after_commit(&runtime, async move {
            Self::deliver_search_outbox(inbox, batch).await
        })?;
        Ok(())
    }

    /// Refuse-policy jobs check once that every target is present.
    pub(super) async fn validate_purge_job_targets(
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
            job.storage_prefix.as_deref(),
        )
        .await
        .map_err(PurgeAttemptError::Retry)?
        {
            crate::fold_db_core::purge::PurgeTargetPresence::Present => {
                job.validated_present = true;
                Ok(())
            }
            crate::fold_db_core::purge::PurgeTargetPresence::Missing(error) => {
                Err(PurgeAttemptError::Rejected(error))
            }
        }
    }
}
