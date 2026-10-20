//! Persist-lane hard-erasure purge job.

use super::*;
use crate::db_operations::search_index::{IndexChangeBatch, IndexSink, SearchOutboxSink};

impl MutationManager {
    pub(in crate::fold_db_core::mutation_manager) async fn run_purge_job(
        &self,
        job: &mut PurgeEnvelope,
    ) -> Result<(), SchemaError> {
        let has_completion = job.completion.is_some();
        let native = self.db_ops.namespaced_store().disk_pack_runtime();
        let original_flags = native
            .as_ref()
            .map(|_| (job.validated_present, job.retry_requires_finalize));
        let attempt = async {
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
                self.reserve_native_core_memory(runtime)
                    .map_err(PurgeAttemptError::Retry)?;
            }
            Ok(result)
        };
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
                crate::storage::disk_pack::transaction::scope(native.clone(), attempt),
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
