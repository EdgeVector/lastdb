//! Moved verbatim out of the parent module; see the parent for context.
// lint:file-size-ok verbatim move; the one oversized function is separate split work

use super::*;

impl MutationManager {
    // lint:fn-size-ok verbatim move from persist.rs; splitting this function is separate work
    pub(in crate::fold_db_core::mutation_manager) async fn run_deferred_job(
        &self,
        job: &mut DeferredPersistJob,
        commit_id: u64,
        slot_revisions: &[crate::resident::PersistSlotRevision],
    ) -> Result<(), SchemaError> {
        // Persist jobs created after the apply-gate cut carry no guards. Drop
        // any rolling-upgrade leftovers before the first storage await.
        job.molecule_write_guards.clear();
        let deferred_start = std::time::Instant::now();
        let persist_body = async {
            // Keep the owned atom batch in the envelope until every durable
            // stage succeeds. A failed stage returns the same batch to the lane.
            if let Some(located) = job.deferred_atoms.as_deref() {
                if !located.is_empty() {
                    self.db_ops
                        .atoms()
                        .batch_store_atoms_located_borrowed(located, job.storage_prefix.as_deref())
                        .await
                        .map_err(|error| {
                            tracing::error!(
                                error = %error,
                                schema = %job.schema_name,
                                commit_id,
                                "deferred resident-write atom store failed"
                            );
                            error
                        })?;

                    #[cfg(feature = "sharing")]
                    if job.storage_prefix.is_none() {
                        for share_prefix in &job.share_prefixes {
                            self.db_ops
                                .atoms()
                                .batch_store_atoms_located_borrowed(located, Some(share_prefix))
                                .await?;
                        }
                    }
                }
            }

            let mut timing = std::collections::HashMap::new();
            self.persist_modified_molecules(
                &mut job.schema,
                &job.modified_fields,
                &job.mutation_events,
                &job.share_prefixes,
                &mut timing,
            )
            .await
            .map_err(|error| {
                tracing::error!(
                    error = %error,
                    schema = %job.schema_name,
                    commit_id,
                    "deferred resident-write molecule persist failed"
                );
                error
            })?;

            let prefix = job
                .schema
                .runtime_fields
                .values()
                .find_map(|field| field.common().storage_prefix())
                .map(str::to_string);
            self.persist_sibling_tip_updates(&job.sibling_updates, prefix.as_deref())
                .await
                .map_err(|error| {
                    tracing::error!(
                        error = %error,
                        schema = %job.schema_name,
                        siblings = job.sibling_updates.len(),
                        commit_id,
                        "deferred resident-write protein sibling tip persist failed"
                    );
                    error
                })?;

            // A request scope must not enter durable schema metadata. Keep the
            // envelope schema unchanged because a later stage can force retry.
            let mut durable_schema = job.schema.clone();
            super::super::super::helpers::clear_storage_prefix_on_schema(&mut durable_schema);
            durable_schema.sync_molecule_uuids();
            self.db_ops
                .store_schema(&job.schema_name, &durable_schema)
                .await
                .map_err(|error| {
                    tracing::error!(
                        error = %error,
                        schema = %job.schema_name,
                        commit_id,
                        "deferred resident-write schema store failed"
                    );
                    error
                })?;
            self.schema_manager
                .load_schema_internal(durable_schema)
                .await
                .map_err(|error| {
                    tracing::error!(
                        error = %error,
                        schema = %job.schema_name,
                        commit_id,
                        "deferred resident-write schema reload failed"
                    );
                    // The catalog row is already durable (store_schema above
                    // succeeded); this reload only refreshes the in-process
                    // cache. A partial failure here must not leave a stale
                    // copy silently cached — evict so the next lookup
                    // self-heals from the just-persisted row instead of
                    // drifting until the next reload or restart.
                    if let Err(evict_error) = self
                        .schema_manager
                        .evict_stale_schema_cache(&job.schema_name)
                    {
                        tracing::error!(
                            error = %evict_error,
                            schema = %job.schema_name,
                            commit_id,
                            "failed to evict stale schema cache after reload failure"
                        );
                    }
                    error
                })?;

            if !job.idempotency_entries.is_empty() {
                self.db_ops
                    .metadata()
                    .batch_put_idempotency(job.idempotency_entries.clone())
                    .await
                    .map_err(|error| {
                        tracing::error!(
                            error = %error,
                            schema = %job.schema_name,
                            commit_id,
                            "deferred resident-write idempotency store failed"
                        );
                        error
                    })?;
            }

            if let Some((keys, written_at)) = &job.retention_write {
                self.db_ops
                    .schemas()
                    .record_schema_retention_writes(&job.schema_name, keys, *written_at)
                    .await
                    .map_err(|error| {
                        tracing::error!(
                            error = %error,
                            schema = %job.schema_name,
                            commit_id,
                            "deferred resident-write retention store failed"
                        );
                        error
                    })?;
            }

            if let Some(keys) = &job.retention_hash_partitions {
                self.db_ops
                    .schemas()
                    .record_schema_retention_hash_partitions(&job.schema_name, keys)
                    .await
                    .map_err(|error| {
                        tracing::error!(
                            error = %error,
                            schema = %job.schema_name,
                            commit_id,
                            "deferred resident-write retention partition store failed"
                        );
                        error
                    })?;
            }

            if let Some(batch) = &job.search_batch {
                self.deliver_index_change_batch(batch)
                    .await
                    .map_err(|error| {
                        tracing::error!(
                            error = %error,
                            schema = %job.schema_name,
                            commit_id,
                            "schema-lane Search outbox delivery failed"
                        );
                        error
                    })?;
            }

            // Complete the exact turn after every durable stage. The dirty
            // clear is then infallible, so a rejected completion keeps all pins.
            self.complete_resident_persist_turn(slot_revisions)?;
            self.clear_deferred_job_dirty_after_success(job, slot_revisions);
            Ok(())
        };
        #[cfg(feature = "cloud-sync")]
        let result = {
            let _kv_suppress = self
                .capture_router()
                .map(|router| router.enter_kv_suppress());
            crate::sync::capture::with_capture_suppressed(persist_body).await
        };
        #[cfg(not(feature = "cloud-sync"))]
        let result = persist_body.await;

        let metrics = self.db_ops.resident().metrics();
        metrics.record_deferred_persist_duration(deferred_start.elapsed());
        if result.is_err() {
            metrics.record_deferred_persist_failure();
        }
        result
    }

    // lint:fn-size-ok verbatim move from persist.rs; splitting this function is separate work
    pub(in crate::fold_db_core::mutation_manager) async fn run_purge_job(
        &self,
        job: &mut PurgeEnvelope,
        slot_revisions: &[crate::resident::PersistSlotRevision],
    ) -> Result<(), SchemaError> {
        let attempt = async {
            if job.missing == crate::fold_db_core::purge::PurgeMissingPolicy::Refuse
                && !job.validated_present
            {
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
                    }
                    crate::fold_db_core::purge::PurgeTargetPresence::Missing(error) => {
                        return Err(PurgeAttemptError::Missing(error));
                    }
                }
            }

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

            if job.retry_requires_finalize {
                let schema_name = job
                    .erasures
                    .first()
                    .map_or("", |mutation| mutation.schema_name.as_str());
                self.replay_purge_finalize(schema_name)
                    .await
                    .map_err(PurgeAttemptError::Retry)?;
                job.retry_requires_finalize = false;
            }

            if let Some(keys) = &job.retention_keys {
                let schema_name = job
                    .erasures
                    .first()
                    .map_or("", |mutation| mutation.schema_name.as_str());
                self.db_ops
                    .schemas()
                    .remove_schema_retention_keys(schema_name, keys)
                    .await
                    .map_err(PurgeAttemptError::Retry)?;
            }

            if let Some(batch) = &job.search_batch {
                self.deliver_index_change_batch(batch)
                    .await
                    .map_err(PurgeAttemptError::Retry)?;
            }

            Ok(result)
        };
        #[cfg(feature = "cloud-sync")]
        let attempt = async {
            // The lane task no longer has the request's logical-commit scope,
            // and LastStore can move its writes onto spawn_blocking. Hold both
            // suppress guards so tip and catalog deletes do not re-enter the
            // mutation log as leftover physical operations. Product requests
            // still stage MutationIntent while router depth is nonzero.
            let _kv_suppress = self
                .capture_router()
                .map(|router| router.enter_kv_suppress());
            crate::sync::capture::with_capture_suppressed(attempt).await
        };
        let (result, phases) = if job.completion.is_some() {
            let (result, phases, _) = crate::request_phases::run_with_phases(attempt).await;
            (result, phases)
        } else {
            (
                attempt.await,
                crate::request_phases::RequestPhaseTotals::default(),
            )
        };
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

    /// Run one legacy storage-slot drain page through its schema lane.
    ///
    /// A retry rebuilds the schema snapshot because a failed core attempt can
    /// reload the already-purged schema before its flush fails. The evidence
    /// checkpoint stays in the envelope and therefore survives that rebuild.
    // lint:fn-size-ok verbatim move from persist.rs; splitting this function is separate work
    pub(in crate::fold_db_core::mutation_manager) async fn run_storage_slot_purge_job(
        &self,
        job: &mut StorageSlotPurgeEnvelope,
    ) -> Result<(), SchemaError> {
        const CHUNK: usize = 64;

        let attempt = async {
            if !job.durable_complete {
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
                        crate::fold_db_core::purge::record_barrier_write_acquisition(
                            &job.schema_name,
                        );
                        self.record_schema_barrier_acquisition(&job.schema_name);
                        let held_from = std::time::Instant::now();
                        let mut acct = crate::fold_db_core::purge::PurgeCommitAccounting::default();
                        let result = match crate::fold_db_core::purge::purge_storage_slots_bulk(
                            &self.db_ops,
                            &self.schema_manager,
                            &job.schema_name,
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
            Ok::<(), SchemaError>(())
        };

        #[cfg(feature = "cloud-sync")]
        let attempt = async {
            let _kv_suppress = self
                .capture_router()
                .map(|router| router.enter_kv_suppress());
            crate::sync::capture::with_capture_suppressed(attempt).await
        };
        let (result, phases, _) = crate::request_phases::run_with_phases(attempt).await;
        result?;
        if job.completion.is_some() {
            job.pending_completion = Some(StorageSlotPurgeCompletion {
                evidence: std::mem::take(&mut job.evidence_checkpoint),
                phases,
            });
        }
        Ok(())
    }
}
