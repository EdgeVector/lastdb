//! Persist-lane worker: runs dequeued envelopes through the persist surface.

use super::*;

/// Lane worker: runs dequeued envelopes through the persist surface.
pub(in crate::fold_db_core::mutation_manager) struct DeferredLaneWriter {
    pub mm: super::super::super::MutationManager,
}

pub(super) enum LaneStep {
    Done(Option<Box<dyn FnOnce() + Send>>),
    Failed(SchemaError),
}

pub(super) fn purge_release_hook(job: &mut PurgeEnvelope) -> Option<Box<dyn FnOnce() + Send>> {
    let message = job.pending_completion.take()?;
    let tx = job.completion.take()?;
    Some(Box::new(move || {
        let _ = tx.send(message);
    }))
}

pub(super) fn storage_slot_release_hook(
    job: &mut StorageSlotPurgeEnvelope,
) -> Option<Box<dyn FnOnce() + Send>> {
    let message = job.pending_completion.take()?;
    let tx = job.completion.take()?;
    Some(Box::new(move || {
        let _ = tx.send(message);
    }))
}

#[async_trait::async_trait]
impl crate::resident::PersistLaneWriter<LanePersistJob> for DeferredLaneWriter {
    // lint:fn-size-ok moved verbatim from its original module
    async fn write_batch(
        &self,
        batch: Vec<crate::resident::PersistEnvelope<LanePersistJob>>,
    ) -> crate::resident::PersistBatchOutcome<LanePersistJob> {
        let mut envelopes = batch.into_iter();
        let mut finished = Vec::new();
        while let Some(mut envelope) = envelopes.next() {
            let author_clock_barrier = match &envelope.payload {
                LanePersistJob::Write { job, .. } => job.author_clock_barrier.as_ref(),
                LanePersistJob::Purge { job, .. } => job.author_clock_barrier.as_ref(),
                LanePersistJob::StorageSlotPurge { .. } => None,
            };
            if let Some(barrier) = author_clock_barrier {
                if let Err(error) = barrier.wait().await {
                    let mut remaining = Vec::with_capacity(envelopes.len() + 1);
                    remaining.push(envelope);
                    remaining.extend(envelopes);
                    return crate::resident::PersistBatchOutcome::Retry {
                        remaining,
                        finished,
                        error: format!("author clock durability wait failed: {error}"),
                    };
                }
            }
            let owns_resident_turn = matches!(
                &envelope.payload,
                LanePersistJob::Write { .. } | LanePersistJob::Purge { .. }
            );
            if owns_resident_turn {
                if let Err(error) = self
                    .mm
                    .db_ops
                    .resident()
                    .wait_for_persist_turn(&envelope.slot_revisions)
                    .await
                {
                    let mut remaining = Vec::with_capacity(envelopes.len() + 1);
                    remaining.push(envelope);
                    remaining.extend(envelopes);
                    return crate::resident::PersistBatchOutcome::Retry {
                        remaining,
                        finished,
                        error: format!("resident persist turn wait failed: {error}"),
                    };
                }
            }
            // Waiters stay on the envelope. The lane runs them after it drops
            // the byte charge. A send here races the next reserve on a 1-byte
            // lane (`LASTDB_RESIDENT_MAX_DEFERRED_BYTES=0`).
            let step = match &mut envelope.payload {
                LanePersistJob::Write {
                    job,
                    counts,
                    counts_completion,
                    ..
                } => {
                    let batch_log = job.batch_log.clone();
                    let attempt =
                        self.mm
                            .run_deferred_job(job, envelope.commit_id, &envelope.slot_revisions);
                    let attempt = async move {
                        if let Some(log) = batch_log {
                            crate::durable_flush::scope(log, attempt).await
                        } else {
                            attempt.await
                        }
                    };
                    if counts_completion.is_some() {
                        let (result, _, attempt_counts) =
                            crate::request_phases::run_with_phases(attempt).await;
                        counts.molecules_persisted = counts
                            .molecules_persisted
                            .saturating_add(attempt_counts.molecules_persisted);
                        counts.molecule_store_commits = counts
                            .molecule_store_commits
                            .saturating_add(attempt_counts.molecule_store_commits);
                        match result {
                            Ok(()) => LaneStep::Done(counts_completion.take().map(|tx| {
                                let counts = *counts;
                                Box::new(move || {
                                    let _ = tx.send(counts);
                                }) as Box<dyn FnOnce() + Send>
                            })),
                            Err(error) => LaneStep::Failed(error),
                        }
                    } else {
                        match attempt.await {
                            Ok(()) => LaneStep::Done(None),
                            Err(error) => LaneStep::Failed(error),
                        }
                    }
                }
                LanePersistJob::Purge { job, .. } => {
                    match self.mm.run_purge_job(job, &envelope.slot_revisions).await {
                        Ok(()) => LaneStep::Done(purge_release_hook(job)),
                        Err(error) => LaneStep::Failed(error),
                    }
                }
                LanePersistJob::StorageSlotPurge { job, .. } => {
                    match self.mm.run_storage_slot_purge_job(job).await {
                        Ok(()) => LaneStep::Done(storage_slot_release_hook(job)),
                        Err(error) => LaneStep::Failed(error),
                    }
                }
            };
            match step {
                LaneStep::Done(hook) => {
                    envelope.after_release = hook;
                    finished.push(envelope);
                }
                LaneStep::Failed(error) => {
                    // Name the job and its targets: a bare decode error on the
                    // lane ERROR line cannot identify the stuck envelope.
                    let described = format!(
                        "{} commit_id={}: {error}",
                        describe_lane_job(&envelope),
                        envelope.commit_id
                    );
                    let poisoned = is_deterministic_decode_error(&error);
                    let mut remaining = Vec::with_capacity(envelopes.len() + 1);
                    remaining.push(envelope);
                    remaining.extend(envelopes);
                    if poisoned {
                        return crate::resident::PersistBatchOutcome::Poisoned {
                            remaining,
                            finished,
                            error: described,
                        };
                    }
                    return crate::resident::PersistBatchOutcome::Retry {
                        remaining,
                        finished,
                        error: described,
                    };
                }
            }
        }
        crate::resident::PersistBatchOutcome::Released(finished)
    }

    async fn quarantine(
        &self,
        envelope: crate::resident::PersistEnvelope<LanePersistJob>,
        error: &str,
    ) -> Result<(), crate::resident::PersistEnvelope<LanePersistJob>> {
        let record = quarantine_record(&envelope, error);
        let key = format!(
            "{PERSIST_LANE_QUARANTINE_PREFIX}{}:{}:{}",
            lane_job_schema(&envelope.payload),
            chrono::Utc::now().timestamp_millis(),
            envelope.commit_id
        );
        let store = async {
            self.mm
                .db_ops
                .metadata()
                .put_typed_durable(&key, &record)
                .await
        };
        // The quarantine row is node-local forensic state, never a mutation
        // for other devices.
        #[cfg(feature = "cloud-sync")]
        let stored = {
            let _kv_suppress = self
                .mm
                .capture_router()
                .map(|router| router.enter_kv_suppress());
            crate::sync::capture::with_capture_suppressed(store).await
        };
        #[cfg(not(feature = "cloud-sync"))]
        let stored = store.await;
        if let Err(store_error) = stored {
            tracing::error!(
                error = %store_error,
                key = %key,
                "persist lane quarantine record did not persist; the lane keeps the envelope"
            );
            return Err(envelope);
        }

        // Release this envelope's slot turns so later envelopes on the same
        // slots can run. The durable row keeps its older value; the resident
        // dirty pins stay, so eviction cannot drop the in-memory value.
        let lock_keys = envelope
            .slot_revisions
            .iter()
            .map(|ticket| {
                MutationManager::molecule_persist_lock_key(
                    &ticket.molecule_uuid,
                    Some(&ticket.disk_hash),
                    Some(&ticket.disk_range),
                    None,
                )
            })
            .collect();
        let _guards = self.mm.acquire_molecule_write_lock_keys(lock_keys).await;
        if let Err(turn_error) = self
            .mm
            .complete_resident_persist_turn(&envelope.slot_revisions)
        {
            tracing::error!(
                error = %turn_error,
                key = %key,
                "quarantined envelope could not release its resident persist turn"
            );
        }
        tracing::error!(
            key = %key,
            bytes = envelope.bytes,
            "persist lane envelope quarantined; its durable caller receives an error \
             and the record keeps its content for repair"
        );
        // Dropping the envelope drops its completion senders, so a durable
        // waiter gets an error, never a false durable receipt.
        drop(envelope);
        Ok(())
    }
}
