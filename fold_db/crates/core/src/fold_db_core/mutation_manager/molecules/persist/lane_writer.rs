//! Persist-lane worker: runs dequeued envelopes through the persist surface.

use super::*;
use futures::FutureExt;

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
    async fn write_batch(
        &self,
        batch: Vec<crate::resident::PersistEnvelope<LanePersistJob>>,
    ) -> crate::resident::PersistBatchOutcome<LanePersistJob> {
        let mut envelopes = batch.into_iter();
        let mut finished = Vec::new();
        while let Some(mut envelope) = envelopes.next() {
            if let Err(error) = self.wait_for_envelope_turn(&mut envelope).await {
                let mut remaining = Vec::with_capacity(envelopes.len() + 1);
                remaining.push(envelope);
                remaining.extend(envelopes);
                return crate::resident::PersistBatchOutcome::Retry {
                    remaining,
                    finished,
                    error,
                };
            }
            let step = self.run_envelope(&mut envelope).boxed().await;
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
        let stored = {
            let _kv_suppress = self
                .mm
                .capture_router()
                .map(|router| router.enter_kv_suppress());
            crate::sync::capture::with_capture_suppressed(store).await
        };
        if let Err(store_error) = stored {
            tracing::error!(
                error = %store_error,
                key = %key,
                "persist lane quarantine record did not persist; the lane keeps the envelope"
            );
            return Err(envelope);
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

impl DeferredLaneWriter {
    /// Wait for the author-clock barrier and the resident persist turn.
    /// Returns the retry error text when either wait fails.
    async fn wait_for_envelope_turn(
        &self,
        envelope: &mut crate::resident::PersistEnvelope<LanePersistJob>,
    ) -> Result<(), String> {
        let author_clock_barrier = match &envelope.payload {
            LanePersistJob::Purge { job, .. } => job.author_clock_barrier.as_ref(),
            LanePersistJob::StorageSlotPurge { .. } => None,
        };
        if let Some(barrier) = author_clock_barrier {
            barrier
                .wait()
                .await
                .map_err(|error| format!("author clock durability wait failed: {error}"))?;
        }
        Ok(())
    }

    /// Run one envelope through the persist surface.
    // Waiters stay on the envelope. The lane runs them after it drops
    // the byte charge. A send here races the next reserve on a 1-byte
    // lane (`LASTDB_RESIDENT_MAX_DEFERRED_BYTES=0`).
    async fn run_envelope(
        &self,
        envelope: &mut crate::resident::PersistEnvelope<LanePersistJob>,
    ) -> LaneStep {
        match &mut envelope.payload {
            LanePersistJob::Purge { job, .. } => match self.mm.run_purge_job(job).await {
                Ok(()) => LaneStep::Done(purge_release_hook(job)),
                Err(error) => LaneStep::Failed(error),
            },
            LanePersistJob::StorageSlotPurge { job, .. } => {
                match self.mm.run_storage_slot_purge_job(job).await {
                    Ok(()) => LaneStep::Done(storage_slot_release_hook(job)),
                    Err(error) => LaneStep::Failed(error),
                }
            }
        }
    }
}
