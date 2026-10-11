//! The per-lane worker task: dequeue a batch, write it, and apply the
//! health policy to the outcome.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use super::super::metrics::ResidentMetrics;
use super::health::*;
use super::*;

/// Everything one lane worker shares across the batches it writes.
struct LaneWorker<T> {
    lane: Arc<LaneRuntime<T>>,
    writer: Arc<dyn PersistLaneWriter<T>>,
    metrics: Arc<PersistLaneCounters>,
    graph_metrics: Option<Arc<ResidentMetrics>>,
    age_tracker: Arc<PersistLaneAgeTracker>,
    breaker_cooldown_base: Duration,
}

/// Count and byte size of the batch a writer call attempted.
#[derive(Clone, Copy)]
struct Attempt {
    count: u64,
    bytes: u64,
}

fn envelope_bytes<T>(envelopes: &[PersistEnvelope<T>]) -> u64 {
    envelopes.iter().fold(0_u64, |total, envelope| {
        total.saturating_add(envelope.bytes)
    })
}

pub(super) async fn run_lane_worker<T: Send + 'static>(
    lane: Arc<LaneRuntime<T>>,
    writer: Arc<dyn PersistLaneWriter<T>>,
    metrics: Arc<PersistLaneCounters>,
    graph_metrics: Option<Arc<ResidentMetrics>>,
    age_tracker: Arc<PersistLaneAgeTracker>,
    group_commit_max: usize,
    breaker_cooldown_base: Duration,
) {
    let worker = LaneWorker {
        lane,
        writer,
        metrics,
        graph_metrics,
        age_tracker,
        breaker_cooldown_base,
    };
    loop {
        let batch = dequeue_batch(
            &worker.lane,
            group_commit_max,
            &worker.age_tracker,
            worker.graph_metrics.as_deref(),
        );
        if batch.is_empty() {
            if worker.lane.stop.load(Ordering::Relaxed)
                && lane_has_no_reserved_or_queued_work(&worker.lane)
            {
                break;
            }
            worker.lane.notify.notified().await;
            continue;
        }
        let attempt = Attempt {
            count: batch.len() as u64,
            bytes: envelope_bytes(&batch),
        };
        match worker.writer.write_batch(batch).await {
            PersistBatchOutcome::Complete => worker.record_success(attempt),
            PersistBatchOutcome::Released(envelopes) => {
                worker.record_success(attempt);
                notify_released(envelopes);
            }
            PersistBatchOutcome::Retry {
                remaining,
                finished,
                error,
            } => {
                clear_poisoned_head(&worker.lane);
                worker
                    .fail_batch(remaining, finished, &error, attempt)
                    .await;
            }
            PersistBatchOutcome::Poisoned {
                remaining,
                finished,
                error,
            } => {
                worker
                    .handle_poisoned(remaining, finished, error, attempt)
                    .await;
            }
        }
    }
    worker.lane.drained.store(true, Ordering::Release);
    worker.lane.drain_notify.notify_waiters();
}

impl<T: Send + 'static> LaneWorker<T> {
    fn record_success(&self, attempt: Attempt) {
        record_success(
            &self.lane,
            attempt.count,
            attempt.bytes,
            &self.metrics,
            self.graph_metrics.as_deref(),
            &self.age_tracker,
        );
    }

    async fn fail_batch(
        &self,
        remaining: Vec<PersistEnvelope<T>>,
        finished: Vec<PersistEnvelope<T>>,
        error: &str,
        attempt: Attempt,
    ) {
        handle_failed_batch(
            &self.lane,
            remaining,
            finished,
            error,
            attempt.count,
            attempt.bytes,
            &self.metrics,
            self.graph_metrics.as_deref(),
            &self.age_tracker,
        )
        .await;
    }

    /// Apply the quarantine breaker to a deterministic failure: quarantine
    /// the head when the breaker allows it, otherwise retry the batch.
    async fn handle_poisoned(
        &self,
        mut remaining: Vec<PersistEnvelope<T>>,
        finished: Vec<PersistEnvelope<T>>,
        error: String,
        attempt: Attempt,
    ) {
        let returned_bytes = envelope_bytes(&remaining);
        let completed = attempt.count.saturating_sub(remaining.len() as u64);
        let completed_bytes = attempt.bytes.saturating_sub(returned_bytes);
        remaining.sort_by_key(|envelope| envelope.commit_id);
        let decision = remaining.first().map(|head| {
            note_poisoned_head(
                &self.lane,
                head.commit_id,
                completed > 0,
                self.breaker_cooldown_base,
            )
        });
        if decision == Some(PoisonDecision::HalfOpenQuarantine) {
            tracing::error!(
                error = %error,
                schema = %self.lane.key.schema_name,
                storage_prefix = %self.lane.key.storage_prefix,
                "persist lane quarantine breaker HALF-OPEN: the head stayed \
                 poisoned for the whole cooldown; quarantining it and \
                 reopening admission. The next poisoned envelope trips the \
                 breaker again with a longer cooldown"
            );
        }
        if matches!(
            decision,
            Some(PoisonDecision::Quarantine | PoisonDecision::HalfOpenQuarantine)
        ) {
            record_completed(
                &self.lane,
                completed,
                completed_bytes,
                &self.metrics,
                self.graph_metrics.as_deref(),
            );
            self.quarantine_head(remaining, finished, &error).await;
            return;
        }
        if let Some(PoisonDecision::BreakerTripped { trips, cooldown }) = decision {
            self.metrics.breaker_trips.fetch_add(1, Ordering::Relaxed);
            tracing::error!(
                error = %error,
                schema = %self.lane.key.schema_name,
                storage_prefix = %self.lane.key.storage_prefix,
                limit = QUARANTINE_LIMIT_WITHOUT_SUCCESS,
                trips,
                cooldown_secs = cooldown.as_secs_f64(),
                "persist lane quarantine breaker TRIPPED: poison repeats \
                 across envelopes, so the lane retries the head and closes \
                 admission. A success closes the breaker; otherwise the head \
                 is quarantined after the cooldown (half-open) and admission \
                 reopens without a restart"
            );
        }
        self.fail_batch(remaining, finished, &error, attempt).await;
    }

    /// Hand the poisoned head to the writer. On refusal the lane keeps it and
    /// retries.
    async fn quarantine_head(
        &self,
        mut remaining: Vec<PersistEnvelope<T>>,
        finished: Vec<PersistEnvelope<T>>,
        error: &str,
    ) {
        let head = remaining.remove(0);
        let commit_id = head.commit_id;
        let head_bytes = head.bytes;
        let slots = head.slot_revisions.len();
        let molecules: Vec<String> = head
            .slot_revisions
            .iter()
            .map(|slot| slot.molecule_uuid.clone())
            .collect();
        match self.writer.quarantine(head, error).await {
            Ok(()) => {
                tracing::error!(
                    error = %error,
                    schema = %self.lane.key.schema_name,
                    storage_prefix = %self.lane.key.storage_prefix,
                    commit_id,
                    bytes = head_bytes,
                    slots,
                    molecules = ?molecules,
                    untried = remaining.len(),
                    "persist lane QUARANTINED an envelope that fails \
                     deterministically; the lane keeps serving other writes"
                );
                record_quarantined(
                    &self.lane,
                    head_bytes,
                    remaining,
                    &self.metrics,
                    self.graph_metrics.as_deref(),
                    &self.age_tracker,
                );
                notify_released(finished);
            }
            Err(head) => {
                tracing::error!(
                    error = %error,
                    schema = %self.lane.key.schema_name,
                    storage_prefix = %self.lane.key.storage_prefix,
                    commit_id,
                    bytes = head_bytes,
                    "persist writer could not preserve a poisoned envelope; \
                     the lane retains it"
                );
                remaining.insert(0, head);
                // `record_completed` already released the prefix.
                self.fail_batch(remaining, finished, error, Attempt { count: 0, bytes: 0 })
                    .await;
            }
        }
    }
}

pub(super) async fn wait_for_lane_drain<T>(lane: &LaneRuntime<T>) {
    loop {
        if lane.drained.load(Ordering::Acquire) {
            return;
        }
        let notified = lane.drain_notify.notified();
        if lane.drained.load(Ordering::Acquire) {
            return;
        }
        notified.await;
    }
}

pub(super) fn dequeue_batch<T>(
    lane: &LaneRuntime<T>,
    max: usize,
    age_tracker: &PersistLaneAgeTracker,
    graph_metrics: Option<&ResidentMetrics>,
) -> Vec<PersistEnvelope<T>> {
    let mut fifo = lane.fifo.lock().expect("persist lane fifo");
    let take = fifo.queue.len().min(max);
    if take == 0 {
        return Vec::new();
    }
    let mut out = Vec::with_capacity(take);
    for _ in 0..take {
        if let Some(env) = fifo.queue.pop_front() {
            out.push(env);
        }
    }
    fifo.in_flight_oldest = out.first().map(|envelope| envelope.enqueued_at);
    age_tracker.publish(&lane.key, fifo_oldest_enqueued_at(&fifo), graph_metrics);
    out
}
