//! The per-lane worker task: dequeue a batch, write it, and apply the
//! health policy to the outcome.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use super::super::metrics::ResidentMetrics;
use super::health::*;
use super::*;

pub(super) async fn run_lane_worker<T: Send + 'static>(
    lane: Arc<LaneRuntime<T>>,
    writer: Arc<dyn PersistLaneWriter<T>>,
    metrics: Arc<PersistLaneCounters>,
    graph_metrics: Option<Arc<ResidentMetrics>>,
    age_tracker: Arc<PersistLaneAgeTracker>,
    group_commit_max: usize,
    breaker_cooldown_base: Duration,
) {
    // lint:fn-size-ok moved verbatim from lane.rs; the loop is one state machine over a shared batch
    loop {
        let batch = dequeue_batch(
            &lane,
            group_commit_max,
            &age_tracker,
            graph_metrics.as_deref(),
        );
        if batch.is_empty() {
            if lane.stop.load(Ordering::Relaxed) && lane_has_no_reserved_or_queued_work(&lane) {
                break;
            }
            lane.notify.notified().await;
            continue;
        }
        let attempted = batch.len() as u64;
        let attempted_bytes = batch.iter().fold(0_u64, |total, envelope| {
            total.saturating_add(envelope.bytes)
        });
        match writer.write_batch(batch).await {
            PersistBatchOutcome::Complete => {
                record_success(
                    &lane,
                    attempted,
                    attempted_bytes,
                    &metrics,
                    graph_metrics.as_deref(),
                    &age_tracker,
                );
            }
            PersistBatchOutcome::Released(envelopes) => {
                record_success(
                    &lane,
                    attempted,
                    attempted_bytes,
                    &metrics,
                    graph_metrics.as_deref(),
                    &age_tracker,
                );
                notify_released(envelopes);
            }
            PersistBatchOutcome::Retry {
                remaining,
                finished,
                error,
            } => {
                clear_poisoned_head(&lane);
                handle_failed_batch(
                    &lane,
                    remaining,
                    finished,
                    &error,
                    attempted,
                    attempted_bytes,
                    &metrics,
                    graph_metrics.as_deref(),
                    &age_tracker,
                )
                .await;
            }
            PersistBatchOutcome::Poisoned {
                mut remaining,
                finished,
                error,
            } => {
                let returned_bytes = remaining.iter().fold(0_u64, |total, envelope| {
                    total.saturating_add(envelope.bytes)
                });
                let completed = attempted.saturating_sub(remaining.len() as u64);
                let completed_bytes = attempted_bytes.saturating_sub(returned_bytes);
                remaining.sort_by_key(|envelope| envelope.commit_id);
                let decision = remaining.first().map(|head| {
                    note_poisoned_head(&lane, head.commit_id, completed > 0, breaker_cooldown_base)
                });
                if decision == Some(PoisonDecision::HalfOpenQuarantine) {
                    tracing::error!(
                        error = %error,
                        schema = %lane.key.schema_name,
                        storage_prefix = %lane.key.storage_prefix,
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
                        &lane,
                        completed,
                        completed_bytes,
                        &metrics,
                        graph_metrics.as_deref(),
                    );
                    let head = remaining.remove(0);
                    let commit_id = head.commit_id;
                    let head_bytes = head.bytes;
                    let slots = head.slot_revisions.len();
                    let molecules: Vec<String> = head
                        .slot_revisions
                        .iter()
                        .map(|slot| slot.molecule_uuid.clone())
                        .collect();
                    match writer.quarantine(head, &error).await {
                        Ok(()) => {
                            tracing::error!(
                                error = %error,
                                schema = %lane.key.schema_name,
                                storage_prefix = %lane.key.storage_prefix,
                                commit_id,
                                bytes = head_bytes,
                                slots,
                                molecules = ?molecules,
                                untried = remaining.len(),
                                "persist lane QUARANTINED an envelope that fails \
                                 deterministically; the lane keeps serving other writes"
                            );
                            record_quarantined(
                                &lane,
                                head_bytes,
                                remaining,
                                &metrics,
                                graph_metrics.as_deref(),
                                &age_tracker,
                            );
                            notify_released(finished);
                            continue;
                        }
                        Err(head) => {
                            tracing::error!(
                                error = %error,
                                schema = %lane.key.schema_name,
                                storage_prefix = %lane.key.storage_prefix,
                                commit_id,
                                bytes = head_bytes,
                                "persist writer could not preserve a poisoned envelope; \
                                 the lane retains it"
                            );
                            remaining.insert(0, head);
                            // `record_completed` already released the prefix.
                            handle_failed_batch(
                                &lane,
                                remaining,
                                finished,
                                &error,
                                0,
                                0,
                                &metrics,
                                graph_metrics.as_deref(),
                                &age_tracker,
                            )
                            .await;
                            continue;
                        }
                    }
                }
                if let Some(PoisonDecision::BreakerTripped { trips, cooldown }) = decision {
                    metrics.breaker_trips.fetch_add(1, Ordering::Relaxed);
                    tracing::error!(
                        error = %error,
                        schema = %lane.key.schema_name,
                        storage_prefix = %lane.key.storage_prefix,
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
                handle_failed_batch(
                    &lane,
                    remaining,
                    finished,
                    &error,
                    attempted,
                    attempted_bytes,
                    &metrics,
                    graph_metrics.as_deref(),
                    &age_tracker,
                )
                .await;
            }
        }
    }
    lane.drained.store(true, Ordering::Release);
    lane.drain_notify.notify_waiters();
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
