//! Lane failure policy: retry backoff, poisoned-head quarantine and the
//! quarantine breaker. These functions edit one lane's FIFO state only.

use std::sync::atomic::Ordering;
use std::time::Duration;

use super::super::metrics::ResidentMetrics;
use super::*;

pub(super) const UNHEALTHY_AFTER_FAILURES: u32 = 3;
pub(super) const RETRY_DELAY_INITIAL: Duration = Duration::from_millis(25);
pub(super) const RETRY_DELAY_MAX: Duration = Duration::from_secs(1);
/// A head envelope that fails with a deterministic error this many times in
/// a row moves to quarantine instead of closing the lane.
pub(super) const QUARANTINE_AFTER_POISONED_ATTEMPTS: u32 = 3;
/// Quarantine only helps when the poison belongs to one envelope. When this
/// many envelopes in a row quarantine without one success between them, the
/// fault may be in the store, not in an envelope. The breaker then trips: the
/// lane retries the head and closes admission, so it cannot quietly quarantine
/// every write.
pub(super) const QUARANTINE_LIMIT_WITHOUT_SUCCESS: u32 = 2;
/// Default time a tripped breaker holds the lane closed before its half-open
/// probe. `LASTDB_PERSIST_LANE_BREAKER_COOLDOWN_SECS` overrides it.
pub(super) const BREAKER_COOLDOWN_DEFAULT: Duration = Duration::from_secs(30);
/// Upper bound of the per-trip exponential cooldown.
pub(super) const BREAKER_COOLDOWN_MAX: Duration = Duration::from_secs(600);

/// Base breaker cooldown for this process.
pub(super) fn breaker_cooldown_from_env() -> Duration {
    env_flag::var_parsed::<u64>("LASTDB_PERSIST_LANE_BREAKER_COOLDOWN_SECS")
        .map_or(BREAKER_COOLDOWN_DEFAULT, Duration::from_secs)
}

/// Cooldown before the half-open probe after `trips` consecutive trips.
///
/// Each trip without a success between doubles the wait, up to
/// [`BREAKER_COOLDOWN_MAX`]. A lane whose every write is poisoned therefore
/// quarantines at a bounded, falling rate instead of either quarantining every
/// write or staying closed until a daemon restart.
pub(super) fn breaker_cooldown(base: Duration, trips: u32) -> Duration {
    let exponent = trips.saturating_sub(1).min(16);
    base.saturating_mul(1_u32 << exponent)
        .min(BREAKER_COOLDOWN_MAX.max(base))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PoisonDecision {
    /// Retry the head again.
    Retry,
    /// The head reached its poisoned-attempt limit: quarantine it.
    Quarantine,
    /// The head reached its limit, but the lane already quarantined its
    /// budget without one success. The breaker trips now: retry and let the
    /// lane close for `cooldown`.
    BreakerTripped { trips: u32, cooldown: Duration },
    /// The breaker is open and its cooldown runs. Retry; the lane stays closed.
    BreakerOpen,
    /// The cooldown ended and the head is still poisoned: quarantine it as the
    /// one half-open probe. The budget stays spent, so the next poisoned head
    /// trips the breaker again.
    HalfOpenQuarantine,
}

pub(super) fn clear_poisoned_head<T>(lane: &LaneRuntime<T>) {
    lane.fifo.lock().expect("persist lane fifo").poisoned_head = None;
}

/// Count one deterministic failure of `head_commit_id` and decide its fate.
pub(super) fn note_poisoned_head<T>(
    lane: &LaneRuntime<T>,
    head_commit_id: u64,
    prefix_completed: bool,
    breaker_cooldown_base: Duration,
) -> PoisonDecision {
    let mut fifo = lane.fifo.lock().expect("persist lane fifo");
    if prefix_completed {
        // A completed prefix proves the store works for other envelopes.
        fifo.quarantines_since_success = 0;
        fifo.breaker_trips = 0;
        fifo.breaker_open_since = None;
    }
    let attempts = match fifo.poisoned_head {
        Some((commit_id, attempts)) if commit_id == head_commit_id => attempts.saturating_add(1),
        _ => 1,
    };
    fifo.poisoned_head = Some((head_commit_id, attempts));
    if attempts < QUARANTINE_AFTER_POISONED_ATTEMPTS {
        return PoisonDecision::Retry;
    }
    if fifo.quarantines_since_success < QUARANTINE_LIMIT_WITHOUT_SUCCESS {
        return PoisonDecision::Quarantine;
    }
    // Budget spent. A closed lane accepts no new write, so the success that
    // would reset the budget can never arrive by itself. The breaker bounds
    // the closure instead of holding it until a daemon restart.
    let now = Instant::now();
    match fifo.breaker_open_since {
        None => {
            fifo.breaker_trips = fifo.breaker_trips.saturating_add(1);
            fifo.breaker_open_since = Some(now);
            PoisonDecision::BreakerTripped {
                trips: fifo.breaker_trips,
                cooldown: breaker_cooldown(breaker_cooldown_base, fifo.breaker_trips),
            }
        }
        Some(since)
            if now.saturating_duration_since(since)
                >= breaker_cooldown(breaker_cooldown_base, fifo.breaker_trips) =>
        {
            fifo.breaker_open_since = None;
            PoisonDecision::HalfOpenQuarantine
        }
        Some(_) => PoisonDecision::BreakerOpen,
    }
}

/// Release a quarantined head and requeue the untried suffix.
///
/// The head leaves the lane, so its failures no longer describe the lane: the
/// failure streak resets and an unhealthy lane opens admission again.
pub(super) fn record_quarantined<T>(
    lane: &LaneRuntime<T>,
    head_bytes: u64,
    mut untried: Vec<PersistEnvelope<T>>,
    metrics: &PersistLaneCounters,
    graph_metrics: Option<&ResidentMetrics>,
    age_tracker: &PersistLaneAgeTracker,
) {
    let mut fifo = lane.fifo.lock().expect("persist lane fifo");
    fifo.bytes = fifo.bytes.saturating_sub(head_bytes);
    fifo.in_flight_oldest = None;
    while let Some(envelope) = untried.pop() {
        fifo.queue.push_front(envelope);
    }
    fifo.poisoned_head = None;
    fifo.quarantines_since_success = fifo.quarantines_since_success.saturating_add(1);
    fifo.consecutive_failures = 0;
    let was_unhealthy = fifo.unhealthy;
    fifo.unhealthy = false;
    age_tracker.publish(&lane.key, fifo_oldest_enqueued_at(&fifo), graph_metrics);
    drop(fifo);
    if was_unhealthy {
        metrics.unhealthy_lanes.fetch_sub(1, Ordering::Relaxed);
    }
    metrics.quarantined.fetch_add(1, Ordering::Relaxed);
    metrics.depth.fetch_sub(1, Ordering::Relaxed);
    if let Some(gm) = graph_metrics {
        gm.record_persist_lane_dequeue(1);
    }
    lane.notify.notify_one();
}

/// Shared retry path: release the completed prefix, keep the failed suffix at
/// the head, count the failure, log, and back off.
#[allow(clippy::too_many_arguments)]
pub(super) async fn handle_failed_batch<T>(
    lane: &LaneRuntime<T>,
    mut remaining: Vec<PersistEnvelope<T>>,
    finished: Vec<PersistEnvelope<T>>,
    error: &str,
    attempted: u64,
    attempted_bytes: u64,
    metrics: &PersistLaneCounters,
    graph_metrics: Option<&ResidentMetrics>,
    age_tracker: &PersistLaneAgeTracker,
) {
    let returned = remaining.len() as u64;
    let returned_bytes = remaining.iter().fold(0_u64, |total, envelope| {
        total.saturating_add(envelope.bytes)
    });
    let completed = attempted.saturating_sub(returned);
    let completed_bytes = attempted_bytes.saturating_sub(returned_bytes);
    record_completed(lane, completed, completed_bytes, metrics, graph_metrics);
    remaining.sort_by_key(|envelope| envelope.commit_id);
    let consecutive_failures = prepend_failed(lane, remaining, metrics, graph_metrics, age_tracker);
    // The failed suffix is back on the FIFO before any finished waiter runs,
    // so a follow-up reserve cannot pass that suffix.
    notify_released(finished);
    metrics.record_failure();
    if let Some(gm) = graph_metrics {
        gm.record_persist_lane_failure();
    }
    if consecutive_failures >= UNHEALTHY_AFTER_FAILURES {
        tracing::error!(
            error = %error,
            schema = %lane.key.schema_name,
            storage_prefix = %lane.key.storage_prefix,
            consecutive_failures,
            returned,
            "persist lane is unhealthy; admission is closed"
        );
    } else {
        tracing::warn!(
            error = %error,
            schema = %lane.key.schema_name,
            storage_prefix = %lane.key.storage_prefix,
            consecutive_failures,
            returned,
            "persist lane write failed; retained envelopes will retry"
        );
    }
    tokio::time::sleep(retry_delay(consecutive_failures)).await;
}

pub(super) fn lane_has_no_reserved_or_queued_work<T>(lane: &LaneRuntime<T>) -> bool {
    let fifo = lane.fifo.lock().expect("persist lane fifo");
    fifo.queue.is_empty() && fifo.reserved_entries == 0
}

pub(super) fn record_completed<T>(
    lane: &LaneRuntime<T>,
    completed: u64,
    completed_bytes: u64,
    metrics: &PersistLaneCounters,
    graph_metrics: Option<&ResidentMetrics>,
) {
    if completed == 0 && completed_bytes == 0 {
        return;
    }
    let mut fifo = lane.fifo.lock().expect("persist lane fifo");
    fifo.bytes = fifo.bytes.saturating_sub(completed_bytes);
    if completed > 0 {
        fifo.quarantines_since_success = 0;
        fifo.breaker_trips = 0;
        fifo.breaker_open_since = None;
    }
    drop(fifo);
    metrics.depth.fetch_sub(completed, Ordering::Relaxed);
    if let Some(gm) = graph_metrics {
        gm.record_persist_lane_dequeue(completed);
    }
}

pub(super) fn record_success<T>(
    lane: &LaneRuntime<T>,
    completed: u64,
    completed_bytes: u64,
    metrics: &PersistLaneCounters,
    graph_metrics: Option<&ResidentMetrics>,
    age_tracker: &PersistLaneAgeTracker,
) {
    let mut fifo = lane.fifo.lock().expect("persist lane fifo");
    fifo.bytes = fifo.bytes.saturating_sub(completed_bytes);
    fifo.in_flight_oldest = None;
    let was_unhealthy = fifo.unhealthy;
    fifo.consecutive_failures = 0;
    fifo.unhealthy = false;
    fifo.poisoned_head = None;
    fifo.quarantines_since_success = 0;
    fifo.breaker_trips = 0;
    fifo.breaker_open_since = None;
    if was_unhealthy {
        metrics.unhealthy_lanes.fetch_sub(1, Ordering::Relaxed);
    }
    age_tracker.publish(&lane.key, fifo_oldest_enqueued_at(&fifo), graph_metrics);
    drop(fifo);

    metrics.depth.fetch_sub(completed, Ordering::Relaxed);
    if let Some(gm) = graph_metrics {
        gm.record_persist_lane_dequeue(completed);
    }
    if was_unhealthy {
        tracing::info!(
            schema = %lane.key.schema_name,
            storage_prefix = %lane.key.storage_prefix,
            "persist lane recovered; admission is open"
        );
    }
}

pub(super) fn prepend_failed<T>(
    lane: &LaneRuntime<T>,
    mut remaining: Vec<PersistEnvelope<T>>,
    metrics: &PersistLaneCounters,
    graph_metrics: Option<&ResidentMetrics>,
    age_tracker: &PersistLaneAgeTracker,
) -> u32 {
    let returned = remaining.len() as u64;
    let mut fifo = lane.fifo.lock().expect("persist lane fifo");
    fifo.in_flight_oldest = None;
    while let Some(envelope) = remaining.pop() {
        fifo.queue.push_front(envelope);
    }
    fifo.consecutive_failures = fifo.consecutive_failures.saturating_add(1);
    if !fifo.unhealthy && fifo.consecutive_failures >= UNHEALTHY_AFTER_FAILURES {
        fifo.unhealthy = true;
        metrics.unhealthy_lanes.fetch_add(1, Ordering::Relaxed);
    }
    if returned > 0 {
        lane.notify.notify_one();
    } else {
        tracing::error!(
            schema = %lane.key.schema_name,
            storage_prefix = %lane.key.storage_prefix,
            "persist writer returned Retry without an envelope"
        );
    }
    age_tracker.publish(&lane.key, fifo_oldest_enqueued_at(&fifo), graph_metrics);
    fifo.consecutive_failures
}

pub(super) fn retry_delay(consecutive_failures: u32) -> Duration {
    let exponent = consecutive_failures.saturating_sub(1).min(6);
    RETRY_DELAY_INITIAL
        .saturating_mul(1_u32 << exponent)
        .min(RETRY_DELAY_MAX)
}
