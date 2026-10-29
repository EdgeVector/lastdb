//! Per-schema FIFO persist lanes and immutable persist envelopes.
//!
//! One lane per `(storage_prefix, schema_name)`. Envelopes write in commit
//! order. Adjacent envelopes on the same lane may share one storage commit
//! when that does not change write order. Different lanes run in
//! parallel: a stalled lane delays only its schema.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use tokio::sync::Notify;

use super::metrics::PersistLaneMetrics;

mod counters;
mod envelope;
mod health;
mod status;
mod worker;

use health::*;
use worker::*;

pub use counters::*;
pub use envelope::*;

/// Poll interval for [`PersistLaneSet::wait_for_quiescent`].
const QUIESCENT_POLL: Duration = Duration::from_millis(10);

struct FifoState<T> {
    queue: VecDeque<PersistEnvelope<T>>,
    bytes: u64,
    in_flight_oldest: Option<Instant>,
    /// Capacity held by [`PersistReservation`] that has not yet filled.
    reserved_entries: usize,
    reserved_bytes: u64,
    next_commit_id: u64,
    consecutive_failures: u32,
    unhealthy: bool,
    /// `(commit_id, attempts)` of a head that failed deterministically.
    poisoned_head: Option<(u64, u32)>,
    /// Quarantined envelopes since the last successful write.
    quarantines_since_success: u32,
    /// When the quarantine breaker tripped. `None` while it is closed or
    /// half-open. The lane is unhealthy while this is set.
    breaker_open_since: Option<Instant>,
    /// Trips since the last successful write; sets the cooldown.
    breaker_trips: u32,
}

fn fifo_oldest_enqueued_at<T>(fifo: &FifoState<T>) -> Option<Instant> {
    match (fifo.queue.front(), fifo.in_flight_oldest) {
        (Some(front), Some(in_flight)) => Some(front.enqueued_at.min(in_flight)),
        (Some(front), None) => Some(front.enqueued_at),
        (None, Some(in_flight)) => Some(in_flight),
        (None, None) => None,
    }
}

#[derive(Default)]
struct PersistLaneAgeTracker {
    oldest_by_lane: Mutex<HashMap<PersistLaneKey, Instant>>,
}

impl PersistLaneAgeTracker {
    fn publish(
        &self,
        key: &PersistLaneKey,
        oldest: Option<Instant>,
        shared_metrics: Option<&PersistLaneMetrics>,
    ) {
        let Some(shared_metrics) = shared_metrics else {
            return;
        };
        let mut oldest_by_lane = self
            .oldest_by_lane
            .lock()
            .expect("persist lane age tracker");
        match oldest {
            Some(oldest) => {
                oldest_by_lane.insert(key.clone(), oldest);
            }
            None => {
                oldest_by_lane.remove(key);
            }
        }
        let oldest = oldest_by_lane.values().copied().min();
        // Publish while the tracker lock is held. This keeps a slower, older
        // transition from overwriting a newer aggregate value.
        shared_metrics.set_oldest_enqueued_at(oldest);
    }
}

/// One persist-queue slot reserved before apply-gate acquisition.
///
/// Drop without [`PersistReservation::fill`] returns the capacity. Fill
/// converts the slot into a queued envelope.
pub struct PersistReservation<T: Send + 'static> {
    lane: Arc<LaneRuntime<T>>,
    metrics: Arc<PersistLaneCounters>,
    shared_metrics: Option<Arc<PersistLaneMetrics>>,
    age_tracker: Arc<PersistLaneAgeTracker>,
    bytes: u64,
    armed: bool,
}

struct LaneRuntime<T> {
    key: PersistLaneKey,
    fifo: Mutex<FifoState<T>>,
    notify: Notify,
    stop: AtomicBool,
    drained: AtomicBool,
    drain_notify: Notify,
    join: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

/// Bounded FIFO lanes keyed by schema instance.
pub struct PersistLaneSet<T: Send + 'static> {
    admission: RwLock<()>,
    lanes: Mutex<HashMap<PersistLaneKey, Arc<LaneRuntime<T>>>>,
    max_entries: usize,
    max_bytes: u64,
    group_commit_max: usize,
    writer: Arc<dyn PersistLaneWriter<T>>,
    metrics: Arc<PersistLaneCounters>,
    shared_metrics: Option<Arc<PersistLaneMetrics>>,
    age_tracker: Arc<PersistLaneAgeTracker>,
    stopped: AtomicBool,
    /// Base cooldown of the quarantine breaker (see [`breaker_cooldown`]).
    breaker_cooldown: Duration,
}

mod set;
