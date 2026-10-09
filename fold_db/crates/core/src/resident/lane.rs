//! Per-schema FIFO persist lanes and immutable persist envelopes.
//!
//! One lane per `(storage_prefix, schema_name)`. Envelopes write in commit
//! order. Adjacent envelopes on the same lane may share one LastStore group
//! commit when that does not change write order. Different lanes run in
//! parallel: a stalled lane delays only its schema.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use tokio::sync::Notify;

use super::metrics::ResidentMetrics;

mod health;
mod worker;

use health::*;
use worker::*;

/// Poll interval for [`PersistLaneSet::wait_for_quiescent`].
const QUIESCENT_POLL: Duration = Duration::from_millis(10);

/// Slot identity and exact resident/durable revisions carried on an envelope.
///
/// The lane waits for `durable_revision`, writes the complete envelope, and
/// then advances the slot to `resident_revision`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistSlotRevision {
    pub molecule_uuid: String,
    pub disk_hash: String,
    pub disk_range: String,
    pub resident_revision: u64,
    pub durable_revision: u64,
}

/// Lane identity: one FIFO per schema instance.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PersistLaneKey {
    pub storage_prefix: String,
    pub schema_name: String,
}

impl PersistLaneKey {
    pub fn new(storage_prefix: impl Into<String>, schema_name: impl Into<String>) -> Self {
        Self {
            storage_prefix: storage_prefix.into(),
            schema_name: schema_name.into(),
        }
    }
}

/// Immutable persist envelope — one acknowledged mutation batch.
///
/// `payload` is the mutation batch (atoms, tips, key-set changes, schema
/// metadata, mutation/delete events, search-index events, idempotency
/// result). Optional `completion` and `after_release` run only after the
/// lane drops this envelope's byte charge. A send from inside `write_batch`
/// wakes the next reserve while the charge is still held.
pub struct PersistEnvelope<T> {
    pub commit_id: u64,
    pub slot_revisions: Vec<PersistSlotRevision>,
    pub bytes: u64,
    pub enqueued_at: Instant,
    pub completion: Option<tokio::sync::oneshot::Sender<()>>,
    /// Runs on the lane worker after admission for this envelope is released.
    pub after_release: Option<Box<dyn FnOnce() + Send>>,
    pub payload: T,
}

/// Result of one persist-lane batch write.
///
/// A writer returns ownership of every failed or untried envelope in original
/// commit order. The lane retries that suffix before any later envelope. This
/// ownership-return contract does not copy a payload.
#[must_use]
pub enum PersistBatchOutcome<T> {
    /// The writer made every envelope durable.
    ///
    /// The envelopes are dropped inside `write_batch`. Use [`Self::Released`]
    /// when a waiter must run after the lane drops the byte charge.
    Complete,
    /// Every envelope is durable, and the lane still owns the waiters.
    ///
    /// The lane releases the byte charge, then runs `after_release` and sends
    /// `completion`. A writer must not send those waiters itself.
    Released(Vec<PersistEnvelope<T>>),
    /// The writer could not make `remaining` durable.
    ///
    /// `remaining` must contain only envelopes from the input batch. It must
    /// contain the first failed envelope and every untried envelope.
    /// `finished` is the durable prefix. The lane notifies it after the
    /// failed suffix is back on the FIFO.
    Retry {
        remaining: Vec<PersistEnvelope<T>>,
        finished: Vec<PersistEnvelope<T>>,
        error: String,
    },
    /// The first envelope of `remaining` failed with a deterministic error.
    ///
    /// A deterministic error does not change on retry, for example a durable
    /// row that does not decode. The lane retries the head a few times, then
    /// hands it to [`PersistLaneWriter::quarantine`] and continues with the
    /// rest. `remaining` has the same contract as [`Self::Retry`].
    Poisoned {
        remaining: Vec<PersistEnvelope<T>>,
        finished: Vec<PersistEnvelope<T>>,
        error: String,
    },
}

impl<T> PersistEnvelope<T> {
    pub fn new(payload: T, bytes: u64) -> Self {
        Self {
            commit_id: 0,
            slot_revisions: Vec::new(),
            bytes,
            enqueued_at: Instant::now(),
            completion: None,
            after_release: None,
            payload,
        }
    }
}

/// Wake durable waiters after their byte charge is gone.
fn notify_released<T>(envelopes: Vec<PersistEnvelope<T>>) {
    for mut envelope in envelopes {
        if let Some(hook) = envelope.after_release.take() {
            hook();
        }
        if let Some(tx) = envelope.completion.take() {
            let _ = tx.send(());
        }
    }
}

/// Why [`PersistLaneSet::enqueue`] refused an envelope.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PersistLaneFull {
    Entries,
    Bytes,
    /// The lane rejected admission after repeated durable-store failures.
    Unhealthy,
}

/// Writer invoked by a lane worker with one or more adjacent envelopes.
#[async_trait]
pub trait PersistLaneWriter<T: Send + 'static>: Send + Sync + 'static {
    async fn write_batch(&self, batch: Vec<PersistEnvelope<T>>) -> PersistBatchOutcome<T>;

    /// Preserve one poisoned envelope outside the lane and release its turn.
    ///
    /// `Ok(())` means the writer kept a durable record of the envelope and
    /// released every resource that later envelopes wait on. `Err` gives the
    /// envelope back. The lane then keeps it at the head and uses the normal
    /// retry and unhealthy path. The default refuses: a writer that cannot
    /// preserve an envelope must never lose one.
    async fn quarantine(
        &self,
        envelope: PersistEnvelope<T>,
        _error: &str,
    ) -> Result<(), PersistEnvelope<T>> {
        Err(envelope)
    }
}

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
        graph_metrics: Option<&ResidentMetrics>,
    ) {
        let Some(graph_metrics) = graph_metrics else {
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
        graph_metrics.set_persist_lane_oldest_enqueued_at(oldest);
    }
}

/// One persist-queue slot reserved before apply-gate acquisition.
///
/// Drop without [`PersistReservation::fill`] returns the capacity. Fill
/// converts the slot into a queued envelope.
pub struct PersistReservation<T: Send + 'static> {
    lane: Arc<LaneRuntime<T>>,
    metrics: Arc<PersistLaneCounters>,
    graph_metrics: Option<Arc<ResidentMetrics>>,
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
    graph_metrics: Option<Arc<ResidentMetrics>>,
    age_tracker: Arc<PersistLaneAgeTracker>,
    stopped: AtomicBool,
    /// Base cooldown of the quarantine breaker (see [`breaker_cooldown`]).
    breaker_cooldown: Duration,
}

/// Process-local lane counters (also mirrored onto [`super::ResidentMetrics`]).
#[derive(Debug, Default)]
pub struct PersistLaneCounters {
    depth: AtomicU64,
    failures: AtomicU64,
    unhealthy_lanes: AtomicU64,
    /// Reserved status field for a future aggregate slot-lag gauge.
    resident_minus_durable_revision: AtomicU64,
    refuse_bytes: AtomicU64,
    refuse_entries: AtomicU64,
    refuse_unhealthy: AtomicU64,
    write_throughs: AtomicU64,
    /// Envelopes moved to quarantine (process lifetime).
    quarantined: AtomicU64,
    /// Quarantine-breaker trips (process lifetime). Each trip closed one
    /// lane's admission until its half-open probe.
    breaker_trips: AtomicU64,
}

/// Occupancy of one persist lane for status / ops.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistLaneOccupancy {
    pub storage_prefix: String,
    pub schema_name: String,
    /// Bytes in the queue or in an active writer call.
    pub queued_bytes: u64,
    pub reserved_bytes: u64,
    pub queued_entries: u64,
    pub reserved_entries: u64,
    pub unhealthy: bool,
}

impl PersistLaneOccupancy {
    #[must_use]
    pub fn occupied_bytes(&self) -> u64 {
        self.queued_bytes.saturating_add(self.reserved_bytes)
    }
}

/// Cheap persist-lane pressure for `lastdb status` / `lastdb ops`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PersistLanePressure {
    pub fair_share_bytes: u64,
    pub write_through_threshold_bytes: u64,
    pub heaviest_schema: String,
    pub heaviest_bytes: u64,
    pub queued_entries: u64,
    pub reserved_entries: u64,
    pub oldest_age_ms: u64,
    pub refuse_bytes: u64,
    pub refuse_entries: u64,
    pub refuse_unhealthy: u64,
    pub write_throughs: u64,
    pub unhealthy_lanes: u64,
    /// Envelopes moved to quarantine since process start.
    pub quarantined: u64,
    /// Quarantine-breaker trips since process start.
    pub breaker_trips: u64,
}

impl PersistLaneCounters {
    pub fn depth(&self) -> u64 {
        self.depth.load(Ordering::Relaxed)
    }

    pub fn failures(&self) -> u64 {
        self.failures.load(Ordering::Relaxed)
    }

    pub fn unhealthy_lanes(&self) -> u64 {
        self.unhealthy_lanes.load(Ordering::Relaxed)
    }

    pub fn resident_minus_durable_revision(&self) -> u64 {
        self.resident_minus_durable_revision.load(Ordering::Relaxed)
    }

    pub fn refuse_bytes(&self) -> u64 {
        self.refuse_bytes.load(Ordering::Relaxed)
    }

    pub fn refuse_entries(&self) -> u64 {
        self.refuse_entries.load(Ordering::Relaxed)
    }

    pub fn refuse_unhealthy(&self) -> u64 {
        self.refuse_unhealthy.load(Ordering::Relaxed)
    }

    pub fn write_throughs(&self) -> u64 {
        self.write_throughs.load(Ordering::Relaxed)
    }

    pub fn quarantined(&self) -> u64 {
        self.quarantined.load(Ordering::Relaxed)
    }

    pub fn breaker_trips(&self) -> u64 {
        self.breaker_trips.load(Ordering::Relaxed)
    }

    pub fn record_failure(&self) {
        self.failures.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_refuse_bytes(&self) {
        self.refuse_bytes.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_refuse_entries(&self) {
        self.refuse_entries.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_refuse_unhealthy(&self) {
        self.refuse_unhealthy.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_write_through(&self) {
        self.write_throughs.fetch_add(1, Ordering::Relaxed);
    }
}

impl<T: Send + 'static> PersistLaneSet<T> {
    pub fn new(
        max_entries: usize,
        max_bytes: u64,
        writer: Arc<dyn PersistLaneWriter<T>>,
        graph_metrics: Option<Arc<ResidentMetrics>>,
    ) -> Self {
        Self {
            admission: RwLock::new(()),
            lanes: Mutex::new(HashMap::new()),
            max_entries: max_entries.max(1),
            max_bytes: max_bytes.max(1),
            group_commit_max: 16,
            writer,
            metrics: Arc::new(PersistLaneCounters::default()),
            graph_metrics,
            age_tracker: Arc::new(PersistLaneAgeTracker::default()),
            stopped: AtomicBool::new(false),
            breaker_cooldown: breaker_cooldown_from_env(),
        }
    }

    /// Override the base cooldown of the quarantine breaker.
    #[must_use]
    pub fn with_breaker_cooldown(mut self, cooldown: Duration) -> Self {
        self.breaker_cooldown = cooldown;
        self
    }

    /// Empty set that never spawns workers. Used by persist-surface clones
    /// that only run an already-dequeued envelope.
    pub fn disabled() -> Self {
        struct Noop;
        #[async_trait]
        impl<U: Send + 'static> PersistLaneWriter<U> for Noop {
            async fn write_batch(&self, _batch: Vec<PersistEnvelope<U>>) -> PersistBatchOutcome<U> {
                PersistBatchOutcome::Complete
            }
        }
        Self {
            admission: RwLock::new(()),
            lanes: Mutex::new(HashMap::new()),
            max_entries: 1,
            max_bytes: 1,
            group_commit_max: 1,
            writer: Arc::new(Noop),
            metrics: Arc::new(PersistLaneCounters::default()),
            graph_metrics: None,
            age_tracker: Arc::new(PersistLaneAgeTracker::default()),
            stopped: AtomicBool::new(true),
            breaker_cooldown: BREAKER_COOLDOWN_DEFAULT,
        }
    }

    pub fn metrics(&self) -> &PersistLaneCounters {
        &self.metrics
    }

    /// Sum of queued and in-flight envelopes across lanes.
    pub fn depth(&self) -> u64 {
        self.metrics.depth()
    }

    /// Age of the oldest queued or in-flight envelope.
    pub fn oldest_age(&self) -> Option<Duration> {
        let now = Instant::now();
        let lanes = self.lanes.lock().expect("persist lanes map");
        let mut oldest: Option<Instant> = None;
        for lane in lanes.values() {
            let fifo = lane.fifo.lock().expect("persist lane fifo");
            let lane_oldest = fifo_oldest_enqueued_at(&fifo);
            if let Some(front) = lane_oldest {
                oldest = Some(match oldest {
                    Some(t) if t <= front => t,
                    _ => front,
                });
            }
        }
        oldest.map(|t| now.saturating_duration_since(t))
    }

    pub fn oldest_age_ms(&self) -> u64 {
        self.oldest_age()
            .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
    }

    #[must_use]
    pub fn max_bytes(&self) -> u64 {
        self.max_bytes
    }

    /// Occupancy of every live lane. Status-path cheap: one map lock plus
    /// one fifo lock per lane.
    #[must_use]
    pub fn occupancy(&self) -> Vec<PersistLaneOccupancy> {
        let lanes = self.lanes.lock().expect("persist lanes map");
        lanes
            .values()
            .map(|lane| {
                let fifo = lane.fifo.lock().expect("persist lane fifo");
                PersistLaneOccupancy {
                    storage_prefix: lane.key.storage_prefix.clone(),
                    schema_name: lane.key.schema_name.clone(),
                    queued_bytes: fifo.bytes,
                    reserved_bytes: fifo.reserved_bytes,
                    queued_entries: fifo.queue.len() as u64,
                    reserved_entries: fifo.reserved_entries as u64,
                    unhealthy: fifo.unhealthy,
                }
            })
            .collect()
    }

    #[must_use]
    pub fn heaviest_occupancy(&self) -> Option<PersistLaneOccupancy> {
        self.occupancy()
            .into_iter()
            .max_by_key(PersistLaneOccupancy::occupied_bytes)
            .filter(|row| row.occupied_bytes() > 0)
    }

    #[must_use]
    pub fn pressure(&self) -> PersistLanePressure {
        let heaviest = self.heaviest_occupancy();
        PersistLanePressure {
            fair_share_bytes: self.max_bytes,
            write_through_threshold_bytes: crate::memory_budget::write_through_threshold_bytes(),
            heaviest_schema: heaviest
                .as_ref()
                .map_or_else(String::new, |row| row.schema_name.clone()),
            heaviest_bytes: heaviest
                .as_ref()
                .map_or(0, PersistLaneOccupancy::occupied_bytes),
            queued_entries: heaviest.as_ref().map_or(0, |row| row.queued_entries),
            reserved_entries: heaviest.as_ref().map_or(0, |row| row.reserved_entries),
            oldest_age_ms: self.oldest_age_ms(),
            refuse_bytes: self.metrics.refuse_bytes(),
            refuse_entries: self.metrics.refuse_entries(),
            refuse_unhealthy: self.metrics.refuse_unhealthy(),
            write_throughs: self.metrics.write_throughs(),
            unhealthy_lanes: self.metrics.unhealthy_lanes(),
            quarantined: self.metrics.quarantined(),
            breaker_trips: self.metrics.breaker_trips(),
        }
    }

    fn entries_full(&self, fifo: &FifoState<T>) -> bool {
        fifo.queue.len().saturating_add(fifo.reserved_entries) >= self.max_entries
    }

    fn bytes_full(&self, fifo: &FifoState<T>, extra: u64) -> bool {
        let occupied = fifo.bytes.saturating_add(fifo.reserved_bytes);
        occupied.saturating_add(extra) > self.max_bytes && occupied > 0
    }

    /// Reserve one queue entry and `bytes` before apply-gate acquisition.
    ///
    /// A failed reserve is backpressure: the caller must not apply. Drop the
    /// reservation to return the slot; [`PersistReservation::fill`] enqueues.
    pub fn reserve(
        &self,
        key: PersistLaneKey,
        bytes: u64,
    ) -> Result<PersistReservation<T>, PersistLaneFull> {
        let _admission = self.admission.read().expect("persist lane admission");
        if self.stopped.load(Ordering::Acquire) {
            return Err(PersistLaneFull::Entries);
        }
        let lane = self.lane_for(key);
        {
            let mut fifo = lane.fifo.lock().expect("persist lane fifo");
            if fifo.unhealthy {
                self.metrics.record_refuse_unhealthy();
                return Err(PersistLaneFull::Unhealthy);
            }
            if self.entries_full(&fifo) {
                self.metrics.record_refuse_entries();
                return Err(PersistLaneFull::Entries);
            }
            if self.bytes_full(&fifo, bytes) {
                self.metrics.record_refuse_bytes();
                return Err(PersistLaneFull::Bytes);
            }
            fifo.reserved_entries = fifo.reserved_entries.saturating_add(1);
            fifo.reserved_bytes = fifo.reserved_bytes.saturating_add(bytes);
        }
        Ok(PersistReservation {
            lane,
            metrics: Arc::clone(&self.metrics),
            graph_metrics: self.graph_metrics.clone(),
            age_tracker: Arc::clone(&self.age_tracker),
            bytes,
            armed: true,
        })
    }

    /// Reserve a commit id and enqueue `envelope` on `key`'s FIFO.
    ///
    /// Returns the assigned `commit_id`. A refusal returns the envelope. A
    /// caller must not bypass an unhealthy lane because later same-schema work
    /// cannot pass its retained head.
    pub fn enqueue(
        &self,
        key: PersistLaneKey,
        mut envelope: PersistEnvelope<T>,
    ) -> Result<u64, (PersistLaneFull, PersistEnvelope<T>)> {
        let admission = self.admission.read().expect("persist lane admission");
        if self.stopped.load(Ordering::Acquire) {
            return Err((PersistLaneFull::Entries, envelope));
        }
        let lane = self.lane_for(key);
        let commit_id = {
            let mut fifo = lane.fifo.lock().expect("persist lane fifo");
            if fifo.unhealthy {
                self.metrics.record_refuse_unhealthy();
                return Err((PersistLaneFull::Unhealthy, envelope));
            }
            if self.entries_full(&fifo) {
                self.metrics.record_refuse_entries();
                return Err((PersistLaneFull::Entries, envelope));
            }
            if self.bytes_full(&fifo, envelope.bytes) {
                self.metrics.record_refuse_bytes();
                return Err((PersistLaneFull::Bytes, envelope));
            }
            let commit_id = fifo.next_commit_id;
            fifo.next_commit_id = fifo.next_commit_id.saturating_add(1);
            envelope.commit_id = commit_id;
            fifo.bytes = fifo.bytes.saturating_add(envelope.bytes);
            fifo.queue.push_back(envelope);
            self.metrics.depth.fetch_add(1, Ordering::Relaxed);
            if let Some(gm) = &self.graph_metrics {
                gm.record_persist_lane_enqueue();
            }
            self.age_tracker.publish(
                &lane.key,
                fifo_oldest_enqueued_at(&fifo),
                self.graph_metrics.as_deref(),
            );
            commit_id
        };
        drop(admission);
        lane.notify.notify_one();
        Ok(commit_id)
    }

    /// Signal workers to drain and exit. Idempotent.
    pub fn stop(&self) {
        self.stopped.store(true, Ordering::Release);
        let _admission = self.admission.write().expect("persist lane admission");
        let lanes: Vec<Arc<LaneRuntime<T>>> = self
            .lanes
            .lock()
            .expect("persist lanes map")
            .values()
            .cloned()
            .collect();
        for lane in lanes {
            lane.stop.store(true, Ordering::Relaxed);
            lane.notify.notify_one();
        }
    }

    /// Stop admission and wait for every lane worker to drain.
    ///
    /// Reserved, queued, and in-flight envelopes all delay completion. A
    /// timeout leaves each join handle in its lane so [`Drop`] can abort it.
    pub async fn wait_for_drain(&self, timeout: Duration) -> bool {
        self.stop();
        let lanes: Vec<Arc<LaneRuntime<T>>> = self
            .lanes
            .lock()
            .expect("persist lanes map")
            .values()
            .cloned()
            .collect();
        tokio::time::timeout(timeout, async move {
            for lane in lanes {
                wait_for_lane_drain(&lane).await;
            }
        })
        .await
        .is_ok()
    }

    /// Wait until no lane holds reserved, queued, or in-flight work.
    ///
    /// Admission stays open. Shutdown calls this first, while other
    /// subsystems still stop, so accepted writes become durable before a
    /// supervisor kill window can expire. A lane with a retained unhealthy
    /// head never drains by itself: when every busy lane is unhealthy, this
    /// returns `false` at once instead of at `timeout`.
    pub async fn wait_for_quiescent(&self, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        loop {
            let (busy_healthy, busy_unhealthy) = {
                let lanes = self.lanes.lock().expect("persist lanes map");
                lanes
                    .values()
                    .fold((0_usize, 0_usize), |(healthy, unhealthy), lane| {
                        let fifo = lane.fifo.lock().expect("persist lane fifo");
                        let busy = !fifo.queue.is_empty()
                            || fifo.in_flight_oldest.is_some()
                            || fifo.reserved_entries > 0;
                        match (busy, fifo.unhealthy) {
                            (false, _) => (healthy, unhealthy),
                            (true, false) => (healthy + 1, unhealthy),
                            (true, true) => (healthy, unhealthy + 1),
                        }
                    })
            };
            if busy_healthy == 0 && busy_unhealthy == 0 {
                return true;
            }
            if busy_healthy == 0 {
                return false;
            }
            if Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(QUIESCENT_POLL).await;
        }
    }

    fn lane_for(&self, key: PersistLaneKey) -> Arc<LaneRuntime<T>> {
        let mut lanes = self.lanes.lock().expect("persist lanes map");
        if let Some(existing) = lanes.get(&key) {
            return Arc::clone(existing);
        }
        let runtime = Arc::new(LaneRuntime {
            key: key.clone(),
            fifo: Mutex::new(FifoState {
                queue: VecDeque::new(),
                bytes: 0,
                in_flight_oldest: None,
                reserved_entries: 0,
                reserved_bytes: 0,
                next_commit_id: 0,
                consecutive_failures: 0,
                unhealthy: false,
                poisoned_head: None,
                quarantines_since_success: 0,
                breaker_open_since: None,
                breaker_trips: 0,
            }),
            notify: Notify::new(),
            // The admission read gate already accepted this caller. A
            // concurrent stop can set the set-level flag while it waits for
            // that gate, so this new lane must still start and then drain.
            stop: AtomicBool::new(false),
            drained: AtomicBool::new(false),
            drain_notify: Notify::new(),
            join: Mutex::new(None),
        });
        let worker_lane = Arc::clone(&runtime);
        let writer = Arc::clone(&self.writer);
        let metrics = Arc::clone(&self.metrics);
        let graph_metrics = self.graph_metrics.clone();
        let age_tracker = Arc::clone(&self.age_tracker);
        let group_commit_max = self.group_commit_max;
        let breaker_cooldown = self.breaker_cooldown;
        // lint:spawn-bare-ok process-lifetime persist lane — not request-scoped.
        let handle = tokio::spawn(async move {
            run_lane_worker(
                worker_lane,
                writer,
                metrics,
                graph_metrics,
                age_tracker,
                group_commit_max,
                breaker_cooldown,
            )
            .await;
        });
        *runtime.join.lock().expect("persist lane join") = Some(handle);
        lanes.insert(key, Arc::clone(&runtime));
        runtime
    }
}

impl<T: Send + 'static> PersistReservation<T> {
    /// Convert this granted slot into a queued envelope and wake the worker.
    ///
    /// The reservation grants admission before resident apply. Thus, this
    /// operation cannot refuse after a later lane failure. The reserved byte
    /// charge replaces the envelope estimate, so accounting cannot shrink at
    /// the reserve-to-fill boundary.
    pub fn fill(mut self, mut envelope: PersistEnvelope<T>) -> u64 {
        let commit_id = {
            let mut fifo = self.lane.fifo.lock().expect("persist lane fifo");
            if self.armed {
                fifo.reserved_entries = fifo.reserved_entries.saturating_sub(1);
                fifo.reserved_bytes = fifo.reserved_bytes.saturating_sub(self.bytes);
                self.armed = false;
            }
            envelope.bytes = self.bytes;
            let commit_id = fifo.next_commit_id;
            fifo.next_commit_id = fifo.next_commit_id.saturating_add(1);
            envelope.commit_id = commit_id;
            fifo.bytes = fifo.bytes.saturating_add(envelope.bytes);
            fifo.queue.push_back(envelope);
            self.metrics.depth.fetch_add(1, Ordering::Relaxed);
            if let Some(gm) = &self.graph_metrics {
                gm.record_persist_lane_enqueue();
            }
            self.age_tracker.publish(
                &self.lane.key,
                fifo_oldest_enqueued_at(&fifo),
                self.graph_metrics.as_deref(),
            );
            commit_id
        };
        self.lane.notify.notify_one();
        commit_id
    }
}

impl<T: Send + 'static> Drop for PersistReservation<T> {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let mut fifo = self.lane.fifo.lock().expect("persist lane fifo");
        fifo.reserved_entries = fifo.reserved_entries.saturating_sub(1);
        fifo.reserved_bytes = fifo.reserved_bytes.saturating_sub(self.bytes);
        self.armed = false;
        self.lane.notify.notify_one();
    }
}

impl<T: Send + 'static> Drop for PersistLaneSet<T> {
    fn drop(&mut self) {
        self.stop();
        let lanes: Vec<Arc<LaneRuntime<T>>> = self
            .lanes
            .lock()
            .expect("persist lanes map")
            .values()
            .cloned()
            .collect();
        for lane in lanes {
            if let Some(handle) = lane.join.lock().expect("persist lane join").take() {
                handle.abort();
            }
            self.age_tracker
                .publish(&lane.key, None, self.graph_metrics.as_deref());
        }
    }
}
