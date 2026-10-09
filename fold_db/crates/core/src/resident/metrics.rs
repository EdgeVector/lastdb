//! Resident hit / rehydrate / persist counters (by ladder kind).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::Instant;

use super::types::ResidentKind;

/// Process-local counters for the resident graph.
#[derive(Debug, Default)]
pub struct ResidentMetrics {
    schema_hit: AtomicU64,
    schema_rehydrate: AtomicU64,
    molecule_hit: AtomicU64,
    molecule_rehydrate: AtomicU64,
    atom_hit: AtomicU64,
    atom_rehydrate: AtomicU64,
    file_blob_hit: AtomicU64,
    file_blob_rehydrate: AtomicU64,
    protein_hit: AtomicU64,
    protein_rehydrate: AtomicU64,
    persist_enqueued: AtomicU64,
    persist_flushed: AtomicU64,
    /// Deferred durable persists (mode=write background tasks) whose store I/O
    /// failed; the batch's keys stay dirty for the persist worker to retry.
    deferred_persist_failed: AtomicU64,
    /// Deferred durable persists (mode=write background tasks) that finished,
    /// success or failure. Denominator for `deferred_persist_us` — an average
    /// needs both.
    deferred_persist_completed: AtomicU64,
    /// Cumulative wall-clock microseconds spent inside the deferred durable
    /// persist task (atom store + molecule/schema store + sibling tips),
    /// success or failure. This is the durable-store cost the mutation's own
    /// request phases cannot see: the task runs after the client is already
    /// acked and off the request's task-local scope (`tokio::spawn` does not
    /// inherit task-locals — see `crate::request_phases`), so without this
    /// counter it is readable only via `tracing::debug!` timing that never
    /// fires on a production node.
    deferred_persist_us: AtomicU64,
    /// Clean entries dropped by budget enforcement (all kinds).
    evicted: AtomicU64,
    /// Budget passes that could not get under budget because the overage was
    /// entirely dirty (persist worker owes a drain).
    evict_refused_dirty: AtomicU64,
    /// Complete resident key-set enumeration hits.
    key_set_hit: AtomicU64,
    key_set_marked_complete: AtomicU64,
    /// Resident key-set reads that supplied a `PartialOverlay` — resident held
    /// some members and the read path applied them over the durable walk. Not
    /// a hit, but not wasted work either.
    key_set_overlay: AtomicU64,
    /// Resident key-set reads where resident held no claim at all (`Unknown`).
    /// This is the counter that means "nothing to serve"; a hit rate of zero
    /// against a large `unknown` is a missing producer, not a cold cache.
    key_set_unknown: AtomicU64,
    /// Complete key sets demoted because resident could no longer prove every
    /// member was present after memory-pressure eviction.
    key_set_demote: AtomicU64,
    /// Disk rehydrate skipped because a newer resident apply won the slot.
    stale_rehydrate_rejected: AtomicU64,
    /// Current FIFO depth across all schema persist lanes.
    persist_lane_depth: AtomicU64,
    /// Compatibility value for callers that publish a measured age directly.
    persist_lane_oldest_age_ms: AtomicU64,
    /// Enqueue time of the oldest queued or in-flight persist envelope.
    ///
    /// The snapshot derives age from this timestamp so a stalled writer's age
    /// continues to advance without a queue transition.
    persist_lane_oldest_enqueued_at: Mutex<Option<Instant>>,
    /// Persist-lane write failures (retry retained; dirty stays set).
    persist_lane_failures: AtomicU64,
    /// Reserved field for a future aggregate `resident_revision -
    /// durable_revision` lag gauge. The value stays at zero.
    resident_minus_durable_revision: AtomicU64,
    /// Used logical records in the logical resident set (schema, field, tip,
    /// atom). Instant occupancy. Decision
    /// `decision-2026-10-02-warm-set-exact-logical-key-budget`.
    resident_key_count: AtomicU64,
    /// Used records a call still holds.
    resident_held_keys: AtomicU64,
    /// Used records with an uncovered durability token.
    resident_dirty_keys: AtomicU64,
    /// LRU purge removals of used records since process start.
    resident_purged_keys: AtomicU64,
    /// Bytes in the loader pin table. Measurement, not a cap.
    loader_pin_bytes: AtomicU64,
    /// Hash groups the loader has open right now. Measurement, not a cap.
    loader_groups_open_now: AtomicU64,
    /// Point reads the logical set served from memory.
    resident_point_hits: AtomicU64,
    /// Point reads that went to the loader because the set held no record.
    resident_point_misses: AtomicU64,
    /// Purge passes that removed at least one used record.
    resident_purge_runs: AtomicU64,
    /// Purge passes that ended over the budget because every remaining
    /// record was held or dirty.
    resident_over_cap_stalls: AtomicU64,
    /// Used records above the budget right now. Zero when within budget.
    resident_over_cap_keys: AtomicU64,
    /// Loader point loads (one hash-group open each) since process start.
    loader_loads: AtomicU64,
    /// Cumulative wall-clock microseconds inside those loads. Divide by
    /// `loader_loads` for the mean.
    loader_load_us: AtomicU64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResidentMetricsSnapshot {
    pub schema_hit: u64,
    pub schema_rehydrate: u64,
    pub molecule_hit: u64,
    pub molecule_rehydrate: u64,
    pub atom_hit: u64,
    pub atom_rehydrate: u64,
    pub file_blob_hit: u64,
    pub file_blob_rehydrate: u64,
    pub protein_hit: u64,
    pub protein_rehydrate: u64,
    pub persist_enqueued: u64,
    pub persist_flushed: u64,
    pub deferred_persist_failed: u64,
    pub deferred_persist_completed: u64,
    pub deferred_persist_us: u64,
    pub evicted: u64,
    pub evict_refused_dirty: u64,
    pub key_set_hit: u64,
    pub key_set_marked_complete: u64,
    /// Retained as `overlay + unknown` so existing consumers keep reading the
    /// same total. Prefer the two components — they answer different questions.
    pub key_set_miss: u64,
    pub key_set_overlay: u64,
    pub key_set_unknown: u64,
    pub key_set_demote: u64,
    pub stale_rehydrate_rejected: u64,
    pub persist_lane_depth: u64,
    pub persist_lane_oldest_age_ms: u64,
    pub persist_lane_failures: u64,
    pub resident_minus_durable_revision: u64,
    /// Used logical records currently in the set.
    pub resident_key_count: u64,
    /// Used records a call still holds.
    pub resident_held_keys: u64,
    /// Used records with an uncovered durability token.
    pub resident_dirty_keys: u64,
    /// Used-record budget. Always [`super::RESIDENT_KEY_CAP`].
    pub resident_key_budget: u64,
    /// LRU purge removals of used records since process start.
    pub resident_purged_keys: u64,
    /// Bytes in the loader pin table. Measurement, not a cap.
    pub loader_pin_bytes: u64,
    /// Hash groups the loader has open right now. Measurement, not a cap.
    pub loader_groups_open_now: u64,
    /// Point reads served from the logical set.
    pub resident_point_hits: u64,
    /// Point reads that went to the loader.
    pub resident_point_misses: u64,
    /// Purge passes that removed at least one used record.
    pub resident_purge_runs: u64,
    /// Purge passes that ended over the budget (all remaining held or dirty).
    pub resident_over_cap_stalls: u64,
    /// Used records above the budget right now.
    pub resident_over_cap_keys: u64,
    /// Loader point loads since process start.
    pub loader_loads: u64,
    /// Cumulative microseconds inside loader point loads.
    pub loader_load_us: u64,
}

impl ResidentMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_hit(&self, kind: ResidentKind) {
        match kind.canonical() {
            ResidentKind::Schema => {
                self.schema_hit.fetch_add(1, Ordering::Relaxed);
            }
            ResidentKind::Molecule | ResidentKind::MoleculeTip => {
                self.molecule_hit.fetch_add(1, Ordering::Relaxed);
            }
            ResidentKind::Atom => {
                self.atom_hit.fetch_add(1, Ordering::Relaxed);
            }
            ResidentKind::FileBlob => {
                self.file_blob_hit.fetch_add(1, Ordering::Relaxed);
            }
            ResidentKind::Protein => {
                self.protein_hit.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    pub fn record_rehydrate(&self, kind: ResidentKind) {
        match kind.canonical() {
            ResidentKind::Schema => {
                self.schema_rehydrate.fetch_add(1, Ordering::Relaxed);
            }
            ResidentKind::Molecule | ResidentKind::MoleculeTip => {
                self.molecule_rehydrate.fetch_add(1, Ordering::Relaxed);
            }
            ResidentKind::Atom => {
                self.atom_rehydrate.fetch_add(1, Ordering::Relaxed);
            }
            ResidentKind::FileBlob => {
                self.file_blob_rehydrate.fetch_add(1, Ordering::Relaxed);
            }
            ResidentKind::Protein => {
                self.protein_rehydrate.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    pub fn record_persist_enqueued(&self) {
        self.persist_enqueued.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_persist_flushed(&self) {
        self.persist_flushed.fetch_add(1, Ordering::Relaxed);
    }

    /// A deferred durable persist (mode=write background task) failed its
    /// store I/O — counted once per failed batch; keys stay dirty for retry.
    pub fn record_deferred_persist_failure(&self) {
        self.deferred_persist_failed.fetch_add(1, Ordering::Relaxed);
    }

    /// Record the wall-clock cost of one deferred durable persist task, on
    /// EITHER outcome — the failure counter above already distinguishes
    /// success from failure, so this is purely "how long did the durable
    /// store side take", not a success signal.
    pub fn record_deferred_persist_duration(&self, elapsed: std::time::Duration) {
        self.deferred_persist_completed
            .fetch_add(1, Ordering::Relaxed);
        self.deferred_persist_us.fetch_add(
            u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
    }

    /// A clean entry was dropped by budget enforcement. `kind` is accepted for
    /// forward compatibility with per-kind buckets; today one counter covers
    /// all kinds.
    pub fn record_evicted(&self, kind: ResidentKind) {
        let _ = kind;
        self.evicted.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_evict_refused_dirty(&self) {
        self.evict_refused_dirty.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_key_set_complete(&self) {
        self.key_set_marked_complete.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_key_set_hit(&self) {
        self.key_set_hit.fetch_add(1, Ordering::Relaxed);
    }

    /// A read that got an overlay: resident held members, the durable walk
    /// still had to run, and the overlay was applied on top of it.
    pub fn record_key_set_overlay(&self) {
        self.key_set_overlay.fetch_add(1, Ordering::Relaxed);
    }

    /// A read that got nothing: resident had no claim on this molecule's key
    /// set.
    pub fn record_key_set_unknown(&self) {
        self.key_set_unknown.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_key_set_demote(&self) {
        self.key_set_demote.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_stale_rehydrate_rejected(&self) {
        self.stale_rehydrate_rejected
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_persist_lane_enqueue(&self) {
        self.persist_lane_depth.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_persist_lane_dequeue(&self, n: u64) {
        self.persist_lane_depth.fetch_sub(n, Ordering::Relaxed);
    }

    pub(crate) fn set_persist_lane_oldest_enqueued_at(&self, enqueued_at: Option<Instant>) {
        *self
            .persist_lane_oldest_enqueued_at
            .lock()
            .expect("persist lane oldest timestamp") = enqueued_at;
        self.persist_lane_oldest_age_ms.store(0, Ordering::Relaxed);
    }

    pub fn record_persist_lane_failure(&self) {
        self.persist_lane_failures.fetch_add(1, Ordering::Relaxed);
    }

    /// Used-record budget in force for the logical resident set. A proof
    /// reads this gauge to show which cap it ran under.
    pub fn resident_key_budget() -> u64 {
        super::resident_key_cap() as u64
    }

    /// Instant occupancy of used records, held records, and dirty records.
    pub fn set_logical_occupancy(&self, key_count: u64, held_keys: u64, dirty_keys: u64) {
        self.resident_key_count.store(key_count, Ordering::Relaxed);
        self.resident_held_keys.store(held_keys, Ordering::Relaxed);
        self.resident_dirty_keys
            .store(dirty_keys, Ordering::Relaxed);
    }

    /// Count used records the LRU purge removed.
    pub fn record_purged_keys(&self, n: u64) {
        self.resident_purged_keys.fetch_add(n, Ordering::Relaxed);
    }

    /// Instant loader pin bytes and open-group count. These are measurements.
    pub fn set_loader_measurements(&self, pin_bytes: u64, groups_open_now: u64) {
        self.loader_pin_bytes.store(pin_bytes, Ordering::Relaxed);
        self.loader_groups_open_now
            .store(groups_open_now, Ordering::Relaxed);
    }

    /// A point read the logical set served from memory.
    pub fn record_point_hit(&self) {
        self.resident_point_hits.fetch_add(1, Ordering::Relaxed);
    }

    /// A point read the logical set could not serve; the loader ran.
    pub fn record_point_miss(&self) {
        self.resident_point_misses.fetch_add(1, Ordering::Relaxed);
    }

    /// One purge pass finished. `removed` is the records it dropped, `over`
    /// is the used-record count above the budget afterwards (zero when the
    /// pass reached the budget).
    pub fn record_purge_pass(&self, removed: u64, over: u64) {
        if removed > 0 {
            self.resident_purge_runs.fetch_add(1, Ordering::Relaxed);
        }
        if over > 0 {
            self.resident_over_cap_stalls
                .fetch_add(1, Ordering::Relaxed);
        }
        self.resident_over_cap_keys.store(over, Ordering::Relaxed);
    }

    /// Used records above the budget right now, published outside a purge
    /// pass (admit and release change the count too).
    pub fn set_over_cap_keys(&self, over: u64) {
        self.resident_over_cap_keys.store(over, Ordering::Relaxed);
    }

    /// One loader point load finished, `elapsed` wall clock.
    pub fn record_loader_load(&self, elapsed: std::time::Duration) {
        self.loader_loads.fetch_add(1, Ordering::Relaxed);
        self.loader_load_us.fetch_add(
            u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
    }

    pub fn snapshot(&self) -> ResidentMetricsSnapshot {
        let persist_lane_oldest_age_ms = self
            .persist_lane_oldest_enqueued_at
            .lock()
            .expect("persist lane oldest timestamp")
            .map_or_else(
                || self.persist_lane_oldest_age_ms.load(Ordering::Relaxed),
                |enqueued_at| u64::try_from(enqueued_at.elapsed().as_millis()).unwrap_or(u64::MAX),
            );
        ResidentMetricsSnapshot {
            schema_hit: self.schema_hit.load(Ordering::Relaxed),
            schema_rehydrate: self.schema_rehydrate.load(Ordering::Relaxed),
            molecule_hit: self.molecule_hit.load(Ordering::Relaxed),
            molecule_rehydrate: self.molecule_rehydrate.load(Ordering::Relaxed),
            atom_hit: self.atom_hit.load(Ordering::Relaxed),
            atom_rehydrate: self.atom_rehydrate.load(Ordering::Relaxed),
            file_blob_hit: self.file_blob_hit.load(Ordering::Relaxed),
            file_blob_rehydrate: self.file_blob_rehydrate.load(Ordering::Relaxed),
            protein_hit: self.protein_hit.load(Ordering::Relaxed),
            protein_rehydrate: self.protein_rehydrate.load(Ordering::Relaxed),
            persist_enqueued: self.persist_enqueued.load(Ordering::Relaxed),
            persist_flushed: self.persist_flushed.load(Ordering::Relaxed),
            deferred_persist_failed: self.deferred_persist_failed.load(Ordering::Relaxed),
            deferred_persist_completed: self.deferred_persist_completed.load(Ordering::Relaxed),
            deferred_persist_us: self.deferred_persist_us.load(Ordering::Relaxed),
            evicted: self.evicted.load(Ordering::Relaxed),
            evict_refused_dirty: self.evict_refused_dirty.load(Ordering::Relaxed),
            key_set_hit: self.key_set_hit.load(Ordering::Relaxed),
            key_set_marked_complete: self.key_set_marked_complete.load(Ordering::Relaxed),
            // Derived rather than counted, so the total can never drift from
            // the two components that make it up.
            key_set_miss: self.key_set_overlay.load(Ordering::Relaxed)
                + self.key_set_unknown.load(Ordering::Relaxed),
            key_set_overlay: self.key_set_overlay.load(Ordering::Relaxed),
            key_set_unknown: self.key_set_unknown.load(Ordering::Relaxed),
            key_set_demote: self.key_set_demote.load(Ordering::Relaxed),
            stale_rehydrate_rejected: self.stale_rehydrate_rejected.load(Ordering::Relaxed),
            persist_lane_depth: self.persist_lane_depth.load(Ordering::Relaxed),
            persist_lane_oldest_age_ms,
            persist_lane_failures: self.persist_lane_failures.load(Ordering::Relaxed),
            resident_minus_durable_revision: self
                .resident_minus_durable_revision
                .load(Ordering::Relaxed),
            resident_key_count: self.resident_key_count.load(Ordering::Relaxed),
            resident_held_keys: self.resident_held_keys.load(Ordering::Relaxed),
            resident_dirty_keys: self.resident_dirty_keys.load(Ordering::Relaxed),
            resident_key_budget: Self::resident_key_budget(),
            resident_purged_keys: self.resident_purged_keys.load(Ordering::Relaxed),
            loader_pin_bytes: self.loader_pin_bytes.load(Ordering::Relaxed),
            loader_groups_open_now: self.loader_groups_open_now.load(Ordering::Relaxed),
            resident_point_hits: self.resident_point_hits.load(Ordering::Relaxed),
            resident_point_misses: self.resident_point_misses.load(Ordering::Relaxed),
            resident_purge_runs: self.resident_purge_runs.load(Ordering::Relaxed),
            resident_over_cap_stalls: self.resident_over_cap_stalls.load(Ordering::Relaxed),
            resident_over_cap_keys: self.resident_over_cap_keys.load(Ordering::Relaxed),
            loader_loads: self.loader_loads.load(Ordering::Relaxed),
            loader_load_us: self.loader_load_us.load(Ordering::Relaxed),
        }
    }
}
