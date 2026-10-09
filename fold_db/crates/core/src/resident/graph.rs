//! ResidentGraph — full ladder primary store (schema → mol tip → atom).

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::ops::Bound;
use std::sync::{Arc, Mutex, RwLock};

use tokio::sync::Notify;

use super::config::ResidentPolicy;
use super::lane::PersistSlotRevision;
use super::ledger::ResidentLedger;
use super::metrics::ResidentMetrics;
use super::types::{
    DirtyKey, PersistPlan, ResidentAtom, ResidentKeySetCompleteness, ResidentKeySetSnapshot,
    ResidentKind, ResidentMoleculeKey, ResidentSlotControl, ResidentSlotId, ResidentSlotState,
    ResidentTip, ResolveOutcome, APPROX_SCHEMA_BYTES,
};
use crate::schema::types::{Schema, SchemaError};

#[path = "coverage.rs"]
mod coverage;
#[path = "page_coverage.rs"]
mod page_coverage;
#[path = "slot_reads.rs"]
mod slot_reads;
pub(crate) use slot_reads::SlotRead;
#[path = "overlay_page.rs"]
mod overlay_page;

mod budget;
mod dirty;
mod keys;
mod persist;
mod purge;
mod schemas;
mod slots;
mod tips;

/// Shared cold-rehydrate flight. The disk read does not hold the flight map
/// mutex; waiters park on [`Notify`].
pub(crate) struct RehydrateFlightCell {
    result: Mutex<Option<Result<Option<ResidentTip>, SchemaError>>>,
    notify: Notify,
}

impl std::fmt::Debug for RehydrateFlightCell {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RehydrateFlightCell")
            .finish_non_exhaustive()
    }
}

impl RehydrateFlightCell {
    fn new() -> Self {
        Self {
            result: Mutex::new(None),
            notify: Notify::new(),
        }
    }

    pub(crate) fn complete(&self, outcome: Result<Option<ResidentTip>, SchemaError>) {
        let mut slot = self.result.lock().expect("rehydrate flight result");
        if slot.is_none() {
            *slot = Some(outcome);
        }
        drop(slot);
        self.notify.notify_waiters();
    }

    pub(crate) async fn wait(&self) -> Result<Option<ResidentTip>, SchemaError> {
        loop {
            let notified = self.notify.notified();
            {
                let slot = self.result.lock().expect("rehydrate flight result");
                if let Some(result) = slot.as_ref() {
                    return result.clone();
                }
            }
            notified.await;
        }
    }
}

/// Loader used on schema rehydrate miss (typically SchemaCore / db_ops).
pub trait SchemaLoader: Send + Sync {
    fn load_schema(&self, name: &str) -> Result<Option<Schema>, String>;
}

fn tip_map_key(molecule_uuid: &str, hash: &str, range: &str) -> String {
    format!("{molecule_uuid}\0{hash}\0{range}")
}

/// T0 resident graph — isomorphic to durable ladder once drained.
///
/// Bounded: every resident entry is charged (approximately) against
/// `budget_bytes`; going over evicts **clean** entries LRU-first. Dirty
/// entries are never evicted — the budget can be exceeded by dirty state
/// until the persist worker drains it (counted, not silent).
#[derive(Debug, Default)]
pub struct ResidentGraph {
    schemas: RwLock<HashMap<String, Schema>>,
    tips: RwLock<HashMap<String, ResidentTip>>,
    key_index: RwLock<BTreeMap<String, BTreeSet<ResidentMoleculeKey>>>,
    key_tombstones: RwLock<BTreeMap<String, BTreeMap<ResidentMoleculeKey, u64>>>,
    /// Schema-name × API (hash, range) overlay for Skip Deletes acked before
    /// the persist-lane hard erase. Query filters these keys so a read after
    /// ack does not wait on `purge_plan`.
    schema_key_tombstones: RwLock<HashMap<(String, String, String), u64>>,
    next_tombstone_id: std::sync::atomic::AtomicU64,
    key_completeness: RwLock<HashMap<String, ResidentKeySetCompleteness>>,
    partition_coverage: Mutex<coverage::CoverageRegistry>,
    page_coverage: Mutex<page_coverage::PageRegistry>,
    next_page_id: std::sync::atomic::AtomicU64,
    atoms: RwLock<HashMap<String, ResidentAtom>>,
    dirty: RwLock<HashSet<DirtyKey>>,
    metrics: Arc<ResidentMetrics>,
    /// Byte budget; `0` means unbounded (tests / explicit opt-out).
    budget_bytes: u64,
    ledger: Mutex<ResidentLedger>,
    /// Per-slot revision + Ready/RehydrateFlight flag. Missing == Absent.
    slots: RwLock<HashMap<String, ResidentSlotControl>>,
    /// Wakes persist envelopes after an exact predecessor revision completes.
    persist_turn_changed: Notify,
    /// Single-flight cells for cold rehydrate. The disk read does **not**
    /// hold this map's mutex — waiters share the cell.
    rehydrate_flights: Mutex<HashMap<String, Arc<RehydrateFlightCell>>>,
    slot_readers: Mutex<HashMap<String, usize>>,
    /// LastStore prefix this graph is bound to (empty = default / tests).
    storage_prefix: String,
}

impl ResidentGraph {
    pub fn new() -> Self {
        Self {
            schemas: RwLock::new(HashMap::new()),
            tips: RwLock::new(HashMap::new()),
            key_index: RwLock::new(BTreeMap::new()),
            key_tombstones: RwLock::new(BTreeMap::new()),
            schema_key_tombstones: RwLock::new(HashMap::new()),
            next_tombstone_id: std::sync::atomic::AtomicU64::new(1),
            key_completeness: RwLock::new(HashMap::new()),
            partition_coverage: Mutex::new(BTreeMap::new()),
            page_coverage: Mutex::new(BTreeMap::new()),
            next_page_id: std::sync::atomic::AtomicU64::new(0),
            atoms: RwLock::new(HashMap::new()),
            dirty: RwLock::new(HashSet::new()),
            metrics: Arc::new(ResidentMetrics::new()),
            budget_bytes: ResidentPolicy::from_env().budget_bytes,
            ledger: Mutex::new(ResidentLedger::default()),
            slots: RwLock::new(HashMap::new()),
            persist_turn_changed: Notify::new(),
            rehydrate_flights: Mutex::new(HashMap::new()),
            slot_readers: Mutex::new(HashMap::new()),
            storage_prefix: String::new(),
        }
    }

    /// Override the env-resolved byte budget (`0` = unbounded).
    pub fn with_budget_bytes(mut self, budget_bytes: u64) -> Self {
        self.budget_bytes = budget_bytes;
        self
    }

    pub fn metrics(&self) -> &Arc<ResidentMetrics> {
        &self.metrics
    }

    /// Share process gauges with the logical resident set and loader pin table.
    #[must_use]
    pub fn with_metrics(mut self, metrics: Arc<ResidentMetrics>) -> Self {
        self.metrics = metrics;
        self
    }

    pub fn budget_bytes(&self) -> u64 {
        self.budget_bytes
    }

    /// Approximate bytes currently charged to resident entries.
    pub fn resident_bytes(&self) -> u64 {
        self.ledger
            .lock()
            .expect("resident ledger lock")
            .total_bytes()
    }

    /// Number of charged resident entries.
    pub fn resident_entries(&self) -> usize {
        self.ledger.lock().expect("resident ledger lock").len()
    }

    // ── Budget enforcement ──────────────────────────────────────────────
}

/// Test / stub loader that returns from an in-memory map and counts loads.
#[derive(Debug, Default)]
pub struct MapSchemaLoader {
    pub schemas: HashMap<String, Schema>,
    pub load_count: std::sync::atomic::AtomicU64,
}

impl MapSchemaLoader {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, schema: Schema) {
        self.schemas.insert(schema.name.clone(), schema);
    }

    pub fn load_count(&self) -> u64 {
        self.load_count.load(std::sync::atomic::Ordering::Relaxed)
    }
}

impl SchemaLoader for MapSchemaLoader {
    fn load_schema(&self, name: &str) -> Result<Option<Schema>, String> {
        self.load_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(self.schemas.get(name).cloned())
    }
}
