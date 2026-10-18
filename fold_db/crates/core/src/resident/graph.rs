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
    DirtyKey, ResidentAtom, ResidentKeySetCompleteness, ResidentKeySetSnapshot, ResidentKind,
    ResidentMoleculeKey, ResidentSlotControl, ResidentSlotState, ResidentTip, ResolveOutcome,
};
use crate::schema::types::Schema;

#[path = "coverage.rs"]
mod coverage;
#[path = "page_coverage.rs"]
mod page_coverage;
#[allow(dead_code)]
#[path = "slot_reads.rs"]
mod slot_reads;
pub(crate) use slot_reads::OwnedSlotRead;

#[path = "overlay_page.rs"]
mod overlay_page;
pub(crate) use overlay_page::ResidentOverlayEntry;

mod budget;
mod dirty;
mod keys;
mod persist;
mod purge;
mod schemas;
mod slots;
mod tip_map;
mod tips;

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
#[derive(Debug)]
pub struct ResidentGraph {
    schemas: RwLock<HashMap<String, Schema>>,
    tips: RwLock<tip_map::TipMap>,
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
    /// Per-slot revision + Ready flag. Missing == Absent.
    slots: RwLock<HashMap<String, ResidentSlotControl>>,
    /// Process-unique revision identities never repeat after slot reclamation.
    next_slot_revision: std::sync::atomic::AtomicU64,
    /// Wakes persist envelopes after an exact predecessor revision completes.
    persist_turn_changed: Notify,
    slot_readers: Mutex<HashMap<String, usize>>,
}

impl Default for ResidentGraph {
    fn default() -> Self {
        Self::new()
    }
}

impl ResidentGraph {
    pub fn new() -> Self {
        Self {
            schemas: RwLock::new(HashMap::new()),
            tips: RwLock::new(tip_map::TipMap::default()),
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
            next_slot_revision: std::sync::atomic::AtomicU64::new(1),
            persist_turn_changed: Notify::new(),
            slot_readers: Mutex::new(HashMap::new()),
        }
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
