//! Incremental keep-small meters: live budget, per-schema churn, atom histogram.
//!
//! These are write-path totals (point read of a small in-process projection),
//! not another store walk. `lastdb db inventory` is the wrong daily tool.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Mutex,
    },
};

/// Steady-state budget for live logical DB planes (file blobs exempt).
pub const LIVE_BUDGET_BYTES: u64 = 1 << 30;

/// 16 KiB fence-visible bucket.
pub const ATOM_HISTOGRAM_16KIB: u64 = 16 * 1024;
/// 32 KiB fence-visible bucket.
pub const ATOM_HISTOGRAM_32KIB: u64 = 32 * 1024;
/// Product atom content fence (default / restored).
pub const ATOM_HISTOGRAM_64KIB: u64 = 64 * 1024;

/// Version of the persisted meter trust payload.
pub const KEEP_SMALL_TRUST_VERSION: u32 = 1;

/// The evidence state of one persisted meter domain.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MeterTrustState {
    Absent,
    Incomplete,
    Reconciled,
    TrustedCheckpoint,
}

impl MeterTrustState {
    #[must_use]
    pub const fn is_complete(self) -> bool {
        matches!(self, Self::Reconciled | Self::TrustedCheckpoint)
    }
}

/// Trust for one counter domain, with a stable machine-readable cause.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MeterDomainTrust {
    pub state: MeterTrustState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cause: Option<String>,
}

impl MeterDomainTrust {
    #[must_use]
    pub fn absent() -> Self {
        Self {
            state: MeterTrustState::Absent,
            cause: None,
        }
    }

    #[must_use]
    pub fn incomplete(cause: impl Into<String>) -> Self {
        Self {
            state: MeterTrustState::Incomplete,
            cause: Some(cause.into()),
        }
    }

    #[must_use]
    pub fn trusted() -> Self {
        Self {
            state: MeterTrustState::TrustedCheckpoint,
            cause: None,
        }
    }
}

/// Versioned trust for global, schema, and molecule counter domains.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MeterTrustPayload {
    pub version: u32,
    pub global: MeterDomainTrust,
    #[serde(default)]
    pub schemas: BTreeMap<String, MeterDomainTrust>,
    pub molecules: MeterDomainTrust,
}

impl MeterTrustPayload {
    #[must_use]
    pub fn absent() -> Self {
        Self {
            version: KEEP_SMALL_TRUST_VERSION,
            global: MeterDomainTrust::absent(),
            schemas: BTreeMap::new(),
            molecules: MeterDomainTrust::absent(),
        }
    }

    #[must_use]
    pub fn trusted() -> Self {
        Self {
            version: KEEP_SMALL_TRUST_VERSION,
            global: MeterDomainTrust::trusted(),
            schemas: BTreeMap::new(),
            molecules: MeterDomainTrust::trusted(),
        }
    }

    #[must_use]
    pub fn legacy_incomplete() -> Self {
        Self {
            version: KEEP_SMALL_TRUST_VERSION,
            global: MeterDomainTrust::incomplete("legacy_snapshot_without_trust"),
            schemas: BTreeMap::new(),
            molecules: MeterDomainTrust::incomplete("legacy_snapshot_without_trust"),
        }
    }

    pub fn mark_incomplete(&mut self, cause: &str) {
        self.version = KEEP_SMALL_TRUST_VERSION;
        self.global = MeterDomainTrust::incomplete(cause);
        self.molecules = MeterDomainTrust::incomplete(cause);
        for domain in self.schemas.values_mut() {
            *domain = MeterDomainTrust::incomplete(cause);
        }
    }
}

impl Default for MeterTrustPayload {
    fn default() -> Self {
        Self::trusted()
    }
}

fn legacy_trust_payload() -> MeterTrustPayload {
    MeterTrustPayload::legacy_incomplete()
}

/// Incremental live totals for the daily speedometer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct LiveBudgetTotals {
    pub atom_bytes: u64,
    pub tip_bytes: u64,
    pub bookkeeping_bytes: u64,
    pub atom_count: u64,
    pub tip_count: u64,
}

impl LiveBudgetTotals {
    #[must_use]
    pub fn live_total_bytes(&self) -> u64 {
        self.atom_bytes
            .saturating_add(self.tip_bytes)
            .saturating_add(self.bookkeeping_bytes)
    }
}

/// Cheap budget report vs the 1 GiB target.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BudgetReport {
    pub measured_at: DateTime<Utc>,
    /// Always false: this is a point read of write-path totals, not a scan.
    pub heavy: bool,
    pub budget_bytes: u64,
    pub live_atom_bytes: u64,
    pub live_tip_bytes: u64,
    pub live_bookkeeping_bytes: u64,
    pub live_total_bytes: u64,
    pub live_atom_count: u64,
    pub live_tip_count: u64,
    /// Physical plane bytes the budget cares about (atoms + tips + indexes +
    /// order-log + locators + proteins). File blobs (`cas_blobs`) are exempt.
    pub physical_bytes: u64,
    /// `physical_bytes / live_total_bytes` (1.0 when live is zero).
    pub amplification: f64,
    /// `live_total_bytes / budget_bytes`.
    pub used_ratio: f64,
}

impl BudgetReport {
    #[must_use]
    pub fn from_totals(totals: LiveBudgetTotals, physical_bytes: u64) -> Self {
        let live_total_bytes = totals.live_total_bytes();
        Self {
            measured_at: Utc::now(),
            heavy: false,
            budget_bytes: LIVE_BUDGET_BYTES,
            live_atom_bytes: totals.atom_bytes,
            live_tip_bytes: totals.tip_bytes,
            live_bookkeeping_bytes: totals.bookkeeping_bytes,
            live_total_bytes,
            live_atom_count: totals.atom_count,
            live_tip_count: totals.tip_count,
            physical_bytes,
            amplification: amplification_ratio(physical_bytes, live_total_bytes),
            used_ratio: if LIVE_BUDGET_BYTES == 0 {
                0.0
            } else {
                live_total_bytes as f64 / LIVE_BUDGET_BYTES as f64
            },
        }
    }
}

/// Physical / live amplification. Live 0 → 1.0 so a fresh home is not Inf.
#[must_use]
pub fn amplification_ratio(physical_bytes: u64, live_bytes: u64) -> f64 {
    if live_bytes == 0 {
        1.0
    } else {
        physical_bytes as f64 / live_bytes as f64
    }
}

/// Per-schema incremental meter.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaMeter {
    pub schema_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    pub live_bytes: u64,
    pub atom_count: u64,
    /// Bytes appended on [`Self::appended_day`] (UTC).
    pub appended_bytes: u64,
    /// UTC calendar day `YYYY-MM-DD` the append counter is for.
    pub appended_day: String,
    /// Tip bytes traceable to this schema's molecules.
    ///
    /// Physical bookkeeping, not atom payload. It is kept out of
    /// [`Self::live_bytes`] on purpose: churn is a payload ratio, and folding
    /// index overhead into it would move the ratio for a reason the operator
    /// did not write. `serde(default)` so a home written before this field
    /// hydrates at 0 instead of failing to load.
    #[serde(default)]
    pub tip_bytes: u64,
    /// Order-log / header / secondary-index bytes traceable to this schema's
    /// molecules. Same accounting rule as [`Self::tip_bytes`].
    #[serde(default)]
    pub bookkeeping_bytes: u64,
}

/// The current logical contribution of one molecule.
///
/// This is a write-path projection.  A schema reads the bounded molecule set
/// declared by its catalog and adds these counters; it never walks atoms to
/// answer a storage request.  The counters deliberately count each current
/// tip: an atom shared by two molecule slots contributes to both logical
/// values, while physical atom de-duplication remains a node-level concern.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct MoleculeStorageCounter {
    pub molecule_uuid: String,
    pub active_slot_count: u64,
    pub active_atom_value_bytes: u64,
    #[serde(default)]
    pub active_blob_reference_bytes: u64,
    #[serde(default)]
    pub tip_index_bytes: u64,
    #[serde(default)]
    pub molecule_metadata_bytes: u64,
    #[serde(default)]
    pub retained_history_bytes: u64,
    #[serde(default)]
    pub counter_epoch: u64,
}

/// Durable contribution of one current molecule tip.
///
/// The write path needs this state after a restart to subtract the exact old
/// value before it adds a replacement.  It is source state for the counter,
/// not a liveness count and never authorizes reclaim.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct MoleculeTipCounterSource {
    pub atom_uuid: String,
    pub atom_value_bytes: u64,
    #[serde(default)]
    pub blob_reference_bytes: u64,
}

/// Bytes one molecule row contributes to its storage counter: the length of
/// its JSON serialization. The write path (`account_keep_small_items`) and the
/// bootstrap measurer (`measure_molecule_counter`) both call this, so a row
/// they both see can only count the same.
pub fn molecule_counter_row_bytes<T: Serialize + ?Sized>(
    value: &T,
) -> Result<u64, serde_json::Error> {
    serde_json::to_vec(value).map(|raw| raw.len() as u64)
}

impl MoleculeStorageCounter {
    #[must_use]
    pub fn logical_value_bytes(&self) -> u64 {
        self.active_atom_value_bytes
            .saturating_add(self.active_blob_reference_bytes)
    }

    #[must_use]
    pub fn structure_bytes(&self) -> u64 {
        self.tip_index_bytes
            .saturating_add(self.molecule_metadata_bytes)
    }
}

impl SchemaMeter {
    #[must_use]
    pub fn churn_ratio(&self) -> f64 {
        churn_ratio(self.appended_bytes, self.live_bytes)
    }

    /// Physical tip + bookkeeping bytes this schema owns.
    ///
    /// These are the bytes that used to fall out of per-schema accounting and
    /// land in the report's structural remainder.
    #[must_use]
    pub fn plane_bytes(&self) -> u64 {
        self.tip_bytes.saturating_add(self.bookkeeping_bytes)
    }
}

/// `appended / live`. Live 0 → 0.0 (no division by zero).
#[must_use]
pub fn churn_ratio(appended_bytes: u64, live_bytes: u64) -> f64 {
    if live_bytes == 0 {
        0.0
    } else {
        appended_bytes as f64 / live_bytes as f64
    }
}

/// Move a per-schema counter by the signed delta `new - previous`.
///
/// Saturating in both directions: a counter that hydrated from an older
/// snapshot has no history for keys written before the field existed, so a
/// shrink can legitimately try to subtract more than it holds.
#[must_use]
pub fn apply_delta(current: u64, new_bytes: u64, previous_bytes: u64) -> u64 {
    if new_bytes >= previous_bytes {
        current.saturating_add(new_bytes - previous_bytes)
    } else {
        current.saturating_sub(previous_bytes - new_bytes)
    }
}

/// Churn report: largest ratio first.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChurnReport {
    pub measured_at: DateTime<Utc>,
    pub heavy: bool,
    pub day: String,
    pub per_schema: Vec<SchemaChurnRow>,
}

/// One schema's churn line.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SchemaChurnRow {
    pub schema_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    pub live_bytes: u64,
    pub appended_bytes: u64,
    pub churn_ratio: f64,
}

/// Atom size histogram on the schemas/atom surface.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct AtomHistogram {
    pub p50: u64,
    pub p95: u64,
    pub p99: u64,
    pub max: u64,
    pub count: u64,
    pub ge_16kib: u64,
    pub ge_32kib: u64,
    pub ge_64kib: u64,
}

/// Nearest-rank percentile over an already-sorted ascending slice.
///
/// Index is `round(p/100 * (n-1))`. Empty → 0.
#[must_use]
pub fn percentile_sorted(sorted: &[u64], percentile: u8) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let n = sorted.len();
    let p = u32::from(percentile.min(100));
    let idx = ((p as f64 / 100.0) * (n.saturating_sub(1) as f64)).round() as usize;
    sorted[idx.min(n - 1)]
}

/// Build a histogram from the exact atom-size population just walked.
#[must_use]
pub fn atom_histogram(sizes: &[u64]) -> AtomHistogram {
    if sizes.is_empty() {
        return AtomHistogram::default();
    }
    let mut sorted = sizes.to_vec();
    sorted.sort_unstable();
    AtomHistogram {
        p50: percentile_sorted(&sorted, 50),
        p95: percentile_sorted(&sorted, 95),
        p99: percentile_sorted(&sorted, 99),
        max: *sorted.last().unwrap_or(&0),
        count: sorted.len() as u64,
        ge_16kib: sorted
            .iter()
            .filter(|&&b| b >= ATOM_HISTOGRAM_16KIB)
            .count() as u64,
        ge_32kib: sorted
            .iter()
            .filter(|&&b| b >= ATOM_HISTOGRAM_32KIB)
            .count() as u64,
        ge_64kib: sorted
            .iter()
            .filter(|&&b| b >= ATOM_HISTOGRAM_64KIB)
            .count() as u64,
    }
}

/// UTC calendar day used to bucket append bytes.
#[must_use]
pub fn utc_day(now: DateTime<Utc>) -> String {
    now.format("%Y-%m-%d").to_string()
}

/// Durable projection written to the `keep_small` collection under
/// [`KEEP_SMALL_SNAPSHOT_KEY`] (in `metadata` before 2026-09-21).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct KeepSmallSnapshot {
    pub totals: LiveBudgetTotals,
    pub schemas: HashMap<String, SchemaMeter>,
    /// One compact counter per molecule that this process has observed on the
    /// normal mutation path.  Missing old snapshots are intentionally not
    /// treated as a measured zero; `molecule_counters_complete` says whether
    /// this collection is safe to present as complete.
    #[serde(default)]
    pub molecules: HashMap<String, MoleculeStorageCounter>,
    #[serde(default)]
    pub molecule_counters_complete: bool,
    /// Exact source state for every counter row. Old snapshots omit this and
    /// therefore remain fail-closed until an isolated-copy bootstrap runs.
    #[serde(default)]
    pub molecule_counter_sources_complete: bool,
    #[serde(default)]
    pub molecule_tip_sources: HashMap<String, MoleculeTipCounterSource>,
    #[serde(default)]
    pub molecule_key_bytes: HashMap<String, u64>,
    #[serde(default)]
    pub molecule_schema: HashMap<String, String>,
    #[serde(default)]
    pub pending_protein_folds: u64,
    /// `Some(true)` only when the shutdown flush wrote this snapshot and no
    /// process has booted on the home since. Every other writer — the
    /// debounced persist, an initial hard-erase baseline or bootstrap flush, and the
    /// re-arm put hydrate makes right after reading a clean snapshot — writes
    /// `Some(false)`. So a `false` at hydrate means the previous process
    /// stopped without its shutdown flush, and metered writes after its last
    /// debounce tick are missing: the counters are a hint, not a measurement.
    /// `None` on snapshots from builds before 2026-09-21: no verdict, hydrate
    /// treats them as it always did. Soundness does not need a live epoch
    /// after the restart, only that the last writer of this row is known.
    #[serde(default)]
    pub clean_stop: Option<bool>,
    /// Independent provenance for global, schema, and molecule counters.
    /// Missing on pre-PR2 snapshots, which deserialize as incomplete.
    #[serde(default = "legacy_trust_payload")]
    pub trust: MeterTrustPayload,
    /// Cumulative hard-erase debits already reflected in this snapshot.
    /// A later durable debit row can be replayed without a whole-map flush.
    #[serde(default)]
    pub hard_erase_totals: KeepSmallHardEraseTotals,
    /// Highest committed operation sequence included in this snapshot.
    /// Rows at or below it can be pruned after this snapshot reaches disk.
    #[serde(default)]
    pub hard_erase_journal_checkpoint_seq: u64,
    /// When true, per-schema maps live at [`KEEP_SMALL_SCHEMA_SHARD_PREFIX`]
    /// keys (and [`KEEP_SMALL_UNATTRIBUTED_SHARD_KEY`]) instead of in this
    /// row. A whole-map blob from before this layout deserializes as false.
    #[serde(default)]
    pub sharded: bool,
}

/// One schema's durable meter slice. A persist writes only the schemas that
/// changed, so bytes on disk do not grow with the number of measured schemas.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct KeepSmallSchemaShard {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema: Option<SchemaMeter>,
    #[serde(default)]
    pub molecules: HashMap<String, MoleculeStorageCounter>,
    #[serde(default)]
    pub molecule_tip_sources: HashMap<String, MoleculeTipCounterSource>,
    #[serde(default)]
    pub molecule_key_bytes: HashMap<String, u64>,
    #[serde(default)]
    pub molecule_schema: HashMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trust: Option<MeterDomainTrust>,
}

/// Small durable aggregate; it stores schema totals, never erased row keys.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct KeepSmallHardEraseTotals {
    pub by_schema: HashMap<String, KeepSmallHardEraseDebit>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct KeepSmallHardEraseDebit {
    pub atom_bytes: u64,
    pub atom_count: u64,
    pub tip_bytes: u64,
    pub tip_count: u64,
}

/// Metadata key for [`KeepSmallSnapshot`]. Point get/put — not a store walk.
///
/// Since the sharded layout this row is a small header (totals, trust,
/// clean-stop). Per-schema maps live at [`KEEP_SMALL_SCHEMA_SHARD_PREFIX`].
pub const KEEP_SMALL_SNAPSHOT_KEY: &str = "keep_small:meters";
/// Prefix for one [`KeepSmallSchemaShard`] per measured schema.
pub const KEEP_SMALL_SCHEMA_SHARD_PREFIX: &str = "keep_small:meters:schema:";
/// Shard for molecule rows that have no owning schema yet.
pub const KEEP_SMALL_UNATTRIBUTED_SHARD_KEY: &str = "keep_small:meters:unattributed";
/// Sentinel schema name for [`KEEP_SMALL_UNATTRIBUTED_SHARD_KEY`] dirty tracking.
pub const KEEP_SMALL_UNATTRIBUTED_SCHEMA: &str = "\u{1}unattributed";
/// A small cumulative debit row. Hard erases flush this row, not the snapshot.
pub const KEEP_SMALL_HARD_ERASE_TOTALS_KEY: &str = "keep_small:hard_erase_totals";

const HEX: &[u8; 16] = b"0123456789abcdef";

/// Durable key for one schema's meter shard.
#[must_use]
pub fn keep_small_schema_shard_key(schema: &str) -> String {
    if schema == KEEP_SMALL_UNATTRIBUTED_SCHEMA {
        return KEEP_SMALL_UNATTRIBUTED_SHARD_KEY.to_string();
    }
    let mut out = String::with_capacity(
        KEEP_SMALL_SCHEMA_SHARD_PREFIX.len() + schema.len().saturating_mul(2),
    );
    out.push_str(KEEP_SMALL_SCHEMA_SHARD_PREFIX);
    for &byte in schema.as_bytes() {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

fn schema_owner_for_molecule(
    molecule_uuid: &str,
    molecule_schema: &HashMap<String, String>,
) -> String {
    molecule_schema
        .get(molecule_uuid)
        .cloned()
        .unwrap_or_else(|| KEEP_SMALL_UNATTRIBUTED_SCHEMA.to_string())
}

fn schema_owner_for_store_key(key: &str, molecule_schema: &HashMap<String, String>) -> String {
    crate::atom::molecule_key_codec::molecule_uuid_from_storage_key(key).map_or_else(
        || KEEP_SMALL_UNATTRIBUTED_SCHEMA.to_string(),
        |molecule| schema_owner_for_molecule(molecule, molecule_schema),
    )
}

impl KeepSmallSnapshot {
    /// Split per-schema maps into shards and leave a small header behind.
    ///
    /// `only_schemas = None` emits a shard for every schema (repair, first
    /// split of a legacy whole-map row). `Some(set)` emits only those names,
    /// so a persist of one dirty schema does not rewrite the rest.
    #[must_use]
    pub fn into_header_and_shards(
        mut self,
        only_schemas: Option<&HashSet<String>>,
    ) -> (Self, Vec<(String, KeepSmallSchemaShard)>) {
        let emit_all = only_schemas.is_none();
        let emit = |name: &str| emit_all || only_schemas.is_some_and(|set| set.contains(name));

        let mut buckets: HashMap<String, KeepSmallSchemaShard> = HashMap::new();
        fn take_bucket<'a>(
            buckets: &'a mut HashMap<String, KeepSmallSchemaShard>,
            name: &str,
        ) -> &'a mut KeepSmallSchemaShard {
            buckets.entry(name.to_string()).or_default()
        }

        for (name, meter) in self.schemas.drain() {
            if emit(&name) {
                take_bucket(&mut buckets, &name).schema = Some(meter);
            }
        }
        for (name, domain) in std::mem::take(&mut self.trust.schemas) {
            if emit(&name) {
                take_bucket(&mut buckets, &name).trust = Some(domain);
            }
        }
        for (molecule, counter) in self.molecules.drain() {
            let owner = schema_owner_for_molecule(&molecule, &self.molecule_schema);
            if emit(&owner) {
                take_bucket(&mut buckets, &owner)
                    .molecules
                    .insert(molecule, counter);
            }
        }
        let molecule_schema = std::mem::take(&mut self.molecule_schema);
        for (key, source) in self.molecule_tip_sources.drain() {
            let owner = schema_owner_for_store_key(&key, &molecule_schema);
            if emit(&owner) {
                take_bucket(&mut buckets, &owner)
                    .molecule_tip_sources
                    .insert(key, source);
            }
        }
        for (key, bytes) in self.molecule_key_bytes.drain() {
            let owner = schema_owner_for_store_key(&key, &molecule_schema);
            if emit(&owner) {
                take_bucket(&mut buckets, &owner)
                    .molecule_key_bytes
                    .insert(key, bytes);
            }
        }
        for (molecule, schema) in molecule_schema {
            let owner = if schema.is_empty() {
                KEEP_SMALL_UNATTRIBUTED_SCHEMA.to_string()
            } else {
                schema.clone()
            };
            if emit(&owner) {
                take_bucket(&mut buckets, &owner)
                    .molecule_schema
                    .insert(molecule, schema);
            }
        }
        if let Some(set) = only_schemas {
            for name in set {
                buckets.entry(name.clone()).or_default();
            }
        }

        let mut shards: Vec<(String, KeepSmallSchemaShard)> = buckets
            .into_iter()
            .map(|(name, shard)| (keep_small_schema_shard_key(&name), shard))
            .collect();
        shards.sort_by(|a, b| a.0.cmp(&b.0));
        self.sharded = true;
        (self, shards)
    }

    /// Fold one shard back into a full in-memory snapshot.
    pub fn merge_shard(&mut self, shard: KeepSmallSchemaShard) {
        let schema_name = shard.schema.as_ref().map(|meter| meter.schema_name.clone());
        if let Some(meter) = shard.schema {
            self.schemas.insert(meter.schema_name.clone(), meter);
        }
        if let (Some(name), Some(trust)) = (schema_name, shard.trust) {
            self.trust.schemas.insert(name, trust);
        }
        self.molecules.extend(shard.molecules);
        self.molecule_tip_sources.extend(shard.molecule_tip_sources);
        self.molecule_key_bytes.extend(shard.molecule_key_bytes);
        self.molecule_schema.extend(shard.molecule_schema);
    }
}

/// The LastStore collection that holds [`KEEP_SMALL_SNAPSHOT_KEY`].
///
/// Its own plane since 2026-09-21, when the snapshot's former home
/// (`metadata`, one hash group, no automatic compaction) reached 39 GB of
/// superseded copies and looped the primary. Node-local, rebuildable
/// bookkeeping: capture-skipped, backup-excluded, compact-allowlisted and
/// swept by the residual self-compactor. The key name is unchanged so the
/// capture-skip exact-key contract for the legacy row still applies.
pub const KEEP_SMALL_SNAPSHOT_COLLECTION: &str = "keep_small";

/// In-process incremental projection. Point-read cheap; updated on the write path.
#[derive(Debug)]
pub struct KeepSmallMeters {
    atom_bytes: AtomicU64,
    tip_bytes: AtomicU64,
    bookkeeping_bytes: AtomicU64,
    atom_count: AtomicU64,
    tip_count: AtomicU64,
    schemas: Mutex<HashMap<String, SchemaMeter>>,
    molecules: Mutex<HashMap<String, MoleculeStorageCounter>>,
    molecule_counters_complete: AtomicBool,
    /// atom uuid → (schema, atom bytes, blob bytes, contribution complete).
    pending_atoms: Mutex<HashMap<String, (String, u64, u64, bool)>>,
    /// Last written size per store key. Used so tip rewrite accounting does
    /// not have to GET the live tip (those GETs are observable).
    last_key_bytes: Mutex<HashMap<String, u64>>,
    /// tip key → (atom uuid, logical atom bytes) of the last accounted tip.
    last_tip_atom: Mutex<HashMap<String, MoleculeTipCounterSource>>,
    /// molecule uuid → owning schema, learned from tip puts.
    ///
    /// A molecule belongs to exactly one field of one schema, so its tip and
    /// bookkeeping rows belong to that schema too. This map is the only thing
    /// that carries that fact to the meters, because the tip/bookkeeping keys
    /// themselves hold no schema name.
    ///
    /// It is deliberately **not** part of [`KeepSmallSnapshot`]: it holds one
    /// entry per written molecule, and persisting it would turn a small
    /// point-read projection into a per-molecule table. After a restart the
    /// first tip put re-learns the binding, so attribution resumes on the next
    /// write instead of being carried across process lifetimes.
    molecule_schema: Mutex<HashMap<String, String>>,
    /// Set when boot looked for the durable snapshot and did not find it.
    ///
    /// A miss is not the same as a measured zero. On a home that already holds
    /// data, every total below is then a floor of zero for the pre-existing
    /// corpus, and organic writes never close that gap — they only meter what
    /// they themselves write. Readers must be able to say "never measured"
    /// rather than print zero as an exact answer.
    ///
    /// Sticky for the process: a later write makes the totals nonzero without
    /// making them complete, so clearing this on the first write would restore
    /// exactly the false-exact reading it exists to prevent. Only
    /// [`Self::import`] clears it.
    hydrate_missed: AtomicBool,
    /// Set when boot hydrated a snapshot not written by a shutdown flush
    /// (`clean_stop != Some(true)`): metered writes landed after the last
    /// debounced persist and the process stopped without the shutdown flush
    /// (2026-09-21 design — the debounce accepts that staleness, the report
    /// must not call it exact). Cleared only by
    /// [`Self::install_molecule_counter_bootstrap`], the same repair a
    /// hydrate miss takes.
    stale_after_unclean_stop: AtomicBool,
    pending_protein_folds: AtomicU64,
    trust: Mutex<MeterTrustPayload>,
    /// Schemas whose durable shard is behind the live projection.
    dirty_schemas: Mutex<HashSet<String>>,
}

impl Default for KeepSmallMeters {
    fn default() -> Self {
        Self {
            atom_bytes: AtomicU64::new(0),
            tip_bytes: AtomicU64::new(0),
            bookkeeping_bytes: AtomicU64::new(0),
            atom_count: AtomicU64::new(0),
            tip_count: AtomicU64::new(0),
            schemas: Mutex::new(HashMap::new()),
            molecules: Mutex::new(HashMap::new()),
            molecule_counters_complete: AtomicBool::new(true),
            pending_atoms: Mutex::new(HashMap::new()),
            last_key_bytes: Mutex::new(HashMap::new()),
            last_tip_atom: Mutex::new(HashMap::new()),
            molecule_schema: Mutex::new(HashMap::new()),
            hydrate_missed: AtomicBool::new(false),
            stale_after_unclean_stop: AtomicBool::new(false),
            pending_protein_folds: AtomicU64::new(0),
            trust: Mutex::new(MeterTrustPayload::trusted()),
            dirty_schemas: Mutex::new(HashSet::new()),
        }
    }
}

/// Planes whose physical bytes count toward the budget (file blobs exempt).
pub const BUDGET_PHYSICAL_PLANES: &[&str] = &[
    "atoms",
    "tips",
    "indexes",
    "field_update_order_log",
    "atom_locators",
    "proteins",
    "schema_index",
];

/// Sum `collection_disk_bytes` over [`BUDGET_PHYSICAL_PLANES`].
#[must_use]
pub fn sum_physical_plane_bytes(mut disk_bytes: impl FnMut(&str) -> Option<u64>) -> u64 {
    BUDGET_PHYSICAL_PLANES
        .iter()
        .map(|plane| disk_bytes(plane).unwrap_or(0))
        .fold(0u64, u64::saturating_add)
}

/// Capture-neutral contract: a LastStore segment compact rewrites live keys
/// in the collection directory. It does not go through `KvStore::put`, so the
/// mutation-log capture wrapper cannot emit pin-log records for the rewrite.
///
/// Returning 0 is the whole safety argument for self-compacting captured
/// planes (tips) without reopening the 2026-08-08 amplifier.
#[must_use]
pub fn physical_compact_rewrite_capture_records(_collection: &str) -> u64 {
    0
}

/// True when compacting `collection` is a LastStore segment rewrite (no
/// capture records). Named so a future put-through compact fails the bar
/// instead of silently amplifying the pin log.
#[must_use]
pub fn physical_compact_rewrite_is_capture_neutral(collection: &str) -> bool {
    physical_compact_rewrite_capture_records(collection) == 0
        && matches!(
            collection,
            "tips"
                | "indexes"
                | "atoms"
                | "schemas"
                | "schema_states"
                | "schema_index"
                | "atom_locators"
                | "idempotency"
                | "change_feed"
                | "keep_small"
                | "sync_pin_log"
                | "sync_capture_reexport"
                | "molecule_ref_edges"
                | "blob_ref_edges"
        )
}

mod atoms;
mod molecules;
mod schemas;
mod snapshot_io;
mod tips;
mod trust;
