//! Meter data types, snapshot rows, and durable key names.

use super::*;

/// Steady-state budget for live logical DB planes (file blobs exempt).
pub const LIVE_BUDGET_BYTES: u64 = 1 << 30;

/// 16 KiB fence-visible bucket.
pub const ATOM_HISTOGRAM_16KIB: u64 = 16 * 1024;
/// 32 KiB fence-visible bucket.
pub const ATOM_HISTOGRAM_32KIB: u64 = 32 * 1024;
/// 64 KiB bucket (the product atom content fence before 2026-10-12).
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

pub(super) fn legacy_trust_payload() -> MeterTrustPayload {
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
    /// Physical tip + bookkeeping bytes this schema owns.
    ///
    /// These are the bytes that used to fall out of per-schema accounting and
    /// land in the report's structural remainder.
    #[must_use]
    pub fn plane_bytes(&self) -> u64 {
        self.tip_bytes.saturating_add(self.bookkeeping_bytes)
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
