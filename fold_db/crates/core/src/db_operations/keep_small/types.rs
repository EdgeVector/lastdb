//! Meter data types, snapshot rows, and durable key names.

use super::*;

/// Steady-state budget for live logical DB planes (file blobs exempt).
pub const LIVE_BUDGET_BYTES: u64 = 1 << 30;

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

/// Versioned trust for global, schema, and field key counter domains.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MeterTrustPayload {
    pub version: u32,
    pub global: MeterDomainTrust,
    #[serde(default)]
    pub schemas: BTreeMap<String, MeterDomainTrust>,
    pub field_keys: MeterDomainTrust,
}

impl MeterTrustPayload {
    #[must_use]
    pub fn absent() -> Self {
        Self {
            version: KEEP_SMALL_TRUST_VERSION,
            global: MeterDomainTrust::absent(),
            schemas: BTreeMap::new(),
            field_keys: MeterDomainTrust::absent(),
        }
    }

    #[must_use]
    pub fn trusted() -> Self {
        Self {
            version: KEEP_SMALL_TRUST_VERSION,
            global: MeterDomainTrust::trusted(),
            schemas: BTreeMap::new(),
            field_keys: MeterDomainTrust::trusted(),
        }
    }

    #[must_use]
    pub fn incomplete(cause: &str) -> Self {
        Self {
            version: KEEP_SMALL_TRUST_VERSION,
            global: MeterDomainTrust::incomplete(cause),
            schemas: BTreeMap::new(),
            field_keys: MeterDomainTrust::incomplete(cause),
        }
    }

    pub fn mark_incomplete(&mut self, cause: &str) {
        self.version = KEEP_SMALL_TRUST_VERSION;
        self.global = MeterDomainTrust::incomplete(cause);
        self.field_keys = MeterDomainTrust::incomplete(cause);
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

/// Per-schema incremental meter.
///
/// A snapshot written before the format 7 cleanup also holds `record_count`,
/// `tip_bytes` and `bookkeeping_bytes`. They counted format 6 records, tips and
/// field key bookkeeping; serde ignores them on hydrate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaMeter {
    pub schema_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    /// Stored value bytes of the schema.
    pub live_bytes: u64,
    /// Bytes appended on [`Self::appended_day`] (UTC).
    pub appended_bytes: u64,
    /// UTC calendar day `YYYY-MM-DD` the append counter is for.
    pub appended_day: String,
}

/// The current logical contribution of one field key.
///
/// A schema reads the bounded field key set declared by its catalog and adds
/// these counters; it never walks records to answer a storage request.
///
/// No format 7 write adds to these counters. A schema install seeds an exact
/// zero for each declared field key (`ensure_field_key_counter`), and a
/// snapshot hydrated from an older home keeps the values it had. The schema
/// storage reports and the attribution ledger still read them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct FieldKeyStorageCounter {
    #[serde(alias = "molecule_uuid")]
    pub field_key_id: String,
    pub active_slot_count: u64,
    #[serde(alias = "active_atom_value_bytes")]
    pub active_value_bytes: u64,
    #[serde(default)]
    pub active_blob_reference_bytes: u64,
    #[serde(default)]
    pub tip_index_bytes: u64,
    #[serde(default)]
    #[serde(default, alias = "molecule_metadata_bytes")]
    pub field_key_metadata_bytes: u64,
    #[serde(default)]
    pub retained_history_bytes: u64,
    #[serde(default)]
    pub counter_epoch: u64,
}

impl FieldKeyStorageCounter {
    #[must_use]
    pub fn logical_value_bytes(&self) -> u64 {
        self.active_value_bytes
            .saturating_add(self.active_blob_reference_bytes)
    }

    #[must_use]
    pub fn structure_bytes(&self) -> u64 {
        self.tip_index_bytes
            .saturating_add(self.field_key_metadata_bytes)
    }
}
