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

/// Versioned trust for the global and schema counter domains.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MeterTrustPayload {
    pub version: u32,
    pub global: MeterDomainTrust,
    #[serde(default)]
    pub schemas: BTreeMap<String, MeterDomainTrust>,
}

impl MeterTrustPayload {
    #[must_use]
    pub fn absent() -> Self {
        Self {
            version: KEEP_SMALL_TRUST_VERSION,
            global: MeterDomainTrust::absent(),
            schemas: BTreeMap::new(),
        }
    }

    #[must_use]
    pub fn trusted() -> Self {
        Self {
            version: KEEP_SMALL_TRUST_VERSION,
            global: MeterDomainTrust::trusted(),
            schemas: BTreeMap::new(),
        }
    }

    #[must_use]
    pub fn incomplete(cause: &str) -> Self {
        Self {
            version: KEEP_SMALL_TRUST_VERSION,
            global: MeterDomainTrust::incomplete(cause),
            schemas: BTreeMap::new(),
        }
    }

    pub fn mark_incomplete(&mut self, cause: &str) {
        self.version = KEEP_SMALL_TRUST_VERSION;
        self.global = MeterDomainTrust::incomplete(cause);
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
