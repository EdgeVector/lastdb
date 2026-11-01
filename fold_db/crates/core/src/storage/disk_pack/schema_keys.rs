//! The status of the retired schema key preload.
//!
//! A format 7 home keeps no field molecules, so no complete key membership
//! loads. The status keeps its reply shape: the policy and the byte limit
//! still read their environment, and every count is zero.

use crate::resident::core_keys::{DEFAULT_SCHEMA_KEY_BYTES, SCHEMA_KEY_BYTES_ENV};

#[derive(Default, serde::Serialize)]
pub struct SchemaKeyStatus {
    pub enabled: bool,
    pub domains: usize,
    pub ready: usize,
    pub loading: usize,
    pub disk_only: usize,
    /// Retained for the status contract; refused preloads use disk_only.
    pub failed: usize,
    pub keys: usize,
    pub loads: u64,
    pub memory_selections: u64,
    pub bytes: usize,
    pub peak_bytes: usize,
    pub limit_bytes: usize,
    pub reasons: Vec<String>,
}

pub(super) struct SchemaKeys {
    enabled: bool,
    limit_bytes: usize,
}

impl SchemaKeys {
    pub(super) fn new() -> Self {
        Self {
            enabled: std::env::var("LASTDB_SCHEMA_PRELOAD_SCHEMAS")
                .unwrap_or_default()
                .split(',')
                .any(|name| !name.trim().is_empty()),
            limit_bytes: std::env::var(SCHEMA_KEY_BYTES_ENV)
                .ok()
                .and_then(|raw| raw.trim().parse().ok())
                .unwrap_or(DEFAULT_SCHEMA_KEY_BYTES),
        }
    }

    pub(super) fn status(&self) -> SchemaKeyStatus {
        SchemaKeyStatus {
            enabled: self.enabled,
            limit_bytes: self.limit_bytes,
            ..SchemaKeyStatus::default()
        }
    }
}
