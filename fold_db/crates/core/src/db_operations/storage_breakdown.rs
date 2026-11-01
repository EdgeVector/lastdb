//! Per-schema logical storage breakdown.
//!
//! Answers "how many bytes is each schema responsible for?" by walking the
//! record plane of the local store and summing the stored value bytes of each
//! live record, grouped by schema (`record_rows.rs`).
//!
//! This is a **logical, plaintext** size — the stored JSON bytes of the
//! locally-stored records — NOT the exact encrypted-cloud footprint. Cloud
//! objects are E2E-encrypted opaque blobs the server cannot attribute to a
//! schema (see `exemem_common::storage::is_r2_path` and the storage_service
//! billing path), so per-schema attribution can only be computed client-side
//! here, where the decrypted store and the schema definitions both live. It is
//! an approximation of the cloud footprint, deliberately high-level: encryption
//! overhead, disk file framing, and the sync log/snapshot framing are not
//! modeled. Tom confirmed approximate/high-level is fine for the storage
//! breakdown view.

mod record_rows;

use crate::{db_operations::AtomStore, schema::SchemaError};
use serde::{Deserialize, Serialize};

/// One schema's contribution to local logical storage.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaStorage {
    /// Canonical runtime schema name (identity hash, possibly
    /// `{storage_prefix}:{hash}`) — the name the record space derives from.
    pub schema_name: String,
    /// Catalog `descriptive_name` (or a more human catalog key) when one
    /// exists and differs from `schema_name`. Display join only — never an
    /// accounting key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    /// The stored value bytes of the live records of this schema
    /// (`record_bytes`).
    pub bytes: u64,
    /// Live records of this schema.
    #[serde(default)]
    pub record_count: u64,
    /// Sum of the stored value bytes of those records (`StoredRecord::encode`).
    #[serde(default)]
    pub record_bytes: u64,
}

/// The local logical storage breakdown: per-schema sizes plus the total.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct StorageBreakdown {
    /// One entry per schema that has at least one record, sorted by `bytes`
    /// descending (largest first) so the UI can render a sorted bar/list and
    /// take a top-N without re-sorting.
    pub per_schema: Vec<SchemaStorage>,
    /// `sum(per_schema[].bytes)` — the total logical bytes across all schemas.
    pub total_logical_bytes: u64,
}

impl AtomStore {
    /// The per-schema logical storage breakdown: the live records of each
    /// schema in `schema_names`, under `storage_prefix`. Rows are sorted by
    /// `bytes` descending.
    pub async fn record_storage_breakdown(
        &self,
        schema_names: &[String],
        storage_prefix: Option<&str>,
    ) -> Result<StorageBreakdown, SchemaError> {
        let mut breakdown = StorageBreakdown::default();
        self.add_record_storage(&mut breakdown, schema_names, storage_prefix)
            .await?;
        Ok(breakdown)
    }
}

/// Largest schema first; tie-break on name for deterministic ordering.
fn sort_rows(rows: &mut [SchemaStorage]) {
    rows.sort_by(|a, b| {
        b.bytes
            .cmp(&a.bytes)
            .then_with(|| a.schema_name.cmp(&b.schema_name))
    });
}

impl StorageBreakdown {
    /// Join human catalog names onto accounting rows.
    ///
    /// `names` maps runtime identity (`schema_name`) → display label. A label
    /// equal to the identity is ignored so hashes are not duplicated.
    pub fn apply_display_names(&mut self, names: &std::collections::HashMap<String, String>) {
        for row in &mut self.per_schema {
            if row.display_name.is_some() {
                continue;
            }
            let Some(label) = names.get(&row.schema_name) else {
                continue;
            };
            let label = label.trim();
            if label.is_empty() || label == row.schema_name {
                continue;
            }
            row.display_name = Some(label.to_string());
        }
    }
}
