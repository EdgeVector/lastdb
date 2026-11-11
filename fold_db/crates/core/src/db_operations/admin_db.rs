//! Live DB admin storage reports and schema record key report types.

use super::storage_breakdown::{SchemaStorage, StorageBreakdown};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Per-schema record storage — the dedicated `lastdb db schemas` report.
///
/// The report counts the live records of each schema and their stored value bytes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaStorageReport {
    /// RFC 3339 timestamp when this walk finished.
    pub measured_at: DateTime<Utc>,
    /// Always `true`. This is an admin scan, not a cheap status gauge.
    pub heavy: bool,
    /// One row per schema that has at least one live record, largest first.
    pub per_schema: Vec<SchemaStorage>,
    pub total_logical_bytes: u64,
    /// Live records of all schemas.
    #[serde(default)]
    pub total_records: u64,
    /// Stored value bytes of those records. They are part of
    /// `total_logical_bytes`.
    #[serde(default)]
    pub total_record_bytes: u64,
    pub schema_count: u64,
}

impl SchemaStorageReport {
    pub fn from_breakdown(breakdown: StorageBreakdown) -> Self {
        let total_records = breakdown.per_schema.iter().map(|s| s.record_count).sum();
        let total_record_bytes = breakdown.per_schema.iter().map(|s| s.record_bytes).sum();
        let schema_count = breakdown.per_schema.len() as u64;
        Self {
            measured_at: Utc::now(),
            heavy: true,
            total_logical_bytes: breakdown.total_logical_bytes,
            per_schema: breakdown.per_schema,
            total_records,
            total_record_bytes,
            schema_count,
        }
    }
}

/// Peak resident bytes one storage-breakdown page is allowed to hold.
pub(in crate::db_operations) const INVENTORY_PAGE_BYTE_BUDGET: u64 = 8 * 1024 * 1024;
/// Lower bound on the adaptive page, and the size of the first page of every scan.
pub(in crate::db_operations) const INVENTORY_PAGE_ROWS_MIN: usize = 16;
/// Upper bound on the adaptive page.
pub(in crate::db_operations) const INVENTORY_PAGE_ROWS_MAX: usize = 4096;

/// Re-aim the next page at [`INVENTORY_PAGE_BYTE_BUDGET`] from what the page
/// just read actually weighed.
pub(in crate::db_operations) fn inventory_next_page_rows(
    page_bytes: u64,
    rows_in_page: usize,
) -> usize {
    let avg = (page_bytes / rows_in_page.max(1) as u64).max(1);
    usize::try_from(INVENTORY_PAGE_BYTE_BUDGET / avg)
        .unwrap_or(INVENTORY_PAGE_ROWS_MAX)
        .clamp(INVENTORY_PAGE_ROWS_MIN, INVENTORY_PAGE_ROWS_MAX)
}

/// One live record identity from a schema's record space.
///
/// Product `lastdb list` returns these — never record bodies. `range` is empty
/// for HashKey / hash-only slots.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SchemaRecordKey {
    pub hash: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub range: String,
}

/// Paged API keys for one schema's current records.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaRecordKeysReport {
    /// Caller-facing name (descriptive / `app/Name` when that is what was asked).
    pub schema: String,
    /// Canonical schema identity the walk used.
    pub schema_id: String,
    /// Primary address shape, independent of payload fields.
    pub key_definition: schema_types::record_keys::RecordKeyDefinition,
    /// API hash this page was restricted to, when the caller passed `hash=`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hash_filter: Option<String>,
    pub field_key: String,
    pub keys: Vec<SchemaRecordKey>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
    pub has_more: bool,
    /// Same as `has_more` — honesty flag so a page is never mistaken for a census.
    pub truncated: bool,
}

/// Exact schema identity and page controls for a schema-facing key list.
#[derive(Debug, Clone, Copy)]
pub struct SchemaRecordKeysRequest<'a> {
    pub requested: &'a str,
    pub schema_id: &'a str,
    pub limit: usize,
    pub cursor: Option<&'a str>,
    pub hash_filter: Option<&'a str>,
}
