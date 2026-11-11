//! Live DB admin storage reports, per-schema logical breakdown, and key listing types.

use crate::db_operations::record_store::RangeWalk;
use crate::db_operations::RecordStore;
use crate::schema::types::field::FilterUtils;
use crate::schema::types::SchemaError;
use chrono::{DateTime, Utc};
use lastdb_model::record_space::{record_space_uuid, RECORD_PREFIX};
use lastdb_model::record_value::StoredRecord;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Peak resident bytes one storage-breakdown page is allowed to hold.
pub(in crate::db_operations) const INVENTORY_PAGE_BYTE_BUDGET: u64 = 8 * 1024 * 1024;
/// Lower bound on the adaptive page, and the size of the first page of every scan.
pub(in crate::db_operations) const INVENTORY_PAGE_ROWS_MIN: usize = 16;
/// Upper bound on the adaptive page.
pub(in crate::db_operations) const INVENTORY_PAGE_ROWS_MAX: usize = 4096;

pub(in crate::db_operations) fn inventory_next_page_rows(
    page_bytes: u64,
    rows_in_page: usize,
) -> usize {
    let avg = (page_bytes / rows_in_page.max(1) as u64).max(1);
    usize::try_from(INVENTORY_PAGE_BYTE_BUDGET / avg)
        .unwrap_or(INVENTORY_PAGE_ROWS_MAX)
        .clamp(INVENTORY_PAGE_ROWS_MIN, INVENTORY_PAGE_ROWS_MAX)
}

/// One schema's contribution to local logical storage.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaStorage {
    pub schema_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    pub bytes: u64,
    #[serde(default)]
    pub record_count: u64,
    #[serde(default)]
    pub record_bytes: u64,
}

/// Per-schema logical storage breakdown plus total logical bytes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct StorageBreakdown {
    pub per_schema: Vec<SchemaStorage>,
    pub total_logical_bytes: u64,
}

impl StorageBreakdown {
    /// Join human catalog names onto accounting rows.
    pub fn apply_display_names(&mut self, names: &HashMap<String, String>) {
        for row in &mut self.per_schema {
            if row.display_name.is_some() {
                continue;
            }
            let Some(label) = names.get(&row.schema_name) else {
                continue;
            };
            let label = label.trim();
            if !label.is_empty() && label != row.schema_name {
                row.display_name = Some(label.to_string());
            }
        }
    }

    fn add_records(&mut self, totals: HashMap<String, (u64, u64)>) {
        for (schema_name, (count, bytes)) in totals {
            let row = if let Some(at) = self
                .per_schema
                .iter()
                .position(|r| r.schema_name == schema_name)
            {
                &mut self.per_schema[at]
            } else {
                self.per_schema.push(SchemaStorage {
                    schema_name,
                    display_name: None,
                    bytes: 0,
                    record_count: 0,
                    record_bytes: 0,
                });
                self.per_schema.last_mut().expect("row was just pushed")
            };
            row.record_count += count;
            row.record_bytes += bytes;
            row.bytes += bytes;
            self.total_logical_bytes += bytes;
        }
        self.per_schema.sort_by(|a, b| {
            b.bytes
                .cmp(&a.bytes)
                .then_with(|| a.schema_name.cmp(&b.schema_name))
        });
    }
}

/// Per-schema record storage — the dedicated `lastdb db schemas` report.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaStorageReport {
    pub measured_at: DateTime<Utc>,
    pub heavy: bool,
    pub per_schema: Vec<SchemaStorage>,
    pub total_logical_bytes: u64,
    #[serde(default)]
    pub total_records: u64,
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

/// One live record identity from a schema's record space.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SchemaRecordKey {
    pub hash: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub range: String,
}

/// Paged API keys for one schema's current records.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaRecordKeysReport {
    pub schema: String,
    pub schema_id: String,
    pub key_definition: schema_types::record_keys::RecordKeyDefinition,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hash_filter: Option<String>,
    pub field_key: String,
    pub keys: Vec<SchemaRecordKey>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
    pub has_more: bool,
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

impl RecordStore {
    /// Per-schema logical storage breakdown across `schema_names`, sorted by bytes desc.
    pub async fn record_storage_breakdown(
        &self,
        schema_names: &[String],
    ) -> Result<StorageBreakdown, SchemaError> {
        let mut breakdown = StorageBreakdown::default();
        let spaces: HashMap<String, &str> = schema_names
            .iter()
            .map(|name| (record_space_uuid(name), name.as_str()))
            .collect();
        let start = RECORD_PREFIX;
        let end = FilterUtils::create_prefix_end(start)?;
        let mut walk = RangeWalk::new(start.as_bytes(), end.as_bytes(), INVENTORY_PAGE_ROWS_MIN);
        let mut totals = HashMap::<String, (u64, u64)>::new();
        while !walk.done() {
            let rows = self
                .walk_step(&mut walk)
                .await
                .map_err(|e| SchemaError::InvalidData(format!("record storage scan: {e}")))?
                .rows;
            let mut page_bytes = 0u64;
            for (key, value) in &rows {
                page_bytes += (key.len() + value.len()) as u64;
                note_record_row(&mut totals, &spaces, &key[start.len()..], value)?;
            }
            if !rows.is_empty() {
                walk.set_limit(inventory_next_page_rows(page_bytes, rows.len()));
            }
        }
        breakdown.add_records(totals);
        Ok(breakdown)
    }
}

fn note_record_row(
    totals: &mut HashMap<String, (u64, u64)>,
    spaces: &HashMap<String, &str>,
    rest: &[u8],
    value: &[u8],
) -> Result<(), SchemaError> {
    let record = StoredRecord::decode(value).map_err(|e| {
        SchemaError::InvalidData(format!("record storage scan: a stored record: {e}"))
    })?;
    if !record.is_live() {
        return Ok(());
    }
    let space = rest
        .iter()
        .position(|byte| *byte == b':')
        .and_then(|end| std::str::from_utf8(&rest[..end]).ok())
        .ok_or_else(|| {
            SchemaError::InvalidData("record storage scan: a record key has no space".into())
        })?;
    let name = spaces.get(space).map_or_else(
        || format!("{RECORD_PREFIX}{space}"),
        |name| (*name).to_owned(),
    );
    let entry = totals.entry(name).or_default();
    entry.0 += 1;
    entry.1 += value.len() as u64;
    Ok(())
}
