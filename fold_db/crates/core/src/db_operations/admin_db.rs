//! Live DB admin storage reports, per-schema logical breakdown, and key listing types.

use crate::db_operations::record_store::RangeWalk;
use crate::db_operations::{DbOperations, RecordStore};
use crate::error::FoldDbError;
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

/// Per-schema record storage — the dedicated `GET /api/db/schemas` report.
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

impl DbOperations {
    /// Measure live per-schema logical storage joined with human catalog names.
    pub async fn schema_storage_report(&self) -> Result<SchemaStorageReport, FoldDbError> {
        let schemas = self
            .schemas()
            .get_all_schemas()
            .await
            .map_err(|e| FoldDbError::Database(format!("list schemas: {e}")))?;
        let names: Vec<String> = schemas.values().map(|s| s.name.clone()).collect();
        let (mut per_schema, total_logical_bytes) = self
            .records()
            .record_storage_breakdown(&names)
            .await
            .map_err(|e| FoldDbError::Database(format!("schema record storage: {e}")))?;
        let mut display = HashMap::new();
        for (key, schema) in &schemas {
            let label = schema
                .descriptive_name
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .unwrap_or(schema.name.trim());
            if label.is_empty() {
                continue;
            }
            display.insert(key.clone(), label.to_string());
            display.insert(schema.name.clone(), label.to_string());
            if let Some(hash) = schema.identity_hash.as_ref() {
                display.insert(hash.clone(), label.to_string());
            }
        }
        for row in &mut per_schema {
            if let Some(label) = display
                .get(&row.schema_name)
                .filter(|l| l.as_str() != row.schema_name)
            {
                row.display_name = Some(label.clone());
            }
        }
        let total_records = per_schema.iter().map(|s| s.record_count).sum();
        let total_record_bytes = per_schema.iter().map(|s| s.record_bytes).sum();
        let schema_count = per_schema.len() as u64;
        Ok(SchemaStorageReport {
            measured_at: Utc::now(),
            heavy: true,
            per_schema,
            total_logical_bytes,
            total_records,
            total_record_bytes,
            schema_count,
        })
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

impl RecordStore {
    async fn record_storage_breakdown(
        &self,
        schema_names: &[String],
    ) -> Result<(Vec<SchemaStorage>, u64), SchemaError> {
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
        let mut total_bytes = 0u64;
        let mut per_schema: Vec<SchemaStorage> = totals
            .into_iter()
            .map(|(schema_name, (record_count, record_bytes))| {
                total_bytes += record_bytes;
                SchemaStorage {
                    schema_name,
                    display_name: None,
                    bytes: record_bytes,
                    record_count,
                    record_bytes,
                }
            })
            .collect();
        per_schema.sort_by(|a, b| {
            b.bytes
                .cmp(&a.bytes)
                .then_with(|| a.schema_name.cmp(&b.schema_name))
        });
        Ok((per_schema, total_bytes))
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
