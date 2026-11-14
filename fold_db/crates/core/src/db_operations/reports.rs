//! Per-schema storage accounting reports and schema record-key listing types.

use super::{
    record_store::{RangeWalk, RecordStore},
    DbOperations,
};
use crate::error::FoldDbError;
use crate::schema::types::{field::FilterUtils, SchemaError};
use chrono::{DateTime, Utc};
use lastdb_model::record_space::{record_space_uuid, RECORD_PREFIX};
use lastdb_model::record_value::StoredRecord;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SchemaRecordKey {
    pub hash: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub range: String,
}

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

impl DbOperations {
    pub async fn schema_storage_report(&self) -> Result<SchemaStorageReport, FoldDbError> {
        let schemas = self
            .schemas()
            .get_all_schemas()
            .await
            .map_err(|e| FoldDbError::Database(format!("list schemas: {e}")))?;
        let spaces: HashMap<String, &str> = schemas
            .values()
            .map(|s| (record_space_uuid(&s.name), s.name.as_str()))
            .collect();
        let (mut per_schema, total_logical_bytes) =
            scan_record_storage(&self.record_store, &spaces)
                .await
                .map_err(|e| FoldDbError::Database(format!("schema record storage: {e}")))?;
        let mut display = HashMap::new();
        for (key, s) in &schemas {
            let label = s
                .descriptive_name
                .as_deref()
                .map(str::trim)
                .filter(|v| !v.is_empty())
                .unwrap_or(s.name.trim());
            if !label.is_empty() {
                for a in [
                    Some(key.as_str()),
                    Some(s.name.as_str()),
                    s.identity_hash.as_deref(),
                ]
                .into_iter()
                .flatten()
                {
                    display.insert(a, label);
                }
            }
        }
        for row in &mut per_schema {
            row.display_name = display
                .get(row.schema_name.as_str())
                .filter(|&&l| l != row.schema_name)
                .map(|&l| l.to_owned());
        }
        Ok(SchemaStorageReport {
            measured_at: Utc::now(),
            heavy: true,
            total_records: per_schema.iter().map(|s| s.record_count).sum(),
            total_record_bytes: per_schema.iter().map(|s| s.record_bytes).sum(),
            schema_count: per_schema.len() as u64,
            per_schema,
            total_logical_bytes,
        })
    }
}

async fn scan_record_storage(
    records: &RecordStore,
    spaces: &HashMap<String, &str>,
) -> Result<(Vec<SchemaStorage>, u64), SchemaError> {
    let end = FilterUtils::create_prefix_end(RECORD_PREFIX)?;
    let mut walk = RangeWalk::new(RECORD_PREFIX.as_bytes(), end.as_bytes(), 16);
    let mut totals = HashMap::<String, (u64, u64)>::new();
    while !walk.done() {
        let rows = records
            .walk_step(&mut walk)
            .await
            .map_err(|e| SchemaError::InvalidData(format!("record storage scan: {e}")))?;
        let mut page_bytes = 0u64;
        for (key, value) in &rows {
            page_bytes += (key.len() + value.len()) as u64;
            let rec = StoredRecord::decode(value).map_err(|e| {
                SchemaError::InvalidData(format!("record storage scan: a stored record: {e}"))
            })?;
            if !rec.is_live() {
                continue;
            }
            let space = key
                .strip_prefix(RECORD_PREFIX.as_bytes())
                .and_then(|rest| {
                    let at = rest.iter().position(|&b| b == b':')?;
                    std::str::from_utf8(rest.get(..at)?).ok()
                })
                .ok_or_else(|| {
                    SchemaError::InvalidData(
                        "record storage scan: a record key has no space".into(),
                    )
                })?;
            let name = spaces
                .get(space)
                .map_or_else(|| format!("{RECORD_PREFIX}{space}"), |&n| n.to_owned());
            let entry = totals.entry(name).or_default();
            entry.0 += 1;
            entry.1 += value.len() as u64;
        }
        if !rows.is_empty() {
            let avg = (page_bytes / rows.len() as u64).max(1);
            walk.set_limit(
                usize::try_from((8 * 1024 * 1024) / avg)
                    .unwrap_or(4096)
                    .clamp(16, 4096),
            );
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
