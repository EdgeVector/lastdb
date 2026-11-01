//! Format 7 rows of the per-schema logical storage breakdown.
//!
//! A format 7 home stores no atoms. Each record is one value at
//! `rk:{S}:{esc(hash)}\0{range}`, where `S` is the record space of one schema
//! (`record_space_uuid`). The logical size of a record is the length of that
//! stored value: the encoded `StoredRecord` that the write path puts
//! (`RecordStore::stage_records`).
//!
//! A tombstone is not counted. A delete leaves a tombstone at the record key,
//! so the record count and bytes drop after a delete.

use super::{sort_rows, SchemaStorage, StorageBreakdown};
use crate::db_operations::admin_db::{inventory_next_page_rows, INVENTORY_PAGE_ROWS_MIN};
use crate::db_operations::record_store::RangeWalk;
use crate::db_operations::RecordStore;
use crate::schema::types::field::{build_storage_key, FilterUtils};
use crate::schema::SchemaError;
use lastdb_model::record_space::{record_space_uuid, RECORD_PREFIX};
use lastdb_model::record_value::StoredRecord;
use std::collections::HashMap;

/// Live records and their stored value bytes, by row name.
type RecordTotals = HashMap<String, (u64, u64)>;

impl RecordStore {
    /// Add the live records of a format 7 home to `breakdown`, one row per
    /// schema.
    ///
    /// One paged walk reads the whole record plane under `storage_prefix`, so
    /// the cost is O(all records). `schema_names` maps
    /// each record space back to its schema: it names the rows, it does not
    /// narrow the walk. A space that no listed schema owns keeps its `rk:{S}`
    /// prefix as the row name, so the total stays complete.
    pub(crate) async fn add_record_storage(
        &self,
        breakdown: &mut StorageBreakdown,
        schema_names: &[String],
        storage_prefix: Option<&str>,
    ) -> Result<(), SchemaError> {
        let spaces: HashMap<String, &str> = schema_names
            .iter()
            .map(|name| (record_space_uuid(name), name.as_str()))
            .collect();
        let start = build_storage_key(storage_prefix, RECORD_PREFIX);
        let end = FilterUtils::create_prefix_end(&start)?;
        let mut walk = RangeWalk::new(start.as_bytes(), end.as_bytes(), INVENTORY_PAGE_ROWS_MIN);
        let mut totals = RecordTotals::new();
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
        Ok(())
    }
}

/// Count one stored record under its schema. `rest` is the key after the
/// `rk:` prefix: `{S}:{esc(hash)}\0{range}`.
fn note_record_row(
    totals: &mut RecordTotals,
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

impl StorageBreakdown {
    /// Merge record totals into the rows, one row for each schema. The bytes
    /// of a row join `bytes` and the logical total.
    fn add_records(&mut self, totals: RecordTotals) {
        for (schema_name, (count, bytes)) in totals {
            let at = self
                .per_schema
                .iter()
                .position(|row| row.schema_name == schema_name);
            let row = if let Some(at) = at {
                &mut self.per_schema[at]
            } else {
                self.per_schema.push(SchemaStorage {
                    schema_name,
                    display_name: None,
                    bytes: 0,
                    record_count: 0,
                    record_bytes: 0,
                });
                self.per_schema.last_mut().expect("the row was just pushed")
            };
            row.record_count += count;
            row.record_bytes += bytes;
            row.bytes += bytes;
            self.total_logical_bytes += bytes;
        }
        sort_rows(&mut self.per_schema);
    }
}
