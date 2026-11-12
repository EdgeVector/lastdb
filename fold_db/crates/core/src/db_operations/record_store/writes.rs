//! Record writes, warm-set point reads, storage inventory, schema release, and delete ledger.

use super::{RangeWalk, RecordStore};
use crate::db_operations::SchemaStorage;
use crate::error::FoldDbError;
use crate::schema::types::field::FilterUtils;
use crate::schema::types::{FileWrite, KeyValue, Schema, SchemaError};
use crate::storage::format::records::{
    self as format, release_families, RecordAddress, ReleaseKind,
};
use chrono::Utc;
use futures::FutureExt;
use lastdb_model::kind_partition::exact_prefix_bounds;
use lastdb_model::record_space::{record_space_uuid, RECORD_PREFIX};
use lastdb_model::record_value::StoredRecord;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::Arc;

const READ_CHUNK: usize = 10_000;
const INVENTORY_PAGE_BYTE_BUDGET: u64 = 8 * 1024 * 1024;
const INVENTORY_PAGE_ROWS_MIN: usize = 16;
const INVENTORY_PAGE_ROWS_MAX: usize = 4096;

pub const LEDGER_VERB_PURGE: &str = "purge";
pub const LEDGER_VERB_DELETE: &str = "delete";

pub(crate) use format::RecordChange;
pub use format::RecordDeleteLedgerEntry;

#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct SchemaRelease {
    pub(crate) records: u64,
    pub(crate) alternates: u64,
}

impl RecordDeleteLedgerEntry {
    pub fn erasure(verb: &str, caller: &str, schema: &str, described_key: &str) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(schema.as_bytes());
        hasher.update([0u8]);
        hasher.update(described_key.as_bytes());
        Self {
            verb: verb.to_string(),
            at: Utc::now().to_rfc3339(),
            caller: caller.to_string(),
            schema: schema.to_string(),
            key_fingerprint: format!("{:x}", hasher.finalize()),
            records_erased: 0,
            file_records_erased: 0,
        }
    }
}

impl RecordStore {
    pub async fn list_record_delete_ledger(
        &self,
        limit: usize,
    ) -> Result<Vec<RecordDeleteLedgerEntry>, SchemaError> {
        let prefix =
            lastdb_model::kind_partition::anchored(crate::storage::format::DELETE_AUDIT_FAMILY, "");
        let mut rows = self
            .reader()
            .scan_prefix(prefix.as_bytes())
            .await
            .map_err(|e| SchemaError::InvalidData(format!("delete ledger scan: {e}")))?;
        rows.sort_by(|a, b| a.0.cmp(&b.0));
        let take = if limit > 0 { limit } else { rows.len() };
        rows.into_iter()
            .take(take)
            .map(|(k, v)| {
                serde_json::from_slice(&v).map_err(|e| {
                    SchemaError::InvalidData(format!(
                        "delete ledger row {} is unreadable: {e}",
                        String::from_utf8_lossy(&k)
                    ))
                })
            })
            .collect()
    }

    pub(crate) async fn record_storage_breakdown(
        &self,
        schema_names: &[String],
    ) -> Result<(Vec<SchemaStorage>, u64), SchemaError> {
        let spaces: HashMap<String, &str> = schema_names
            .iter()
            .map(|n| (record_space_uuid(n), n.as_str()))
            .collect();
        let end = FilterUtils::create_prefix_end(RECORD_PREFIX)?;
        let mut walk = RangeWalk::new(
            RECORD_PREFIX.as_bytes(),
            end.as_bytes(),
            INVENTORY_PAGE_ROWS_MIN,
        );
        let mut totals = HashMap::<String, (u64, u64)>::new();
        while !walk.done() {
            let rows = self
                .walk_step(&mut walk)
                .await
                .map_err(|e| SchemaError::InvalidData(format!("record storage scan: {e}")))?;
            let mut page_bytes = 0u64;
            for (key, value) in &rows {
                page_bytes += (key.len() + value.len()) as u64;
                let record = StoredRecord::decode(value).map_err(|e| {
                    SchemaError::InvalidData(format!("record storage scan: a stored record: {e}"))
                })?;
                if !record.is_live() {
                    continue;
                }
                let rest = &key[RECORD_PREFIX.len()..];
                let space = rest
                    .iter()
                    .position(|&b| b == b':')
                    .and_then(|end| std::str::from_utf8(&rest[..end]).ok())
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
                    usize::try_from(INVENTORY_PAGE_BYTE_BUDGET / avg)
                        .unwrap_or(INVENTORY_PAGE_ROWS_MAX)
                        .clamp(INVENTORY_PAGE_ROWS_MIN, INVENTORY_PAGE_ROWS_MAX),
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

    pub(crate) async fn release_schema_rows(
        &self,
        schema: &Schema,
    ) -> Result<SchemaRelease, SchemaError> {
        let mut out = SchemaRelease::default();
        for family in release_families(schema)? {
            let (start, end) = exact_prefix_bounds(family.prefix());
            let mut walk = RangeWalk::new(
                start.as_bytes(),
                end.as_bytes(),
                crate::storage::format::SCHEMA_RELEASE_ROWS,
            );
            let mut count = 0;
            while !walk.done() {
                let rows = self.walk_step(&mut walk).await?;
                if !rows.is_empty() {
                    let keys = rows.into_iter().map(|(k, _)| k).collect();
                    count += self
                        .in_write_scope(self.format.release_schema_batch(keys, &family))
                        .await?;
                }
            }
            match family.kind() {
                ReleaseKind::Records => out.records += count,
                ReleaseKind::Alternates => out.alternates += count,
                ReleaseKind::AppliedDeletes | ReleaseKind::InlineFiles => {}
            }
        }
        Ok(out)
    }

    pub(crate) fn record_key(
        &self,
        schema: &Schema,
        key: &KeyValue,
    ) -> Result<String, SchemaError> {
        self.record_address(schema, key)
            .map(RecordAddress::into_string)
    }

    pub(crate) fn record_address(
        &self,
        schema: &Schema,
        key: &KeyValue,
    ) -> Result<RecordAddress, SchemaError> {
        RecordAddress::new(
            &self.primary_codec(schema)?,
            &record_space_uuid(&schema.name),
            key,
        )
    }

    pub(crate) async fn read_records<K: AsRef<str> + Sync>(
        &self,
        keys: &[K],
    ) -> Result<Vec<Option<Arc<StoredRecord>>>, SchemaError> {
        self.record_read_scope(
            async move {
                let mut warm = self
                    .runtime
                    .warm_records(keys)
                    .map_err(|e| SchemaError::InvalidData(format!("read records: {e}")))?;
                let mut out = warm
                    .as_mut()
                    .map_or_else(|| vec![None; keys.len()], |r| r.take_found());
                let misses: Vec<usize> = (0..keys.len()).filter(|&at| out[at].is_none()).collect();
                let mut admitted = Vec::new();
                for chunk in misses.chunks(READ_CHUNK) {
                    let req = chunk
                        .iter()
                        .map(|&at| keys[at].as_ref().as_bytes().to_vec())
                        .collect();
                    let rows = self
                        .main_store
                        .get_many(req)
                        .await
                        .map_err(|e| SchemaError::InvalidData(format!("read records: {e}")))?;
                    if rows.len() != chunk.len() {
                        return Err(SchemaError::InvalidData(
                            "the record read batch is incomplete".into(),
                        ));
                    }
                    for (&at, row) in chunk.iter().zip(rows) {
                        let Some(bytes) = row else { continue };
                        let record = Arc::new(super::reads::decode(&bytes)?);
                        if warm.is_some() {
                            admitted.push((at, Arc::clone(&record), bytes.len()));
                        }
                        out[at] = Some(record);
                    }
                }
                if let Some(read) = &warm {
                    self.runtime.admit_read_records(read, admitted);
                }
                Ok(out)
            }
            .boxed(),
        )
        .await
    }

    pub fn seal_record_file(
        &self,
        plaintext: &[u8],
        name: Option<String>,
        media_type: Option<String>,
    ) -> Result<FileWrite, FoldDbError> {
        self.format.seal_record_file(plaintext, name, media_type)
    }

    pub async fn reference_local_file(
        &self,
        path: &std::path::Path,
        name: Option<String>,
        media_type: Option<String>,
    ) -> Result<FileWrite, FoldDbError> {
        self.format
            .reference_local_file(path, name, media_type)
            .await
    }
}
