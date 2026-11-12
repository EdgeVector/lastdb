//! Database operations coordinator, storage reports, schema lifecycle, and page scopes.

mod core {
    mod open;
}
pub mod record_store;
pub mod schema_store;
pub mod search_index;

pub mod admin_db {
    pub use super::{SchemaRecordKey, SchemaRecordKeysReport, SchemaStorage, SchemaStorageReport};
}

pub use record_store::{RecordDeleteLedgerEntry, RecordStore, WriterWatch};
pub use schema_store::SchemaStore;
pub use search_index::{IndexChange, IndexChangeBatch, IndexChangeKind};

use crate::error::FoldDbError;
use crate::schema::schema_types::SchemaState;
use crate::schema::types::{field::FilterUtils, KeyValue, Schema, SchemaError};
use crate::storage::disk_pack::{PackReadScope, PackRuntime};
use crate::storage::error::StorageResult;
use crate::storage::format::metadata::Metadata;
use chrono::{DateTime, Utc};
use futures::{future::BoxFuture, FutureExt};
use lastdb_model::record_space::{record_space_uuid, RECORD_PREFIX};
use lastdb_model::record_value::StoredRecord;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::Arc;

impl RecordDeleteLedgerEntry {
    pub fn erasure(verb: &str, caller: &str, schema: &str, described_key: &str) -> Self {
        let mut h = Sha256::new();
        h.update(schema.as_bytes());
        h.update([0u8]);
        h.update(described_key.as_bytes());
        Self {
            verb: verb.to_string(),
            at: Utc::now().to_rfc3339(),
            caller: caller.to_string(),
            schema: schema.to_string(),
            key_fingerprint: format!("{:x}", h.finalize()),
            records_erased: 0,
            file_records_erased: 0,
        }
    }
}

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

#[derive(Clone)]
pub struct DbOperations {
    runtime: Arc<PackRuntime>,
    schema_store: SchemaStore,
    record_store: RecordStore,
    metadata: Metadata,
}

impl DbOperations {
    pub fn schemas(&self) -> &SchemaStore {
        &self.schema_store
    }

    pub fn records(&self) -> &RecordStore {
        &self.record_store
    }

    pub async fn read_scope(&self) -> StorageResult<PackReadScope> {
        PackReadScope::for_runtime(&self.runtime).await
    }

    pub(crate) fn metadata(&self) -> &Metadata {
        &self.metadata
    }

    pub fn native_runtime(&self) -> Arc<PackRuntime> {
        Arc::clone(&self.runtime)
    }

    #[must_use]
    pub fn batch_operation_limit(&self) -> usize {
        self.runtime.batch_operation_limit()
    }

    pub async fn flush(&self) -> Result<(), SchemaError> {
        self.schema_store.flush().await?;
        self.record_store.flush().await?;
        self.metadata.flush().await?;
        Ok(())
    }

    pub fn store_schema<'a>(
        &'a self,
        schema_name: &'a str,
        schema: &'a Schema,
    ) -> BoxFuture<'a, Result<(), SchemaError>> {
        crate::storage::disk_pack::transaction::scope(self.native_runtime(), async move {
            schema
                .validate_definition()
                .map_err(SchemaError::InvalidData)?;
            let next = schema_field_key_profiles(schema)?;
            if let Some(old) = self.schemas().get_schema(schema_name).await? {
                let prev: HashMap<_, _> = schema_field_key_profiles(&old)?.into_iter().collect();
                if next
                    .iter()
                    .any(|(k, p)| prev.get(k).is_some_and(|s| s != p))
                {
                    return Err(SchemaError::InvalidData(
                        "the schema range type differs from the stored field key profile".into(),
                    ));
                }
            }
            self.schemas().store_schema(schema_name, schema).await
        })
        .boxed()
    }

    pub fn drop_schema<'a>(
        &'a self,
        schema_name: &'a str,
    ) -> BoxFuture<'a, Result<bool, SchemaError>> {
        async move {
            if let Some(schema) = self.block_schema_for_drop(schema_name).await? {
                let released = self.records().release_schema_rows(&schema).await?;
                tracing::info!(
                    target: "fold_node::database", schema = schema_name,
                    records = released.records, alternates = released.alternates,
                    "schema drop released the stored rows of the schema"
                );
            }
            self.schemas().drop_schema(schema_name).await
        }
        .boxed()
    }

    pub async fn block_schema_for_drop(
        &self,
        schema_name: &str,
    ) -> Result<Option<Schema>, SchemaError> {
        crate::storage::disk_pack::transaction::scope(self.native_runtime(), async {
            let Some(schema) = self.schemas().get_schema(schema_name).await? else {
                return Ok(None);
            };
            if self.schemas().get_schema_state(schema_name).await? != Some(SchemaState::Blocked) {
                self.schemas()
                    .store_schema_state(schema_name, &SchemaState::Blocked)
                    .await?;
            }
            Ok(Some(schema))
        })
        .await
    }

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
                for alias in [
                    Some(key.as_str()),
                    Some(s.name.as_str()),
                    s.identity_hash.as_deref(),
                ]
                .into_iter()
                .flatten()
                {
                    display.insert(alias, label);
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
    let mut walk = record_store::RangeWalk::new(RECORD_PREFIX.as_bytes(), end.as_bytes(), 16);
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
            let rest = &key[RECORD_PREFIX.len()..];
            let space = rest
                .iter()
                .position(|&b| b == b':')
                .and_then(|i| std::str::from_utf8(&rest[..i]).ok())
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

fn schema_field_key_profiles(
    schema: &Schema,
) -> Result<Vec<(String, schema_types::field_range_profile::FieldRangeProfile)>, SchemaError> {
    let check = |id: String, msg: &'static str| {
        lastdb_model::field_key_id::is_canonical_field_key_id(&id)
            .then_some(id)
            .ok_or_else(|| SchemaError::InvalidData(msg.into()))
    };
    let defs = schema.require_record_keys()?;
    let mut out = Vec::with_capacity(defs.alternates.len() + 1);
    for (name, key) in &defs.alternates {
        let id = lastdb_model::field_key_id::alternate_field_key_id(schema, name);
        out.push((
            check(id, "invalid alternate field key identity")?,
            key.range_profile().map_err(SchemaError::InvalidData)?,
        ));
    }
    out.push((
        check(
            record_space_uuid(&schema.name),
            "invalid record space identity",
        )?,
        defs.primary
            .range_profile()
            .map_err(SchemaError::InvalidData)?,
    ));
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out.dedup();
    Ok(out)
}

tokio::task_local! {
    static PAGE_END: RefCell<PageEnd>;
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PageEnd {
    pub resume: Option<PageResume>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PageResume {
    Key(KeyValue),
}

pub fn page_can_resume() -> bool {
    PAGE_END.try_with(|_| ()).is_ok()
}

pub fn note_page_resume(resume: PageResume) {
    let _ = PAGE_END.try_with(|end| end.borrow_mut().resume = Some(resume));
}

pub async fn with_query_page_tally<F: std::future::Future>(fut: F) -> (F::Output, PageEnd) {
    PAGE_END
        .scope(RefCell::new(PageEnd::default()), async move {
            (fut.await, PAGE_END.with(RefCell::take))
        })
        .await
}
