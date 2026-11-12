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

pub use record_store::{
    RecordDeleteLedgerEntry, RecordStore, WriterWatch, LEDGER_VERB_DELETE, LEDGER_VERB_PURGE,
};
pub use schema_store::SchemaStore;
pub use search_index::{IndexChange, IndexChangeBatch, IndexChangeKind};

use crate::error::FoldDbError;
use crate::schema::schema_types::SchemaState;
use crate::schema::types::{KeyValue, Schema, SchemaError};
use crate::storage::disk_pack::{PackReadScope, PackRuntime};
use crate::storage::error::StorageResult;
use crate::storage::format::metadata::Metadata;
use chrono::{DateTime, Utc};
use futures::{future::BoxFuture, FutureExt};
use lastdb_model::record_space::record_space_uuid;
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::Arc;

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

/// Database operations coordinator over typed domain stores.
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

/// Where one resumable page ended when key-examination budget ran out early.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PageEnd {
    pub resume: Option<PageResume>,
}

/// Position after which the next page of a resumable read starts.
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
