//! Schema domain store.
//!
//! Owns the storage namespaces for schemas and schema states.
//! External callers access schema
//! operations through this type via `DbOperations::schemas()`.

use crate::schema::{Schema, SchemaError, SchemaState};
use crate::storage::traits::KvStore;
use crate::storage::TypedKvStore;
use serde::de::DeserializeOwned;
use std::collections::HashMap;
use std::sync::Arc;

mod catalog_keys;
mod claims_retention;
mod key_format;
mod name_claims;
mod native_memory;
mod read;
mod read_snapshot;
mod retention_backfill;
mod retention_floors;
mod retention_format;
mod retention_layers;
mod retention_move;
mod retention_page;
mod retention_recheck;
mod retention_sharing;
mod write;

pub(crate) use native_memory::{schema_bytes, snapshot_bytes, snapshot_sources};
pub(crate) use read_snapshot::SchemaReadSnapshot;
pub use retention_floors::RetentionFloors;

use catalog_keys::*;

/// Domain store for schema-related persistence.
#[derive(Clone)]
pub struct SchemaStore {
    schemas_store: Arc<TypedKvStore<dyn KvStore>>,
    schema_states_store: Arc<TypedKvStore<dyn KvStore>>,
    runtime: Arc<crate::storage::disk_pack::PackRuntime>,
    /// The floors of the TTL sweep, in process memory (`retention_floors.rs`).
    retention_floors: Arc<RetentionFloors>,
}

impl SchemaStore {
    pub(crate) fn new(
        schemas_store: Arc<TypedKvStore<dyn KvStore>>,
        schema_states_store: Arc<TypedKvStore<dyn KvStore>>,
        runtime: Arc<crate::storage::disk_pack::PackRuntime>,
    ) -> Self {
        Self {
            schemas_store,
            schema_states_store,
            runtime,
            retention_floors: Arc::default(),
        }
    }

    /// Flush every schema-owned namespace to durable storage.
    pub(crate) async fn flush(&self) -> Result<(), SchemaError> {
        self.schemas_store.inner().flush().await?;
        self.schema_states_store.inner().flush().await?;
        Ok(())
    }

    /// Get all schemas.
    ///
    /// The read is strict: a row that does not decode, or whose payload names
    /// another schema, fails the read.
    pub async fn get_all_schemas(&self) -> Result<HashMap<String, Schema>, SchemaError> {
        let rows =
            read_catalog_range::<Schema>(&self.schemas_store, "schemas", b"", CATALOG_SCAN_END)
                .await?;
        let mut schemas = HashMap::with_capacity(rows.len());
        for (key, mut schema) in rows {
            if schema.name != key {
                return Err(SchemaError::InvalidData(format!(
                    "schema catalog row {key} names another schema: {}",
                    schema.name
                )));
            }
            schema.populate_runtime_fields().map_err(|error| {
                SchemaError::InvalidData(format!(
                    "schema catalog row {key} does not resolve: {error}"
                ))
            })?;
            schemas.insert(key, schema);
        }
        Ok(schemas)
    }

    /// Get all schema states.
    ///
    /// The node-local families that share this namespace (name claims and
    /// retention rows) sit under keys that start with a zero byte, so the read
    /// starts above them.
    pub async fn get_all_schema_states(&self) -> Result<HashMap<String, SchemaState>, SchemaError> {
        let rows = read_catalog_range::<SchemaState>(
            &self.schema_states_store,
            "schema_states",
            CATALOG_STATE_SCAN_START,
            CATALOG_SCAN_END,
        )
        .await?;
        Ok(rows.into_iter().collect())
    }
}

/// Read every row of one catalog namespace whose key falls in `start..end`.
///
/// The read walks the range in bounded pages, so it has no single unbounded
/// scan. It is strict: a row that does not decrypt or decode fails the read,
/// and so does a page that the backend returns out of order, or short while
/// rows remain.
async fn read_catalog_range<T: DeserializeOwned>(
    store: &TypedKvStore<dyn KvStore>,
    catalog: &'static str,
    start: &[u8],
    end: &[u8],
) -> Result<Vec<(String, T)>, SchemaError> {
    let mut cursor = start.to_vec();
    let mut out = Vec::new();
    loop {
        let page = store
            .inner()
            .scan_range_paged(&cursor, end, CATALOG_PAGE_ROWS)
            .await
            .map_err(|error| {
                SchemaError::InvalidData(format!("read the {catalog} catalog: {error}"))
            })?;
        if page.is_empty() {
            break;
        }
        if page.len() > CATALOG_PAGE_ROWS
            || page.windows(2).any(|pair| pair[0].0 >= pair[1].0)
            || page
                .iter()
                .any(|(key, _)| key.as_slice() < cursor.as_slice() || key.as_slice() >= end)
        {
            return Err(SchemaError::InvalidData(format!(
                "the {catalog} catalog read received an invalid or unordered page"
            )));
        }
        let full = page.len() == CATALOG_PAGE_ROWS;
        let mut next = page.last().expect("non-empty page").0.clone();
        next.push(0);
        for (key_bytes, value_bytes) in page {
            let key = String::from_utf8(key_bytes).map_err(|error| {
                SchemaError::InvalidData(format!(
                    "the {catalog} catalog has a non-UTF-8 key: {error}"
                ))
            })?;
            let value = decode_catalog_json::<T>(&value_bytes).map_err(|error| {
                SchemaError::InvalidData(format!(
                    "{catalog} catalog row {key} is unreadable: {error}"
                ))
            })?;
            out.push((key, value));
        }
        if !full {
            let trailing = store.inner().scan_range_paged(&next, end, 1).await?;
            if !trailing.is_empty() {
                return Err(SchemaError::InvalidData(format!(
                    "the {catalog} catalog read received a truncated page"
                )));
            }
            break;
        }
        cursor = next;
    }
    Ok(out)
}
