//! Schema domain store.
//!
//! Providers read through restricted views and supply typed values to the
//! format-owned writer. Callers use `DbOperations::schemas()`.

use crate::schema::{Schema, SchemaError, SchemaState};
use crate::storage::field_key_store::StoredPayloadFieldKeys;
use crate::storage::format::read::ReadStore;
use crate::storage::format::schemas::{
    schema_input, SchemaFormat, StateChange, StateDelete, StateKey, Tracking,
};
use serde::de::DeserializeOwned;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

mod catalog_keys;
mod claims_retention;
mod drop_tracking;
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
mod retention_partitions;
mod retention_recheck;
mod retention_sharing;
mod write;

pub(crate) use native_memory::{schema_bytes, snapshot_bytes, snapshot_sources};
pub(crate) use read_snapshot::SchemaReadSnapshot;
pub use retention_floors::RetentionFloors;

use catalog_keys::*;

/// Schemas loaded for open, plus the retired payload-field ids on each raw row.
pub(crate) struct OpenCatalog {
    pub(crate) schemas: HashMap<String, Schema>,
    pub(crate) stored_field_key_ids: StoredPayloadFieldKeys,
}

/// Domain store for schema-related persistence.
#[derive(Clone)]
pub struct SchemaStore {
    schemas_store: ReadStore,
    schema_states_store: ReadStore,
    format: SchemaFormat,
    runtime: Arc<crate::storage::disk_pack::PackRuntime>,
    /// The floors of the TTL sweep, in process memory (`retention_floors.rs`).
    retention_floors: Arc<RetentionFloors>,
}

impl SchemaStore {
    pub(crate) fn new(format: SchemaFormat) -> Self {
        let (schemas_store, schema_states_store) = format.readers();
        let runtime = format.runtime();
        Self {
            schemas_store,
            schema_states_store,
            format,
            runtime,
            retention_floors: Arc::default(),
        }
    }

    /// Flush every schema-owned namespace to durable storage.
    pub(crate) async fn flush(&self) -> Result<(), SchemaError> {
        self.format.flush_schemas().await?;
        self.format.flush_states().await?;
        Ok(())
    }

    /// Get all schemas.
    ///
    /// The read is strict: a row that does not decode, or whose payload names
    /// another schema, fails the read.
    pub async fn get_all_schemas(&self) -> Result<HashMap<String, Schema>, SchemaError> {
        Ok(self.load_open_catalog().await?.schemas)
    }

    /// Load every schema and the retired payload-field ids still stored on its row.
    ///
    /// The ids come from the raw catalog bytes. The decoded schema no longer
    /// has that map. Open uses them to delete the old bundles before anything
    /// rewrites the row.
    pub(crate) async fn load_open_catalog(&self) -> Result<OpenCatalog, SchemaError> {
        let rows = read_catalog_bytes(
            &self.schemas_store,
            crate::storage::format::SCHEMAS_NAMESPACE,
            b"",
            CATALOG_SCAN_END,
        )
        .await?;
        let mut schemas = HashMap::with_capacity(rows.len());
        let mut stored_field_key_ids = StoredPayloadFieldKeys::default();
        for (key, bytes) in rows {
            let mut schema = decode_catalog_json::<Schema>(&bytes).map_err(|error| {
                SchemaError::InvalidData(format!(
                    "schemas catalog row {key} is unreadable: {error}"
                ))
            })?;
            if schema.name != key {
                return Err(SchemaError::InvalidData(format!(
                    "schema catalog row {key} names another schema: {}",
                    schema.name
                )));
            }
            let ids = stored_payload_field_key_ids(&bytes).map_err(|error| {
                SchemaError::InvalidData(format!(
                    "schema catalog row {key} field_key_ids is unreadable: {error}"
                ))
            })?;
            schema.populate_runtime_fields().map_err(|error| {
                SchemaError::InvalidData(format!(
                    "schema catalog row {key} does not resolve: {error}"
                ))
            })?;
            stored_field_key_ids.insert(key.clone(), ids);
            schemas.insert(key, schema);
        }
        Ok(OpenCatalog {
            schemas,
            stored_field_key_ids,
        })
    }

    /// Get all schema states.
    ///
    /// The node-local families that share this namespace (name claims and
    /// retention rows) sit under keys that start with a zero byte, so the read
    /// starts above them.
    pub async fn get_all_schema_states(&self) -> Result<HashMap<String, SchemaState>, SchemaError> {
        let rows = read_catalog_range::<SchemaState>(
            &self.schema_states_store,
            crate::storage::format::SCHEMA_STATES_NAMESPACE,
            CATALOG_STATE_SCAN_START,
            CATALOG_SCAN_END,
        )
        .await?;
        Ok(rows.into_iter().collect())
    }
}

/// Decode every row of one catalog namespace whose key falls in `start..end`.
///
/// A row that does not decode fails the read. The page walk lives in
/// [`read_catalog_bytes`].
async fn read_catalog_range<T: DeserializeOwned>(
    store: &ReadStore,
    catalog: &'static str,
    start: &[u8],
    end: &[u8],
) -> Result<Vec<(String, T)>, SchemaError> {
    let rows = read_catalog_bytes(store, catalog, start, end).await?;
    let mut out = Vec::with_capacity(rows.len());
    for (key, bytes) in rows {
        let value = decode_catalog_json::<T>(&bytes).map_err(|error| {
            SchemaError::InvalidData(format!(
                "{catalog} catalog row {key} is unreadable: {error}"
            ))
        })?;
        out.push((key, value));
    }
    Ok(out)
}

/// Read every raw row of one catalog namespace whose key falls in `start..end`.
///
/// The read walks the range in bounded pages, so it has no single unbounded
/// scan. A page that the backend returns out of order, or short while rows
/// remain, fails the read. The bytes stay intact so open can read a key the
/// schema decoder ignores.
async fn read_catalog_bytes(
    store: &ReadStore,
    catalog: &'static str,
    start: &[u8],
    end: &[u8],
) -> Result<Vec<(String, Vec<u8>)>, SchemaError> {
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
            out.push((key, value_bytes));
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

/// The old `field_key_ids` object on one catalog row. Absent and null are empty.
fn stored_payload_field_key_ids(bytes: &[u8]) -> Result<BTreeMap<String, String>, String> {
    let value: serde_json::Value =
        serde_json::from_slice(bytes).map_err(|error| error.to_string())?;
    let Some(ids) = value.get("field_key_ids") else {
        return Ok(BTreeMap::new());
    };
    if ids.is_null() {
        return Ok(BTreeMap::new());
    }
    let Some(object) = ids.as_object() else {
        return Err("field_key_ids is not a map".into());
    };
    let mut out = BTreeMap::new();
    for (name, id) in object {
        let Some(id) = id.as_str() else {
            return Err(format!("field_key_ids.{name} is not a string"));
        };
        out.insert(name.clone(), id.to_owned());
    }
    Ok(out)
}
