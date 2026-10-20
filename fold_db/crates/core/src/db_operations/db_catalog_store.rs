//! Durable database catalog: `(database locator, schema) -> instance`.
//!
//! Databases are catalogs over the node-universal molecule/atom store. A
//! catalog entry selects an existing schema instance; it never copies data.
//! The exact-key layout keeps resolution O(1) and avoids any catalog scan.

use crate::storage::error::StorageResult;
use crate::storage::traits::KvStore;
use crate::storage::{StorageError, TypedKvStore};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::sync::Arc;
mod cloud_purge_membership;
mod memory;

/// Sentinel local storage prefix for the legacy unprefixed personal instance.
/// Catalog `instance_id` stays `None`; org-sync `storage_prefixes` uses this
/// token so unprefixed keys can route to an org cloud head.
pub const UNPREFIXED_INSTANCE_ID: &str = "unprefixed";

/// Reserved selection shape for future partial-schema sharing.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DbCatalogKeySelection {
    /// The whole schema instance is selected (v1 behavior).
    #[default]
    WholeSchema,
}

/// One database's reference to a schema instance in the universal store.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct DbCatalogEntry {
    pub db_locator: String,
    pub schema_name: String,
    /// Storage instance namespace. `None` references the legacy unprefixed
    /// personal instance without migrating or copying its molecules/atoms.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance_id: Option<String>,
    #[serde(default)]
    pub key_selection: DbCatalogKeySelection,
}

/// Exact-key database-catalog persistence.
#[derive(Clone)]
pub struct DbCatalogStore {
    entries: Arc<TypedKvStore<dyn KvStore>>,
    runtime: Option<Arc<crate::storage::disk_pack::PackRuntime>>,
}

impl DbCatalogStore {
    pub(crate) fn new(
        entries: Arc<TypedKvStore<dyn KvStore>>,
        runtime: Option<Arc<crate::storage::disk_pack::PackRuntime>>,
    ) -> Self {
        Self { entries, runtime }
    }

    /// Insert or replace one exact `(database, schema)` reference.
    pub async fn put(&self, entry: &DbCatalogEntry) -> StorageResult<()> {
        validate_entry(entry)?;
        self.put_membership_batch(std::slice::from_ref(entry))
            .await?;
        self.entries.inner().flush().await
    }

    /// Point-get one catalog reference. This never enumerates the catalog.
    pub async fn get(
        &self,
        db_locator: &str,
        schema_name: &str,
    ) -> StorageResult<Option<DbCatalogEntry>> {
        validate_identity(db_locator, schema_name)?;
        self.get_entry_from_memory(&entry_key(db_locator, schema_name))
            .await
    }

    /// Delete one exact catalog reference.
    pub async fn delete(&self, db_locator: &str, schema_name: &str) -> StorageResult<bool> {
        validate_identity(db_locator, schema_name)?;
        let key = entry_key(db_locator, schema_name);
        let deleted = self.entries.inner().get(key.as_bytes()).await?.is_some();
        let locator_hash = Sha256::digest(db_locator.as_bytes());
        self.entries
            .inner()
            .batch_delete(vec![
                key.into_bytes(),
                format!("membership:{locator_hash:x}\0{schema_name}").into_bytes(),
            ])
            .await?;
        self.entries.inner().flush().await?;
        Ok(deleted)
    }

    /// Resolve the storage instance for a request.
    ///
    /// An entry wins even when it selects `None`: that is how an org/database
    /// handle references legacy personal data without copying it. A named
    /// database with no entry fails closed; it must never fall back to the
    /// historical DB-hash prefix supplied by the request edge.
    pub async fn resolve_storage_prefix(
        &self,
        db_locator: Option<&str>,
        schema_name: &str,
        legacy_prefix: Option<&str>,
    ) -> StorageResult<Option<String>> {
        let Some(db_locator) = db_locator else {
            return Ok(legacy_prefix.map(str::to_owned));
        };
        self.get(db_locator, schema_name)
            .await?
            .map(|entry| entry.instance_id)
            .ok_or_else(|| StorageError::CatalogMembershipDenied {
                db_locator: db_locator.to_string(),
                schema_name: schema_name.to_string(),
            })
    }

    pub(crate) async fn flush(&self) -> StorageResult<()> {
        self.entries.inner().flush().await
    }

    /// Hygiene-only: enumerate catalog entries. Not a data-plane read.
    pub async fn list_entries(&self) -> StorageResult<Vec<DbCatalogEntry>> {
        let entries = self
            .entries
            .scan_items_with_prefix("")
            .await?
            .into_iter()
            .filter(|(key, _)| key.starts_with("v1:"))
            .map(|(_key, entry)| entry)
            .collect();
        Ok(entries)
    }
}

/// Instance ids currently referenced by catalog entries.
pub fn referenced_instance_ids(entries: &[DbCatalogEntry]) -> std::collections::HashSet<String> {
    entries
        .iter()
        .filter_map(|entry| entry.instance_id.clone())
        .collect()
}

/// Remove one current database scope. The blob counters that the scope's atom
/// counters and tips held go down in the same batch as the rows that held them.
pub async fn drop_prefixed_rows_for_instance(
    atoms: &super::AtomStore,
    main: &dyn crate::storage::traits::KvStore,
    instance_id: &str,
) -> StorageResult<usize> {
    if instance_id == UNPREFIXED_INSTANCE_ID || instance_id.is_empty() {
        return Ok(0);
    }
    let prefix = format!("{instance_id}:");
    let (start, end) = crate::kind_partition::exact_prefix_bounds(&prefix);
    let rows = current_scope_rows(main, start.as_bytes(), end.as_bytes()).await?;
    let mut selected = Vec::new();
    for (key, value) in rows {
        let Ok(text) = std::str::from_utf8(&key) else {
            continue;
        };
        if crate::storage::current_keys::exact_scope(text)? == Some(instance_id) {
            selected.push((key, value));
        }
    }
    drop_scoped_source_rows(atoms, main, selected).await
}

async fn current_scope_rows(
    main: &dyn crate::storage::traits::KvStore,
    start: &[u8],
    end: &[u8],
) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
    let mut rows = Vec::new();
    let mut cursor: Option<crate::storage::traits::PhysicalScanCursor> = None;
    loop {
        let page = main
            .scan_range_physical_paged(start, end, cursor.as_ref(), 128, 1)
            .await?;
        rows.extend(page.rows);
        let Some(next) = page.next_cursor else { break };
        if cursor.as_ref() == Some(&next) {
            return Err(StorageError::BackendError(
                "current scope walk did not advance".into(),
            ));
        }
        cursor = Some(next);
    }
    Ok(rows)
}

/// Rows of one scope that one native batch deletes with the blob counter changes
/// of the same rows. A chunk stays far below the batch limit of an older home.
const DROP_CHUNK: usize = 512;

/// Delete the rows of a dropped scope, chunk by chunk. The caller read the rows
/// without a gate, so a writer can change them before a chunk runs. A chunk reads
/// its rows again inside the write gate and acts on what it finds: a row that is
/// gone (a writer deleted it, and lowered its blob counters in that batch) lowers
/// nothing again, and a row that changed lowers what it holds now. A row that a
/// writer added after the walk is not in the chunk and stays, as before.
async fn drop_scoped_source_rows(
    atoms: &super::AtomStore,
    main: &dyn crate::storage::traits::KvStore,
    rows: Vec<(Vec<u8>, Vec<u8>)>,
) -> StorageResult<usize> {
    let removed = rows.len();
    let backend = |error: crate::schema::SchemaError| StorageError::BackendError(error.to_string());
    for chunk in rows.chunks(DROP_CHUNK) {
        atoms
            .in_write_scope(async {
                let store_error = |error: StorageError| {
                    crate::schema::SchemaError::InvalidData(format!("drop scope rows: {error}"))
                };
                let keys: Vec<Vec<u8>> = chunk.iter().map(|(key, _)| key.clone()).collect();
                let values = main.get_many(keys.clone()).await.map_err(store_error)?;
                let current: Vec<(Vec<u8>, Vec<u8>)> = keys
                    .into_iter()
                    .zip(values)
                    .filter_map(|(key, value)| value.map(|value| (key, value)))
                    .collect();
                let mut deltas = super::atom_store::blob_counter::BlobDeltas::default();
                let unreadable = deltas.scope_rows_dropped(&current);
                if unreadable > 0 {
                    tracing::warn!(
                        unreadable,
                        "rows of a dropped scope do not decode; their blob counters stay too high until a recount"
                    );
                }
                let blob_writes = atoms.blob_counter_mutations(deltas).await?;
                let keys = current.into_iter().map(|(key, _)| key).collect();
                main.batch_delete(keys).await.map_err(store_error)?;
                if !blob_writes.is_empty() {
                    main.batch_mutate(blob_writes).await.map_err(store_error)?;
                }
                Ok(())
            })
            .await
            .map_err(backend)?;
    }
    Ok(removed)
}

pub fn leftover_db_hash_prefix(key: &[u8]) -> Option<&str> {
    let text = std::str::from_utf8(key).ok()?;
    let (prefix, rest) = text.split_once(':')?;
    if rest.is_empty() {
        return None;
    }
    if prefix.len() == 64 && prefix.chars().all(|c| c.is_ascii_hexdigit()) {
        Some(prefix)
    } else {
        None
    }
}

fn validate_entry(entry: &DbCatalogEntry) -> StorageResult<()> {
    validate_identity(&entry.db_locator, &entry.schema_name)?;
    if entry.instance_id.as_deref().is_some_and(str::is_empty) {
        return Err(StorageError::InvalidOperation(
            "database catalog instance_id must be absent or non-empty".to_string(),
        ));
    }
    if let Some(scope) = entry.instance_id.as_deref() {
        crate::storage::current_keys::validate_scope(scope)?;
    }
    Ok(())
}

fn validate_identity(db_locator: &str, schema_name: &str) -> StorageResult<()> {
    if db_locator.trim().is_empty() || schema_name.trim().is_empty() {
        return Err(StorageError::InvalidOperation(
            "database catalog requires non-empty db_locator and schema_name".to_string(),
        ));
    }
    Ok(())
}

fn entry_key(db_locator: &str, schema_name: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(db_locator.as_bytes());
    hasher.update([0]);
    hasher.update(schema_name.as_bytes());
    format!("v1:{:x}", hasher.finalize())
}
