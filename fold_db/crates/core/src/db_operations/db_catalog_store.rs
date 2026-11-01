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
mod membership;
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
    records: &super::RecordStore,
    main: &dyn crate::storage::traits::KvStore,
    instance_id: &str,
) -> StorageResult<usize> {
    if instance_id == UNPREFIXED_INSTANCE_ID || instance_id.is_empty() {
        return Ok(0);
    }
    let prefix = format!("{instance_id}:");
    let (start, end) = lastdb_model::kind_partition::exact_prefix_bounds(&prefix);
    let rows = current_scope_rows(main, start.as_bytes(), end.as_bytes()).await?;
    let mut selected = Vec::new();
    for (key, value) in rows {
        let Ok(text) = std::str::from_utf8(&key) else {
            continue;
        };
        if crate::storage::current_keys::exact_scope(text)? == Some(instance_id) {
            selected.push((instance_id.to_string(), key, value));
        }
    }
    drop_scoped_source_rows(records, main, selected).await
}

async fn current_scope_rows(
    main: &dyn crate::storage::traits::KvStore,
    start: &[u8],
    end: &[u8],
) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
    let mut rows = Vec::new();
    let mut bytes = 0usize;
    let mut cursor: Option<crate::storage::traits::PhysicalScanCursor> = None;
    loop {
        let page = main
            .scan_range_physical_paged(start, end, cursor.as_ref(), 128, 1)
            .await?;
        for (key, value) in page.rows {
            // Planning must fit the complete native write budget before it
            // retains another row body. Refusal leaves the entire scope intact.
            bytes = bytes
                .checked_add(160 + 2 * key.len() + value.len())
                .filter(|bytes| *bytes <= 128 * 1024 * 1024 - 256)
                .ok_or_else(|| StorageError::MutationLimit {
                    detail: "complete database cleanup plan exceeds 128 MiB".into(),
                })?;
            rows.push((key, value));
        }
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

/// Delete the rows of a dropped scope, and lower the blob counters that the
/// rows held. The caller runs in the one native write scope of the catalog
/// delete (`FoldDB::delete_catalog_membership`), and it read the rows inside that
/// scope, so a writer cannot change a row between the read and the delete: the
/// blob counters go down once for each row that goes. The blob counter changes
/// join the same batch as the deletes. A scope too large for one native batch is
/// refused before anything changes.
async fn drop_scoped_source_rows(
    records: &super::RecordStore,
    main: &dyn crate::storage::traits::KvStore,
    rows: Vec<(String, Vec<u8>, Vec<u8>)>,
) -> StorageResult<usize> {
    let removed = rows.len();
    let mut deltas = super::record_store::blob_counter::BlobDeltas::default();
    let unreadable =
        deltas.scope_rows_dropped(rows.iter().map(|(_, key, value)| (&key[..], &value[..])));
    if unreadable > 0 {
        tracing::warn!(
            unreadable,
            "rows of a dropped scope do not decode; their blob counters stay too high until a recount"
        );
    }
    let blob_writes = records
        .blob_counter_mutations(deltas)
        .await
        .map_err(|error| StorageError::BackendError(error.to_string()))?;
    // The catalog's outer native scope stages the deletes. A refusal or process
    // stop before publication retains the rows, the counters and the catalog row.
    for chunk in rows.chunks(2_048) {
        main.batch_delete(chunk.iter().map(|(_, key, _)| key.clone()).collect())
            .await?;
    }
    for chunk in blob_writes.chunks(1_024) {
        main.batch_mutate(chunk.to_vec()).await?;
    }
    Ok(removed)
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
