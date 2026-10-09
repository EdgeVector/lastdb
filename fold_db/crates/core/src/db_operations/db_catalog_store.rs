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
}

impl DbCatalogStore {
    pub(crate) fn new(entries: Arc<TypedKvStore<dyn KvStore>>) -> Self {
        Self { entries }
    }

    /// Insert or replace one exact `(database, schema)` reference.
    pub async fn put(&self, entry: &DbCatalogEntry) -> StorageResult<()> {
        validate_entry(entry)?;
        self.entries
            .put_item(&entry_key(&entry.db_locator, &entry.schema_name), entry)
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
        self.entries
            .get_item(&entry_key(db_locator, schema_name))
            .await
    }

    /// Insert membership for a named locator if absent. An existing row wins
    /// so re-declare does not mint a second instance.
    pub async fn ensure_named_membership(
        &self,
        db_locator: &str,
        schema_name: &str,
        instance_id: Option<String>,
    ) -> StorageResult<DbCatalogEntry> {
        if let Some(existing) = self.get(db_locator, schema_name).await? {
            return Ok(existing);
        }
        let entry = DbCatalogEntry {
            db_locator: db_locator.to_string(),
            schema_name: schema_name.to_string(),
            instance_id,
            key_selection: DbCatalogKeySelection::WholeSchema,
        };
        self.put(&entry).await?;
        Ok(entry)
    }

    /// Delete one exact catalog reference.
    pub async fn delete(&self, db_locator: &str, schema_name: &str) -> StorageResult<bool> {
        validate_identity(db_locator, schema_name)?;
        let deleted = self
            .entries
            .delete_item(&entry_key(db_locator, schema_name))
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
        Ok(self
            .entries
            .scan_items_with_prefix("")
            .await?
            .into_iter()
            .map(|(_key, entry)| entry)
            .collect())
    }
}

/// Instance ids currently referenced by catalog entries.
pub fn referenced_instance_ids(entries: &[DbCatalogEntry]) -> std::collections::HashSet<String> {
    entries
        .iter()
        .filter_map(|entry| entry.instance_id.clone())
        .collect()
}

/// Drop leftover `{64hex}:` copy-geometry rows whose prefix is not a live catalog instance.
pub async fn drop_unreferenced_prefixed_rows(
    main: &dyn crate::storage::traits::KvStore,
    live_instances: &std::collections::HashSet<String>,
) -> StorageResult<usize> {
    let rows = main.scan_prefix(b"").await?;
    let mut deleted = 0usize;
    for (key, _) in rows {
        let Some(prefix) = leftover_db_hash_prefix(&key) else {
            continue;
        };
        if live_instances.contains(prefix) {
            continue;
        }
        main.delete(&key).await?;
        deleted += 1;
    }
    Ok(deleted)
}

/// Drop every main-store key for one instance prefix (`{instance_id}:…`).
pub async fn drop_prefixed_rows_for_instance(
    main: &dyn crate::storage::traits::KvStore,
    instance_id: &str,
) -> StorageResult<usize> {
    if instance_id == UNPREFIXED_INSTANCE_ID || instance_id.is_empty() {
        return Ok(0);
    }
    let prefix = format!("{instance_id}:");
    let rows = main.scan_prefix(prefix.as_bytes()).await?;
    let mut deleted = 0usize;
    for (key, _) in rows {
        main.delete(&key).await?;
        deleted += 1;
    }
    Ok(deleted)
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
