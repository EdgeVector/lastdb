use crate::db_operations::search_index::{IndexChange, IndexChangeBatch, IndexChangeKind};
use crate::db_operations::{
    drop_prefixed_rows_for_instance, referenced_instance_ids, DbCatalogEntry,
    SchemaRecordKeysRequest, UNPREFIXED_INSTANCE_ID,
};
use crate::error::FoldDbResult;
use crate::schema::types::{KeyValue, Schema, SchemaError};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

use super::FoldDB;

/// Catalog rows of one schema that a schema drop removes in one native batch. A row
/// costs about two operations for each field of the schema.
const CATALOG_ROWS_PER_BATCH: usize = 64;

/// Result of deleting one catalog membership and reclaiming a zero-ref instance.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct CatalogDeleteReclaimResult {
    pub deleted: bool,
    pub instance_id: Option<String>,
    pub reclaimed_rows: usize,
    /// Instance cleanup retains node-global molecule domain wraps.
    pub reclaimed_wraps: usize,
    pub search_tombstones: usize,
}

/// What `reclaim_unreferenced_instance` removed.
#[derive(Default)]
struct InstanceReclaim {
    rows: usize,
    search_tombstones: usize,
}

impl FoldDB {
    /// Delete one catalog membership and any now-unreferenced instance in one
    /// native batch. Complete-request limits refuse oversized cleanup before
    /// the catalog row, bindings, or data change.
    pub async fn delete_catalog_membership(
        &self,
        db_locator: &str,
        schema_name: &str,
    ) -> FoldDbResult<CatalogDeleteReclaimResult> {
        Ok(self
            .db_ops()
            .atoms()
            .in_write_scope(self.delete_catalog_membership_in_scope(db_locator, schema_name))
            .await?)
    }

    async fn delete_catalog_membership_in_scope(
        &self,
        db_locator: &str,
        schema_name: &str,
    ) -> Result<CatalogDeleteReclaimResult, SchemaError> {
        let catalog = self.db_ops().db_catalog();
        let existing = catalog.get(db_locator, schema_name).await?;
        let schema = self.schema_manager().get_schema_metadata(schema_name)?;
        let storage_prefix = existing
            .as_ref()
            .and_then(|entry| entry.instance_id.clone());
        let _molecule_guards = self
            .lock_schema_molecules(schema.as_ref(), storage_prefix.as_deref())
            .await;
        // The native gate precedes molecule locks, as it does for normal writes.
        // It also protects the last-reference decision from a concurrent bind.
        let atoms = self.db_ops().atoms();
        let deleted = catalog.delete(db_locator, schema_name).await?;
        if let (true, Some(_), Some(schema)) = (deleted, existing.as_ref(), schema.as_ref()) {
            atoms
                .unbind_catalog_molecules(
                    db_locator,
                    schema_name,
                    schema,
                    storage_prefix.as_deref(),
                )
                .await?;
        }
        let instance_id = existing.and_then(|entry| entry.instance_id);
        let reclaim = match instance_id.as_deref() {
            Some(instance) if instance != UNPREFIXED_INSTANCE_ID => {
                self.reclaim_unreferenced_instance(schema_name, schema.as_ref(), instance)
                    .await?
            }
            _ => InstanceReclaim::default(),
        };
        Ok(CatalogDeleteReclaimResult {
            deleted,
            instance_id,
            reclaimed_rows: reclaim.rows,
            reclaimed_wraps: 0,
            search_tombstones: reclaim.search_tombstones,
        })
    }

    /// Remove every catalog row of `schema_name` with the bindings of the row, and
    /// reclaim no instance. This is the catalog step of a schema drop. The molecule
    /// drain erases the keys of an instance once its last binding is gone, in small
    /// batches. The reclaim of `delete_catalog_membership` would delete the whole
    /// instance in one native request, and a large instance is over the budget of
    /// one request (the drop would then fail).
    ///
    /// Up to `CATALOG_ROWS_PER_BATCH` rows go in one native batch. A failure leaves
    /// the rows of the later groups in place, and the schema installed, so the drop
    /// can run again. Returns the number of rows removed.
    pub(super) async fn delete_catalog_rows_for_drop(
        &self,
        schema_name: &str,
    ) -> FoldDbResult<usize> {
        let rows: Vec<_> = self
            .db_ops()
            .db_catalog()
            .list_entries()
            .await
            .map_err(SchemaError::from)?
            .into_iter()
            .filter(|entry| entry.schema_name == schema_name)
            .collect();
        let mut removed = 0;
        for group in rows.chunks(CATALOG_ROWS_PER_BATCH) {
            removed += self
                .db_ops()
                .atoms()
                .in_write_scope(self.delete_catalog_rows_in_scope(schema_name, group))
                .await?;
        }
        Ok(removed)
    }

    async fn delete_catalog_rows_in_scope(
        &self,
        schema_name: &str,
        rows: &[DbCatalogEntry],
    ) -> Result<usize, SchemaError> {
        let catalog = self.db_ops().db_catalog();
        let schema = self.schema_manager().get_schema_metadata(schema_name)?;
        let mut removed = 0;
        for row in rows {
            let existing = catalog.get(&row.db_locator, schema_name).await?;
            let storage_prefix = existing.and_then(|entry| entry.instance_id);
            let _molecule_guards = self
                .lock_schema_molecules(schema.as_ref(), storage_prefix.as_deref())
                .await;
            if !catalog.delete(&row.db_locator, schema_name).await? {
                continue;
            }
            removed += 1;
            if let Some(schema) = schema.as_ref() {
                self.db_ops()
                    .atoms()
                    .unbind_catalog_molecules(
                        &row.db_locator,
                        schema_name,
                        schema,
                        storage_prefix.as_deref(),
                    )
                    .await?;
            }
        }
        Ok(removed)
    }

    /// Take the commit lock of every molecule the schema owns, in sorted
    /// order so concurrent deleters cannot deadlock.
    async fn lock_schema_molecules(
        &self,
        schema: Option<&Schema>,
        storage_prefix: Option<&str>,
    ) -> Vec<impl Sized> {
        let mut molecule_uuids: Vec<String> = schema
            .and_then(|schema| schema.field_molecule_uuids.as_ref())
            .into_iter()
            .flat_map(|molecules| molecules.values().cloned())
            .collect();
        molecule_uuids.sort_unstable();
        molecule_uuids.dedup();
        let mut guards = Vec::with_capacity(molecule_uuids.len());
        for molecule_uuid in &molecule_uuids {
            guards.push(
                self.db_ops()
                    .atoms()
                    .lock_molecule_commit(molecule_uuid, storage_prefix)
                    .await,
            );
        }
        guards
    }

    /// When no catalog entry references `instance` any more, drop its search
    /// entries and `{instance}:` rows in main. Molecule domain wraps are global;
    /// this instance-only proof cannot authorize their removal.
    async fn reclaim_unreferenced_instance(
        &self,
        schema_name: &str,
        schema: Option<&Schema>,
        instance: &str,
    ) -> Result<InstanceReclaim, SchemaError> {
        let catalog = self.db_ops().db_catalog();
        let live = referenced_instance_ids(&catalog.list_entries().await?);
        let mut reclaim = InstanceReclaim::default();
        if live.contains(instance) {
            return Ok(reclaim);
        }
        let search_changes = if let Some(schema) = schema {
            let batch = self
                .build_catalog_unshare_tombstones(schema_name, schema, instance)
                .await?;
            reclaim.search_tombstones = batch.changes.len();
            Some(batch)
        } else {
            None
        };
        let main = self
            .db_ops()
            .namespaced_store()
            .open_namespace("main")
            .await?;
        reclaim.rows =
            drop_prefixed_rows_for_instance(self.db_ops().atoms(), main.as_ref(), instance).await?;
        if let Some(batch) = search_changes.filter(|batch| !batch.changes.is_empty()) {
            self.publish_catalog_unshare_search(batch).await?;
        }
        Ok(reclaim)
    }

    /// Search sees removal only after the complete catalog cleanup is durable.
    async fn publish_catalog_unshare_search(
        &self,
        batch: IndexChangeBatch,
    ) -> Result<(), SchemaError> {
        let Some(runtime) = self.db_ops().namespaced_store().disk_pack_runtime() else {
            return self
                .mutation_manager
                .deliver_index_change_batch(&batch)
                .await;
        };
        let manager = std::sync::Arc::clone(&self.mutation_manager);
        crate::storage::disk_pack::transaction::after_commit(&runtime, async move {
            manager
                .deliver_index_change_batch(&batch)
                .await
                .map_err(|error| crate::storage::StorageError::BackendError(error.to_string()))
        })?;
        Ok(())
    }

    async fn build_catalog_unshare_tombstones(
        &self,
        schema_name: &str,
        schema: &Schema,
        instance: &str,
    ) -> Result<IndexChangeBatch, SchemaError> {
        let key_field = schema.key.as_ref().and_then(|key| key.hash_field.clone());
        let Some(key_field) = key_field else {
            return Ok(IndexChangeBatch {
                schema_name: schema_name.to_string(),
                searchable_fields: None,
                changes: Vec::new(),
            });
        };
        let molecule = schema
            .field_molecule_uuids
            .as_ref()
            .and_then(|molecules| molecules.get(&key_field).cloned())
            .or_else(|| {
                schema
                    .runtime_fields
                    .get(&key_field)
                    .and_then(|field| field.inner.molecule_uuid().cloned())
            })
            .unwrap_or_else(|| crate::atom::deterministic_molecule_uuid(schema_name, &key_field));
        let mut schema = schema.clone();
        for field in schema.runtime_fields.values_mut() {
            field
                .common_mut()
                .set_storage_prefix(Some(instance.to_owned()));
        }
        let has_range = matches!(
            schema.schema_type,
            crate::schema::types::schema::DeclarativeSchemaType::HashRange
        );
        let mut seen = HashSet::new();
        let mut changes = Vec::new();
        let mut cursor = None;
        loop {
            let request = SchemaRecordKeysRequest {
                requested: schema_name,
                schema_id: &schema.name,
                limit: 128,
                cursor: cursor.as_deref(),
                hash_filter: None,
            };
            let (keys, next_cursor, has_more) = self
                .list_schema_key_page(&schema, &key_field, &molecule, &request, Some(instance))
                .await?;
            for (hash, range) in keys {
                let key_value = KeyValue::new(Some(hash), has_range.then_some(range));
                if seen.insert(key_value.clone()) {
                    changes.push(IndexChange {
                        mutation_id: format!(
                            "catalog-unshare-{schema_name}-{instance}-{}",
                            changes.len()
                        ),
                        kind: IndexChangeKind::Tombstone,
                        key_value,
                        fields_and_values: HashMap::default(),
                    });
                }
            }
            if !has_more {
                break;
            }
            if next_cursor.is_none() || next_cursor == cursor {
                return Err(SchemaError::StorageRead {
                    detail: "catalog removal key page did not advance".into(),
                });
            }
            cursor = next_cursor;
        }
        Ok(IndexChangeBatch {
            schema_name: schema_name.to_string(),
            searchable_fields: None,
            changes,
        })
    }
}
