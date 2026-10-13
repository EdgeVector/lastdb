use crate::db_operations::search_index::{IndexChange, IndexChangeBatch, IndexChangeKind};
use crate::db_operations::{
    drop_prefixed_rows_for_instance, drop_unreferenced_prefixed_rows, referenced_instance_ids,
    UNPREFIXED_INSTANCE_ID,
};
use crate::error::FoldDbResult;
use crate::schema::types::{KeyValue, Schema};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

use super::FoldDB;

/// Result of deleting one catalog membership and reclaiming a zero-ref instance.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct CatalogDeleteReclaimResult {
    pub deleted: bool,
    pub instance_id: Option<String>,
    pub reclaimed_rows: usize,
    pub reclaimed_wraps: usize,
    pub search_tombstones: usize,
}

/// Result of leftover copy-geometry reclaim.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct CatalogReclaimResult {
    pub reclaimed_rows: usize,
}

/// What `reclaim_unreferenced_instance` removed.
#[derive(Default)]
struct InstanceReclaim {
    rows: usize,
    wraps: usize,
    search_tombstones: usize,
}

impl FoldDB {
    /// Delete one catalog membership. When that was the last reference to a
    /// prefixed instance, drop `{instance_id}:` rows in main.
    pub async fn delete_catalog_membership(
        &self,
        db_locator: &str,
        schema_name: &str,
    ) -> FoldDbResult<CatalogDeleteReclaimResult> {
        let catalog = self.db_ops().db_catalog();
        let existing = catalog.get(db_locator, schema_name).await?;
        let schema = self.schema_manager().get_schema_metadata(schema_name)?;
        let storage_prefix = existing
            .as_ref()
            .and_then(|entry| entry.instance_id.clone());
        let _molecule_guards = self
            .lock_schema_molecules(schema.as_ref(), storage_prefix.as_deref())
            .await;
        let atom_ref_plan = match (existing.as_ref(), schema.as_ref()) {
            (Some(_), Some(schema)) => Some(
                self.db_ops()
                    .atoms()
                    .prepare_catalog_atom_ref_removal(
                        db_locator,
                        schema_name,
                        schema,
                        storage_prefix.as_deref(),
                    )
                    .await?,
            ),
            _ => None,
        };
        let deleted = catalog.delete(db_locator, schema_name).await?;
        if deleted {
            if let Some(plan) = atom_ref_plan {
                self.db_ops()
                    .atoms()
                    .commit_catalog_atom_ref_removal(plan)
                    .await?;
            }
        } else if let Some(plan) = atom_ref_plan.as_ref() {
            self.db_ops().atoms().abort_catalog_atom_refs(plan).await?;
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
            reclaimed_wraps: reclaim.wraps,
            search_tombstones: reclaim.search_tombstones,
        })
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
    /// entries, domain key wraps, and `{instance}:` rows in main.
    async fn reclaim_unreferenced_instance(
        &self,
        schema_name: &str,
        schema: Option<&Schema>,
        instance: &str,
    ) -> FoldDbResult<InstanceReclaim> {
        let catalog = self.db_ops().db_catalog();
        let live = referenced_instance_ids(&catalog.list_entries().await?);
        let mut reclaim = InstanceReclaim::default();
        if live.contains(instance) {
            return Ok(reclaim);
        }
        if let Some(schema) = schema {
            let batch = self
                .build_catalog_unshare_tombstones(schema_name, schema, instance)
                .await?;
            reclaim.search_tombstones = batch.changes.len();
            if !batch.changes.is_empty() {
                self.mutation_manager
                    .deliver_index_change_batch(&batch)
                    .await
                    .map_err(crate::error::FoldDbError::from)?;
            }
            reclaim.wraps = self
                .db_ops()
                .molecule_keys()
                .drop_schema_domain_wraps(schema)
                .await?;
        }
        let main = self
            .db_ops()
            .namespaced_store()
            .open_namespace("main")
            .await?;
        reclaim.rows = drop_prefixed_rows_for_instance(main.as_ref(), instance).await?;
        Ok(reclaim)
    }

    async fn build_catalog_unshare_tombstones(
        &self,
        schema_name: &str,
        schema: &Schema,
        instance: &str,
    ) -> FoldDbResult<IndexChangeBatch> {
        let key_field = schema
            .key
            .as_ref()
            .and_then(|key| key.hash_field.clone().or_else(|| key.range_field.clone()));
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
        let report = self
            .db_ops()
            .atoms()
            .list_molecule_keys(&molecule, None, Some(instance))
            .await?;
        let mut seen = HashSet::new();
        let changes = report
            .rows
            .into_iter()
            .filter_map(|row| {
                let key = KeyValue::new(row.hash, row.range.filter(|range| !range.is_empty()));
                (key.hash.is_some() || key.range.is_some())
                    .then_some(key)
                    .filter(|key| seen.insert(key.clone()))
            })
            .enumerate()
            .map(|(index, key_value)| IndexChange {
                mutation_id: format!("catalog-unshare-{schema_name}-{instance}-{index}"),
                kind: IndexChangeKind::Tombstone,
                key_value,
                fields_and_values: HashMap::default(),
            })
            .collect();
        Ok(IndexChangeBatch {
            schema_name: schema_name.to_string(),
            searchable_fields: None,
            changes,
        })
    }

    /// Drop `{64hex}:` main rows whose prefix is not a live catalog instance.
    pub async fn reclaim_unreferenced_prefixed_rows(&self) -> FoldDbResult<CatalogReclaimResult> {
        let live = referenced_instance_ids(&self.db_ops().db_catalog().list_entries().await?);
        let main = self
            .db_ops()
            .namespaced_store()
            .open_namespace("main")
            .await?;
        let reclaimed_rows = drop_unreferenced_prefixed_rows(main.as_ref(), &live).await?;
        Ok(CatalogReclaimResult { reclaimed_rows })
    }
}
