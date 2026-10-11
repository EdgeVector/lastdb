//! Inactive org import preflight and membership changes.

use super::*;
use crate::schema::Schema;

pub(super) async fn selected_memberships(
    destination: &DbOperations,
    catalog: &OrgCatalogImport,
    target: &OrgSyncTarget,
) -> SyncResult<Vec<DbCatalogEntry>> {
    let old_entries = destination
        .db_catalog()
        .list_entries()
        .await
        .map_err(storage)?;
    let survivor_instances: BTreeSet<_> = old_entries
        .iter()
        .filter_map(
            |entry| match locator_belongs_to_target(&entry.db_locator, target) {
                Ok(false) => entry.instance_id.clone(),
                _ => None,
            },
        )
        .collect();
    if catalog
        .entries
        .iter()
        .filter_map(|entry| entry.instance_id.as_ref())
        .any(|instance| survivor_instances.contains(instance))
    {
        return Err(fail(
            "org replacement instance collides with a surviving database",
        ));
    }
    let selected_old: Vec<_> = old_entries
        .into_iter()
        .filter_map(
            |entry| match locator_belongs_to_target(&entry.db_locator, target) {
                Ok(true) => Some(Ok(entry)),
                Ok(false) => None,
                Err(error) => Some(Err(error)),
            },
        )
        .collect::<SyncResult<_>>()?;
    Ok(selected_old)
}

pub(super) async fn preflight_schemas(
    source: &DbOperations,
    destination: &DbOperations,
    catalog: &OrgCatalogImport,
    domain_key: &[u8; 32],
) -> SyncResult<BTreeMap<String, Schema>> {
    let source_store = source.namespaced_store();
    let source_keys = source_store
        .open_namespace("molecule_keys")
        .await
        .map_err(storage)?;
    let mut schemas = BTreeMap::new();
    // Preflight every definition and bundle before any catalog row changes.
    for name in &catalog.schema_names {
        let schema = source
            .schemas()
            .get_schema(name)
            .await
            .map_err(storage)?
            .ok_or_else(|| fail("org replacement lacks its schema definition"))?;
        if let Some(existing) = destination
            .schemas()
            .get_schema(name)
            .await
            .map_err(storage)?
        {
            if canonical(&existing)? != canonical(&schema)? {
                return Err(fail(
                    "org replacement conflicts with an existing canonical schema",
                ));
            }
        }
        destination
            .molecule_keys()
            .import_domain_for_schema(
                source_keys.as_ref(),
                &schema,
                &catalog.access_domain,
                domain_key,
            )
            .await
            .map_err(storage)?;
        schemas.insert(name.clone(), schema);
    }
    Ok(schemas)
}

pub(super) async fn commit_membership(
    source: &DbOperations,
    destination: &DbOperations,
    catalog: &OrgCatalogImport,
    schemas: &BTreeMap<String, Schema>,
    selected_old: &[DbCatalogEntry],
) -> SyncResult<()> {
    for (name, schema) in schemas {
        destination
            .schemas()
            .store_schema(name, schema)
            .await
            .map_err(storage)?;
        if let Some(state) = source
            .schemas()
            .get_schema_state(name)
            .await
            .map_err(storage)?
        {
            if destination
                .schemas()
                .get_schema_state(name)
                .await
                .map_err(storage)?
                .is_none()
            {
                destination
                    .schemas()
                    .store_schema_state(name, &state)
                    .await
                    .map_err(storage)?;
            }
        }
    }
    // No request can observe these individual writes: this store is inactive.
    // Per-entry atom transitions supply crash recovery inside this candidate.
    for old in selected_old {
        let schema = destination
            .schemas()
            .get_schema(&old.schema_name)
            .await
            .map_err(storage)?
            .ok_or_else(|| fail("prior org membership lacks a canonical schema"))?;
        let removal = destination
            .atoms()
            .prepare_catalog_atom_ref_removal(
                &old.db_locator,
                &old.schema_name,
                &schema,
                old.instance_id.as_deref(),
            )
            .await
            .map_err(storage)?;
        destination
            .db_catalog()
            .delete(&old.db_locator, &old.schema_name)
            .await
            .map_err(storage)?;
        destination
            .atoms()
            .commit_catalog_atom_refs(removal)
            .await
            .map_err(storage)?;
    }
    for entry in &catalog.entries {
        let schema = schemas
            .get(&entry.schema_name)
            .ok_or_else(|| fail("org membership schema is absent"))?;
        let addition = destination
            .atoms()
            .prepare_catalog_atom_refs(
                &entry.db_locator,
                &entry.schema_name,
                schema,
                entry.instance_id.as_deref(),
            )
            .await
            .map_err(storage)?;
        destination.db_catalog().put(entry).await.map_err(storage)?;
        destination
            .atoms()
            .commit_catalog_atom_refs(addition)
            .await
            .map_err(storage)?;
    }
    Ok(())
}
