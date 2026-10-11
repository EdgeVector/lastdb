//! Remove inherited org membership from an inactive personal candidate.

use super::*;

/// Remove inherited org membership from a personal replacement candidate.
/// Rows that a personal or named survivor still references remain in place.
/// The old peer's orgs are then imported before the candidate can boot.
pub async fn remove_inherited_orgs_from_inactive_store(ops: &DbOperations) -> SyncResult<usize> {
    let entries = ops.db_catalog().list_entries().await.map_err(storage)?;
    let mut removed_prefixes = BTreeSet::new();
    let mut survivor_prefixes = BTreeSet::new();
    let mut removed = 0;
    for entry in entries {
        let org = matches!(
            parse_db_locator(&entry.db_locator).map_err(fail)?,
            DbLocator::Org { .. }
        );
        if !org {
            if let Some(prefix) = entry.instance_id {
                survivor_prefixes.insert(prefix);
            }
            continue;
        }
        if let Some(prefix) = &entry.instance_id {
            removed_prefixes.insert(prefix.clone());
        }
        let schema = ops
            .schemas()
            .get_schema(&entry.schema_name)
            .await
            .map_err(storage)?
            .ok_or_else(|| fail("inherited org membership lacks a canonical schema"))?;
        let removal = ops
            .atoms()
            .prepare_catalog_atom_ref_removal(
                &entry.db_locator,
                &entry.schema_name,
                &schema,
                entry.instance_id.as_deref(),
            )
            .await
            .map_err(storage)?;
        ops.db_catalog()
            .delete(&entry.db_locator, &entry.schema_name)
            .await
            .map_err(storage)?;
        ops.atoms()
            .commit_catalog_atom_refs(removal)
            .await
            .map_err(storage)?;
        removed += 1;
    }
    let store = ops.namespaced_store();
    let admission = crate::sync::cloud_purge::memory::Admission::acquire(0).await?;
    let raw = store
        .raw_last_store()
        .ok_or_else(|| fail("org cleanup requires LastStore"))?;
    let all_keys = crate::sync::cloud_purge::org_rows::main_rows(&raw, &admission)?;
    for prefix in removed_prefixes.difference(&survivor_prefixes) {
        let prefix = format!("{prefix}:");
        let keys: Vec<_> = all_keys
            .keys()
            .filter(|key| key.starts_with(&prefix))
            .cloned()
            .collect();
        crate::sync::cloud_purge::row_copy::delete(store.as_ref(), "main", &keys, &admission)
            .await?;
    }
    drop(admission);
    reset_device_registry(ops).await?;
    store.restore_durability_barrier().await.map_err(storage)?;
    Ok(removed)
}

async fn reset_device_registry(ops: &DbOperations) -> SyncResult<()> {
    let store = ops.namespaced_store();
    // The registry is device-specific and cannot come from another writer.
    let registry = store
        .open_namespace("org_sync_targets")
        .await
        .map_err(storage)?;
    loop {
        let keys = registry
            .scan_prefix_paged(b"org_sync:", 128)
            .await
            .map_err(storage)?
            .into_iter()
            .map(|(key, _)| key)
            .collect::<Vec<_>>();
        if keys.is_empty() {
            break;
        }
        registry.batch_delete(keys).await.map_err(storage)?;
    }
    // Instance aliases describe one device's local org copy. The old peer's
    // composed aliases replace them only after its scoped data is imported.
    let aliases = store
        .open_namespace(super::super::scope_alias::NAMESPACE)
        .await
        .map_err(storage)?;
    loop {
        let keys = aliases
            .scan_range_paged(&[], &[0xff], 128)
            .await
            .map_err(storage)?
            .into_iter()
            .map(|(key, _)| key)
            .collect::<Vec<_>>();
        if keys.is_empty() {
            break;
        }
        aliases.batch_delete(keys).await.map_err(storage)?;
    }
    Ok(())
}
