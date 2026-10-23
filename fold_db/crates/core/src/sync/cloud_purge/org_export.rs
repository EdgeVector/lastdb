//! Export one org's reachable schema instances into a fresh LastStore.

use super::org_rows::{self, error};
use crate::access::{parse_db_locator, DbLocator};
use crate::db_operations::{DbCatalogEntry, DbOperations, UNPREFIXED_INSTANCE_ID};
use crate::sharing::org_sync_target::OrgSyncTarget;
use crate::storage::{LastStoreNamespacedStore, NamespacedStore};
use crate::sync::{SyncError, SyncResult};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Arc;

pub type OrgCatalogSource = crate::sync::cloud_purge_peer::scope::OrgCatalogImport;

pub struct ScopedExport {
    pub store: Arc<LastStoreNamespacedStore>,
    pub catalog: OrgCatalogSource,
}

/// Select exactly the atom bodies that the Org export includes. The caller
/// holds the source mutation fence and rewrite lock before materialization.
pub(crate) async fn source_atom_keys(
    ops: &DbOperations,
    target: &OrgSyncTarget,
) -> SyncResult<Vec<String>> {
    if !target.active || target.slug.is_empty() {
        return Err(error("org file closure requires an active named target"));
    }
    let admission = super::memory::Admission::acquire(0).await?;
    let source = ops
        .namespaced_store()
        .raw_last_store()
        .ok_or_else(|| error("org file closure requires LastStore"))?;
    let entries = source_membership(ops, target, &source, &admission).await?;
    let names = entries
        .iter()
        .map(|entry| entry.schema_name.clone())
        .collect();
    let schemas = load_schemas(ops, &names, &admission).await?;
    let instances = instance_molecules(&entries, &schemas);
    let main = ops.open_namespace("main").await.map_err(error)?;
    super::org_rekey::source_atom_keys(
        ops,
        main.as_ref(),
        &org_rows::main_rows(&source, &admission)?,
        &instances,
        &admission,
    )
    .await
}

/// The caller stops writes and holds the source rewrite lock. Both stopped
/// peers and selected-device preparation use this same account boundary.
pub async fn export_scoped_store_from_ops(
    ops: &DbOperations,
    target: &OrgSyncTarget,
    destination: &Path,
    _node_key: &[u8; 32],
    target_key: &[u8; 32],
) -> SyncResult<ScopedExport> {
    if !target.active || target.slug.is_empty() {
        return Err(error("org export requires an active named target"));
    }
    let admission = super::memory::Admission::acquire(0).await?;
    let source = ops
        .namespaced_store()
        .raw_last_store()
        .ok_or_else(|| error("org export requires LastStore"))?;
    let entries = source_membership(ops, target, &source, &admission).await?;
    let schema_names: BTreeSet<_> = entries
        .iter()
        .map(|entry| entry.schema_name.clone())
        .collect();
    let schemas = load_schemas(ops, &schema_names, &admission).await?;
    let (instances, instance_ids, catalog) = build_scope(entries, target, &schemas, schema_names)?;
    super::files::owner_directory(destination)?;
    if std::fs::read_dir(destination)?.next().is_some() {
        return Err(error("isolated org destination must be empty"));
    }
    let mut options = LastStoreNamespacedStore::product_hash_group_options();
    options.data_key = Some(*target_key);
    let store = Arc::new(
        LastStoreNamespacedStore::open_with_options_and_high_water_data_key(
            destination,
            options,
            crate::storage::laststore::high_water_path_for_store_root(destination),
        )
        .map_err(error)?,
    );
    super::org_element_protection::initialize(&store, target_key).await?;
    let org_key = crate::sharing::org_sync_target::e2e_key_bytes(target).map_err(error)?;
    let molecules: BTreeSet<_> = instances.values().flatten().cloned().collect();
    let key_names: Vec<_> = molecules
        .iter()
        .flat_map(|id| {
            [
                crate::db_operations::molecule_key_store::metadata_key(id),
                crate::db_operations::molecule_key_store::wrap_key_for(id, "node"),
            ]
        })
        .collect();
    admission.check(super::memory::keys_peak(
        &admission,
        ops.namespaced_store().as_ref(),
        "molecule_keys",
        &key_names,
    )?)?;
    let keys = ops
        .molecule_keys()
        .export_domain_keys(&molecules, &catalog.access_domain, &org_key)
        .await
        .map_err(error)?;
    let main = ops.open_namespace("main").await.map_err(error)?;
    let physical = org_rows::main_rows(&source, &admission)?;
    let rows = super::org_rekey::export_main_rows(
        ops,
        main.as_ref(),
        super::org_rekey::OrgRowExport {
            physical: &physical,
            instances: &instances,
            replacements: &instance_ids,
            bundles: &keys.bundles,
            target_key,
            destination: &store,
            admission: &admission,
        },
    )
    .await?;
    write_export_payload(&store, ops, &catalog, &schemas, rows, keys.rows, &admission).await?;
    store.restore_durability_barrier().await.map_err(error)?;
    store.verify_integrity().map_err(error)?;
    Ok(ScopedExport { store, catalog })
}

async fn source_membership(
    ops: &DbOperations,
    target: &OrgSyncTarget,
    source: &laststore::LastStore,
    admission: &super::memory::Admission,
) -> SyncResult<Vec<DbCatalogEntry>> {
    let mut seeds = BTreeSet::new();
    for bytes in org_rows::physical_rows(source, "db_catalog", admission)?.values() {
        let Ok(entry) = serde_json::from_slice::<DbCatalogEntry>(bytes) else {
            continue;
        };
        if matches!(parse_db_locator(&entry.db_locator).map_err(error)?,DbLocator::Org{org_slug,..} if org_slug==target.slug)
        {
            seeds.insert((entry.db_locator, entry.schema_name));
        }
    }
    let identities: Vec<_> = seeds.into_iter().collect();
    let entries = ops
        .db_catalog()
        .get_many(&identities)
        .await
        .map_err(error)?
        .into_iter()
        .map(|entry| entry.ok_or_else(|| error("org catalog source membership disappeared")))
        .collect::<SyncResult<Vec<_>>>()?;
    if entries.is_empty() {
        return Err(error("org has no exact local catalog membership"));
    }
    // Bootstrap keyed membership before the permanent cloud hold.
    ops.db_catalog()
        .put_membership_batch(&entries)
        .await
        .map_err(error)?;
    Ok(entries)
}

async fn load_schemas(
    ops: &DbOperations,
    names: &BTreeSet<String>,
    admission: &super::memory::Admission,
) -> SyncResult<BTreeMap<String, crate::schema::Schema>> {
    admission.check(super::memory::keys_peak(
        admission,
        ops.namespaced_store().as_ref(),
        "schemas",
        &names.iter().cloned().collect::<Vec<_>>(),
    )?)?;
    let store = ops.open_namespace("schemas").await.map_err(error)?;
    let values = store
        .get_many(names.iter().map(|name| name.as_bytes().to_vec()).collect())
        .await
        .map_err(error)?;
    let mut output = BTreeMap::new();
    for (name, value) in names.iter().zip(values) {
        let value = value.ok_or_else(|| error("org catalog lacks its canonical schema"))?;
        let mut schema: crate::schema::Schema = serde_json::from_slice(&value).map_err(error)?;
        if schema.name != *name {
            return Err(SyncError::Storage(
                "org schema name differs from exact membership".into(),
            ));
        }
        schema.populate_runtime_fields().map_err(error)?;
        output.insert(name.clone(), schema);
    }
    Ok(output)
}

type ExportScope = (
    BTreeMap<String, BTreeSet<String>>,
    BTreeMap<String, String>,
    OrgCatalogSource,
);

fn build_scope(
    entries: Vec<DbCatalogEntry>,
    target: &OrgSyncTarget,
    schemas: &BTreeMap<String, crate::schema::Schema>,
    schema_names: BTreeSet<String>,
) -> SyncResult<ExportScope> {
    let instances = instance_molecules(&entries, schemas);
    let instance_ids: BTreeMap<_, _> = instances
        .keys()
        .map(|old| {
            (
                old.clone(),
                crate::hex::sha256_hex(uuid::Uuid::new_v4().as_bytes()),
            )
        })
        .collect();
    let mut catalog = OrgCatalogSource {
        format_version: 1,
        org_hash: target.org_hash.clone(),
        instance_id: instance_ids
            .values()
            .next()
            .cloned()
            .ok_or_else(|| error("org has no local schema source"))?,
        instance_ids: instance_ids.clone(),
        access_domain: format!("org:{}", target.org_hash),
        entries,
        schema_names: schema_names.into_iter().collect(),
    };
    for entry in &mut catalog.entries {
        entry.instance_id = Some(
            instance_ids[entry
                .instance_id
                .as_deref()
                .unwrap_or(UNPREFIXED_INSTANCE_ID)]
            .clone(),
        );
    }
    catalog.validate(target)?;
    Ok((instances, instance_ids, catalog))
}

fn instance_molecules(
    entries: &[DbCatalogEntry],
    schemas: &BTreeMap<String, crate::schema::Schema>,
) -> BTreeMap<String, BTreeSet<String>> {
    let mut instances: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for entry in entries {
        let instance = entry
            .instance_id
            .as_deref()
            .unwrap_or(UNPREFIXED_INSTANCE_ID);
        let molecules = instances.entry(instance.into()).or_default();
        for (name, field) in &schemas[&entry.schema_name].runtime_fields {
            molecules.insert(field.common().molecule_uuid().cloned().unwrap_or_else(|| {
                crate::atom::deterministic_molecule_uuid(&entry.schema_name, name)
            }));
        }
    }
    instances
}

async fn write_export_payload(
    store: &LastStoreNamespacedStore,
    ops: &DbOperations,
    catalog: &OrgCatalogSource,
    schemas: &BTreeMap<String, crate::schema::Schema>,
    rows: Vec<(Vec<u8>, Vec<u8>)>,
    key_rows: Vec<(Vec<u8>, Vec<u8>)>,
    admission: &super::memory::Admission,
) -> SyncResult<()> {
    store
        .open_namespace("main")
        .await
        .map_err(error)?
        .batch_put(rows)
        .await
        .map_err(error)?;
    store
        .open_namespace("molecule_keys")
        .await
        .map_err(error)?
        .batch_put(key_rows)
        .await
        .map_err(error)?;
    store
        .open_namespace("schemas")
        .await
        .map_err(error)?
        .batch_put(
            schemas
                .values()
                .map(|schema| {
                    Ok((
                        schema.name.as_bytes().to_vec(),
                        serde_json::to_vec(schema).map_err(error)?,
                    ))
                })
                .collect::<SyncResult<_>>()?,
        )
        .await
        .map_err(error)?;
    let state = ops.open_namespace("schema_states").await.map_err(error)?;
    admission.check(super::memory::keys_peak(
        admission,
        ops.namespaced_store().as_ref(),
        "schema_states",
        &catalog.schema_names,
    )?)?;
    let state_keys = catalog
        .schema_names
        .iter()
        .map(|name| name.as_bytes().to_vec())
        .collect();
    let values = state.get_many(state_keys).await.map_err(error)?;
    let states = catalog
        .schema_names
        .iter()
        .zip(values)
        .map(|(name, value)| {
            Ok((
                name.as_bytes().to_vec(),
                value.ok_or_else(|| error("org schema state is absent"))?,
            ))
        })
        .collect::<SyncResult<_>>()?;
    store
        .open_namespace("schema_states")
        .await
        .map_err(error)?
        .batch_put(states)
        .await
        .map_err(error)?;
    Ok(())
}
