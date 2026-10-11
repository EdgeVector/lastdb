//! Inactive-store org import. Never publish a partially changed catalog.

use crate::access::{parse_db_locator, DbLocator};
use crate::db_operations::{DbCatalogEntry, DbOperations};
use crate::sharing::org_sync_target::OrgSyncTarget;
use crate::storage::traits::KvStore;
use crate::sync::{SyncError, SyncResult};
use serde::{Deserialize, Serialize};
use sha2::Digest;
use std::collections::{BTreeMap, BTreeSet};

mod files;
mod hygiene;
use files::copy_reachable_blobs;
pub use hygiene::remove_inherited_orgs_from_inactive_store;

/// Encrypted org metadata binds catalog membership to isolated source rows.
#[derive(Clone, Serialize, Deserialize)]
pub struct OrgCatalogImport {
    pub format_version: u32,
    pub org_hash: String,
    pub instance_id: String,
    #[serde(default)]
    pub instance_ids: BTreeMap<String, String>,
    pub access_domain: String,
    pub entries: Vec<DbCatalogEntry>,
    pub schema_names: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OrgImportReport {
    pub org_hash: String,
    pub instance_ids: Vec<String>,
    pub schemas: usize,
    pub catalog_entries: usize,
    pub replaced_catalog_entries: usize,
    pub copied_rows: u64,
    pub content_sha256: String,
}

impl OrgCatalogImport {
    pub fn validate(&self, target: &OrgSyncTarget) -> SyncResult<()> {
        if self.format_version != 1
            || self.org_hash != target.org_hash
            || self.access_domain != format!("org:{}", target.org_hash)
            || self.entries.is_empty()
            || self.schema_names.is_empty()
        {
            return Err(fail("org replacement catalog identity is incomplete"));
        }
        let schemas: BTreeSet<_> = self.schema_names.iter().collect();
        if schemas.len() != self.schema_names.len() {
            return Err(fail("org replacement repeats a schema"));
        }
        let mut identities = BTreeSet::new();
        let declared_instances: BTreeSet<_> = self.instance_ids.values().cloned().collect();
        for entry in &self.entries {
            if !schemas.contains(&entry.schema_name)
                || !identities.insert((&entry.db_locator, &entry.schema_name))
            {
                return Err(fail("org replacement catalog membership is inconsistent"));
            }
            if !locator_belongs_to_target(&entry.db_locator, target)? {
                return Err(fail("org replacement references another database account"));
            }
            let prefix = entry
                .instance_id
                .as_deref()
                .ok_or_else(|| fail("org replacement contains an unisolated instance"))?;
            if !crate::hex::is_lower_hex_sha256(prefix) {
                return Err(fail("org replacement instance has an invalid identity"));
            }
            if !declared_instances.is_empty() && !declared_instances.contains(prefix) {
                return Err(fail(
                    "org catalog references an undeclared isolated instance",
                ));
            }
        }
        Ok(())
    }
}

/// Merge into a stopped copy of the old store. A host changes the directory
/// only after this returns and writes its durable switch journal. Old instance
/// rows stay in the old-data archive and in the copy for surviving references.
pub async fn import_org_into_inactive_store(
    source: &DbOperations,
    destination: &DbOperations,
    catalog: &OrgCatalogImport,
    target: &OrgSyncTarget,
    domain_key: &[u8; 32],
) -> SyncResult<OrgImportReport> {
    catalog.validate(target)?;
    let source_store = source.namespaced_store();
    let destination_store = destination.namespaced_store();
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
    let instances: BTreeSet<_> = catalog
        .entries
        .iter()
        .filter_map(|entry| entry.instance_id.clone())
        .collect();
    let source_main = source_store.open_namespace("main").await.map_err(storage)?;
    let destination_main = destination_store
        .open_namespace("main")
        .await
        .map_err(storage)?;
    let mut digest = sha2::Sha256::new();
    let mut copied_rows = 0;
    for instance in &instances {
        let prefix = format!("{instance}:");
        copied_rows += copy_prefix(
            source_main.as_ref(),
            destination_main.as_ref(),
            prefix.as_bytes(),
            &mut digest,
        )
        .await?;
    }
    copied_rows += copy_reachable_blobs(&source_store, &destination_store, &mut digest).await?;
    for (name, schema) in &schemas {
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
    for old in &selected_old {
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
    let mut replacement_target = target.clone();
    replacement_target.storage_prefixes = instances.iter().cloned().collect();
    replacement_target.unprefixed_schema_names.clear();
    destination_store
        .open_namespace("org_sync_targets")
        .await
        .map_err(storage)?
        .put(
            format!("org_sync:{}", target.org_hash).as_bytes(),
            serde_json::to_vec(&replacement_target).map_err(storage)?,
        )
        .await
        .map_err(storage)?;
    destination_store
        .restore_durability_barrier()
        .await
        .map_err(storage)?;
    Ok(OrgImportReport {
        org_hash: target.org_hash.clone(),
        instance_ids: instances.into_iter().collect(),
        schemas: schemas.len(),
        catalog_entries: catalog.entries.len(),
        replaced_catalog_entries: selected_old.len(),
        copied_rows,
        content_sha256: crate::hex::hex_lower(sha2::Digest::finalize(digest)),
    })
}

fn locator_belongs_to_target(locator: &str, target: &OrgSyncTarget) -> SyncResult<bool> {
    let parsed = parse_db_locator(locator).map_err(fail)?;
    match parsed {
        DbLocator::Org { org_slug, .. } => {
            if target.slug.is_empty() {
                return Err(fail("org target has no locator slug"));
            }
            Ok(org_slug.eq_ignore_ascii_case(&target.slug))
        }
        // A hash-only locator does not prove org ownership. Preserve it as an
        // unrelated local survivor; source org entries must name the org.
        DbLocator::DbHash(_) | DbLocator::Personal => Ok(false),
    }
}

async fn copy_prefix(
    source: &dyn KvStore,
    destination: &dyn KvStore,
    prefix: &[u8],
    digest: &mut sha2::Sha256,
) -> SyncResult<u64> {
    use sha2::Digest;
    let mut cursor = prefix.to_vec();
    let mut end = prefix.to_vec();
    while end.last() == Some(&255) {
        end.pop();
    }
    *end.last_mut()
        .ok_or_else(|| fail("org prefix has no upper bound"))? += 1;
    let mut count = 0u64;
    loop {
        let page = source
            .scan_range_paged(&cursor, &end, 128)
            .await
            .map_err(storage)?;
        if page.is_empty() {
            break;
        }
        for (key, value) in &page {
            if !key.starts_with(prefix) {
                return Err(fail("org source crosses an instance boundary"));
            }
            digest.update((key.len() as u64).to_be_bytes());
            digest.update(key);
            digest.update((value.len() as u64).to_be_bytes());
            digest.update(value);
        }
        cursor = page
            .last()
            .ok_or_else(|| fail("org source page has no last key"))?
            .0
            .clone();
        cursor.push(0);
        count += page.len() as u64;
        destination.restore_batch_put(page).await.map_err(storage)?;
    }
    Ok(count)
}

fn canonical(value: &impl Serialize) -> SyncResult<Vec<u8>> {
    serde_json::to_vec(&serde_json::to_value(value).map_err(storage)?).map_err(storage)
}

fn fail(message: impl Into<String>) -> SyncError {
    SyncError::Storage(message.into())
}
fn storage(error: impl std::fmt::Display) -> SyncError {
    fail(error.to_string())
}
