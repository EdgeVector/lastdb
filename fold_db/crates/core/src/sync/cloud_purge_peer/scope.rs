//! Inactive-store org import. Never publish a partially changed catalog.

use crate::access::{parse_db_locator, DbLocator};
use crate::db_operations::{DbCatalogEntry, DbOperations};
use crate::sharing::org_sync_target::OrgSyncTarget;
use crate::sync::{SyncError, SyncResult};
use serde::{Deserialize, Serialize};
use sha2::Digest;
use std::collections::{BTreeMap, BTreeSet};

mod files;
mod hygiene;
mod import;
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
    let selected_old = import::selected_memberships(destination, catalog, target).await?;
    let schemas = import::preflight_schemas(source, destination, catalog, domain_key).await?;
    let instances: BTreeSet<_> = catalog
        .entries
        .iter()
        .filter_map(|entry| entry.instance_id.clone())
        .collect();
    let mut digest = sha2::Sha256::new();
    let mut copied_rows = 0;
    for instance in &instances {
        let prefix = format!("{instance}:");
        copied_rows += copy_prefix(
            source_store.as_ref(),
            destination_store.as_ref(),
            prefix.as_bytes(),
            &mut digest,
        )
        .await?;
    }
    copied_rows += copy_reachable_blobs(&source_store, &destination_store, &mut digest).await?;
    import::commit_membership(source, destination, catalog, &schemas, &selected_old).await?;
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
    source: &dyn crate::storage::NamespacedStore,
    destination: &dyn crate::storage::NamespacedStore,
    prefix: &[u8],
    digest: &mut sha2::Sha256,
) -> SyncResult<u64> {
    let admission = crate::sync::cloud_purge::memory::Admission::acquire(0).await?;
    let raw = source
        .raw_last_store()
        .ok_or_else(|| fail("org main source requires LastStore"))?;
    let keys: Vec<_> = crate::sync::cloud_purge::org_rows::main_rows(&raw, &admission)?
        .into_keys()
        .filter(|key| key.as_bytes().starts_with(prefix))
        .collect();
    crate::sync::cloud_purge::row_copy::copy(
        source,
        destination,
        &keys,
        &admission,
        |key, value| {
            digest.update((key.len() as u64).to_be_bytes());
            digest.update(key);
            digest.update((value.len() as u64).to_be_bytes());
            digest.update(value);
        },
    )
    .await
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
