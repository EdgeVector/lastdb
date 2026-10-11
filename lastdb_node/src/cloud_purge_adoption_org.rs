//! Stopped-store org replacement. The common adoption owner controls the
//! clean-stop receipt and the final directory switch; this module never
//! changes a serving store or emits an adoption acknowledgement.

use base64::Engine;
use fold_db::crypto::{CryptoProvider, E2eKeys, LocalCryptoProvider};
use fold_db::db_operations::DbOperations;
use fold_db::sharing::org_sync_target::OrgSyncTarget;
use fold_db::storage::{
    EncryptingNamespacedStore, LastStoreNamespacedStore, NamespacedStore,
    LASTSTORE_PLAINTEXT_NAMESPACES,
};
use fold_db::sync::cloud_purge_peer::scope::{
    import_org_into_inactive_store, remove_inherited_orgs_from_inactive_store, OrgCatalogImport,
    OrgImportReport,
};
use std::path::Path;
use std::sync::Arc;

mod local_export;
mod queues;

pub(crate) use queues::{reset_org_candidate_generation, reset_personal_candidate_generation};

/// Read the exact encrypted catalog object downloaded by the root restore.
pub(crate) async fn read_org_catalog(
    root: &fold_db::sync::cloud_purge::protocol::CloudReplacementRoot,
    restored_root_home: &Path,
    key: &[u8; 32],
) -> Result<Vec<u8>, String> {
    let metadata: Vec<_> = root
        .objects
        .iter()
        .filter(|object| object.role == "org_catalog")
        .collect();
    if metadata.len() != 1 {
        return Err("the org root needs one exact catalog object".into());
    }
    let object = metadata[0];
    object.validate().map_err(message)?;
    if object.logical_root != root.logical_root || object.bytes > 16 * 1024 * 1024 {
        return Err("the org catalog object scope or size differs".into());
    }
    let path = restored_root_home
        .join("download/objects")
        .join(&object.sha256);
    let stat = std::fs::symlink_metadata(&path).map_err(message)?;
    if !stat.file_type().is_file() || stat.len() != object.bytes {
        return Err("the org catalog ciphertext length or type differs".into());
    }
    let bytes = std::fs::read(path).map_err(message)?;
    if fold_db::hex::sha256_hex(&bytes) != object.sha256 {
        return Err("the org catalog ciphertext differs from the ready inventory".into());
    }
    LocalCryptoProvider::from_key(*key)
        .decrypt(&bytes)
        .await
        .map_err(|_| "the org catalog authentication failed".into())
}

/// The destination must already be an inactive stopped copy of the peer's old
/// data. This preserves personal data, other orgs, and surviving shared rows.
pub(crate) async fn merge_org_replacement(
    old_stopped_home: &Path,
    replacement_root_home: &Path,
    inactive_merged_home: &Path,
    encrypted_catalog_plaintext: &[u8],
) -> Result<OrgImportReport, String> {
    distinct_data_paths(
        old_stopped_home,
        replacement_root_home,
        inactive_merged_home,
    )?;
    let catalog: OrgCatalogImport = serde_json::from_slice(encrypted_catalog_plaintext)
        .map_err(|_| "the org replacement catalog is invalid")?;
    let keys = identity_keys(old_stopped_home)?;
    let (_, previous) = open_peer_store(old_stopped_home, &keys).await?;
    let registry = previous
        .open_namespace("org_sync_targets")
        .await
        .map_err(message)?;
    let bytes = registry
        .get(format!("org_sync:{}", catalog.org_hash).as_bytes())
        .await
        .map_err(message)?
        .ok_or("the peer has no registered target for this org")?;
    let target: OrgSyncTarget =
        serde_json::from_slice(&bytes).map_err(|_| "the peer org target is invalid")?;
    catalog.validate(&target).map_err(message)?;
    let org_key = org_key(&target)?;
    let (_, source) = open_local_store(replacement_root_home, org_key).await?;
    let (candidate_raw, candidate) = open_peer_store(inactive_merged_home, &keys).await?;
    let report = import_org_into_inactive_store(&source, &candidate, &catalog, &target, &org_key)
        .await
        .map_err(message)?;
    candidate
        .namespaced_store()
        .restore_durability_barrier()
        .await
        .map_err(message)?;
    candidate_raw.verify_integrity().map_err(message)?;
    Ok(report)
}

pub(crate) async fn open_local_store(
    home: &Path,
    content_key: [u8; 32],
) -> Result<(Arc<LastStoreNamespacedStore>, DbOperations), String> {
    open_store_with_codec(home, content_key, fold_db::atom::MoleculeKeyCodec::plain()).await
}

pub(super) async fn open_peer_store(
    home: &Path,
    keys: &E2eKeys,
) -> Result<(Arc<LastStoreNamespacedStore>, DbOperations), String> {
    let codec = fold_db::atom::MoleculeKeyCodec::with_encodings(
        fold_db::atom::HashKeyEncoding::from_env_or_default(),
        fold_db::atom::RangeKeyEncoding::from_env_or_default(),
        Some(keys.index_key()),
        Some(keys.ope_key()),
    );
    open_store_with_codec(home, keys.encryption_key(), codec).await
}

async fn open_store_with_codec(
    home: &Path,
    content_key: [u8; 32],
    codec: fold_db::atom::MoleculeKeyCodec,
) -> Result<(Arc<LastStoreNamespacedStore>, DbOperations), String> {
    let data = home.join("data");
    if !std::fs::symlink_metadata(&data)
        .map_err(message)?
        .file_type()
        .is_dir()
        || std::fs::read_dir(&data).map_err(message)?.next().is_none()
    {
        return Err("the inactive org data store is absent".into());
    }
    let mut options = LastStoreNamespacedStore::product_hash_group_options();
    options.data_key = Some(content_key);
    let raw =
        Arc::new(LastStoreNamespacedStore::open_with_options(&data, options).map_err(message)?);
    raw.verify_integrity().map_err(message)?;
    let base: Arc<dyn NamespacedStore> = raw.clone();
    let store: Arc<dyn NamespacedStore> =
        Arc::new(EncryptingNamespacedStore::with_plaintext_namespaces(
            base,
            Arc::new(LocalCryptoProvider::from_key(content_key)),
            LASTSTORE_PLAINTEXT_NAMESPACES
                .iter()
                .map(|value| (*value).to_string())
                .collect(),
        ));
    // Imported rows retain their molecule bundle keys. Legacy personal rows
    // keep the identity codec when the candidate boots through the factory.
    let ops = DbOperations::from_namespaced_store_with_atom_and_molecule_keys(
        store,
        Some(content_key),
        codec,
        Some(content_key),
    )
    .await
    .map_err(message)?;
    Ok((raw, ops))
}

pub(crate) fn identity_keys(home: &Path) -> Result<E2eKeys, String> {
    let path = home.join(crate::host::IDENTITY_KEY_FILE);
    let metadata = std::fs::symlink_metadata(&path).map_err(message)?;
    if !metadata.file_type().is_file() || metadata.len() != 32 {
        return Err("the stopped peer identity is invalid".into());
    }
    let mut seed: [u8; 32] = std::fs::read(path)
        .map_err(message)?
        .as_slice()
        .try_into()
        .map_err(|_| "the stopped peer identity is invalid")?;
    let keys =
        E2eKeys::from_ed25519_seed(&seed).map_err(|_| "the stopped peer keys are unavailable")?;
    seed.fill(0);
    Ok(keys)
}

pub(crate) fn org_key(target: &OrgSyncTarget) -> Result<[u8; 32], String> {
    base64::engine::general_purpose::STANDARD
        .decode(&target.e2e_key_b64)
        .map_err(|_| "the org target key is invalid")?
        .as_slice()
        .try_into()
        .map_err(|_| "the org target key is invalid".into())
}

fn distinct_data_paths(old: &Path, source: &Path, candidate: &Path) -> Result<(), String> {
    let paths = [old, source, candidate].map(|path| path.join("data"));
    let canonical: Vec<_> = paths
        .iter()
        .map(std::fs::canonicalize)
        .collect::<Result<_, _>>()
        .map_err(message)?;
    if canonical[0] == canonical[1] || canonical[0] == canonical[2] || canonical[1] == canonical[2]
    {
        return Err("org adoption requires three separate stopped data stores".into());
    }
    Ok(())
}

fn message(error: impl std::fmt::Display) -> String {
    error.to_string()
}

/// Keep the stopped peer's org data when a personal replacement changes the
/// personal store. Legacy shared unprefixed data receives an isolated prefix.
/// No org data or target registry from the selected device becomes live.
pub(crate) async fn preserve_unrelated_orgs(
    old_stopped_home: &Path,
    inactive_new_home: &Path,
) -> Result<Vec<OrgImportReport>, String> {
    let old = std::fs::canonicalize(old_stopped_home.join("data")).map_err(message)?;
    let next = std::fs::canonicalize(inactive_new_home.join("data")).map_err(message)?;
    if old == next {
        return Err("org preservation requires a separate inactive store".into());
    }
    let keys = identity_keys(old_stopped_home)?;
    let (_, source) = open_peer_store(old_stopped_home, &keys).await?;
    let (candidate_raw, candidate) = open_peer_store(inactive_new_home, &keys).await?;
    let targets = fold_db::sharing::org_sync_target::list_org_sync_targets_in_ops(&source)
        .await
        .map_err(message)?;
    // Source closure and keys must be proved before inherited memberships are removed.
    let mut exports = Vec::with_capacity(targets.len());
    for target in targets {
        let destination = inactive_new_home
            .join("org-preservation")
            .join(&target.org_hash);
        let (catalog, preserved) =
            local_export::prepare(&source, &target, &destination, &keys.encryption_key()).await?;
        exports.push((target, catalog, preserved));
    }
    remove_inherited_orgs_from_inactive_store(&candidate)
        .await
        .map_err(message)?;
    let mut reports = Vec::with_capacity(exports.len());
    for (target, catalog, preserved) in exports {
        let report = import_org_into_inactive_store(
            &preserved,
            &candidate,
            &catalog,
            &target,
            &org_key(&target)?,
        )
        .await
        .map_err(message)?;
        fold_db::sync::cloud_purge_peer::scope_alias::install_preserved_org_aliases(
            source.namespaced_store().as_ref(),
            candidate.namespaced_store().as_ref(),
            &catalog,
        )
        .await
        .map_err(message)?;
        reports.push(report);
    }
    candidate
        .namespaced_store()
        .restore_durability_barrier()
        .await
        .map_err(message)?;
    candidate_raw.verify_integrity().map_err(message)?;
    Ok(reports)
}
