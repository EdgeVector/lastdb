//! Exact inactive restore. No ordinary latest pointer or peer log is consulted.

use super::protocol::{CloudPurgeControl, CloudReplacementRoot};
use super::{download_replacement_objects, files};
use crate::crypto::CryptoProvider;
use crate::storage::laststore::{
    cloud_db_hash_for_store_uuid, high_water_path_for_store_root, manifest_sha256_hex,
    validate_manifest_chain, BackupManifest, LastStoreNamespacedStore,
};
use crate::storage::traits::NamespacedStore;
use crate::sync::auth::AuthClient;
use crate::sync::engine::RecoveryDescriptorV1;
use crate::sync::{SyncError, SyncResult};
use std::path::{Path, PathBuf};

pub struct CloudPurgeRootRestore {
    pub data: PathBuf,
    pub manifest: BackupManifest,
    pub restored_epoch: u64,
}

pub async fn restore_cloud_purge_root(
    auth: &AuthClient,
    control: &CloudPurgeControl,
    root: &CloudReplacementRoot,
    directory: &Path,
    crypto: &dyn CryptoProvider,
    account_key: &[u8; 32],
) -> SyncResult<PathBuf> {
    restore_cloud_purge_root_with_report(auth, control, root, directory, crypto, account_key)
        .await
        .map(|restored| restored.data)
}

pub async fn restore_cloud_purge_root_with_report(
    auth: &AuthClient,
    control: &CloudPurgeControl,
    root: &CloudReplacementRoot,
    directory: &Path,
    crypto: &dyn CryptoProvider,
    account_key: &[u8; 32],
) -> SyncResult<CloudPurgeRootRestore> {
    if root.format != "laststore_v1" {
        return Err(SyncError::Storage(
            "replacement requires a LastStore root".into(),
        ));
    }
    files::owner_directory(directory)?;
    let objects =
        download_replacement_objects(auth, control, root, &directory.join("download")).await?;
    let (manifest, descriptor) =
        decode_source_metadata(root, &objects, crypto, account_key).await?;
    let frame = descriptor.layout.packaging == "frame_aead";
    let options = descriptor
        .to_options(account_key, frame)
        .map_err(SyncError::Storage)?;
    let data = directory.join("data");
    if data.exists() && !directory.join("restore-intent.json").exists() {
        return Err(SyncError::Storage(
            "replacement destination is not a fresh owned store".into(),
        ));
    }
    let intent = serde_json::json!({"format_version":1,"operation_id":control.operation_id,"generation":control.generation,"manifest_sha256":root.manifest_sha256});
    if directory.join("restore-intent.json").exists() {
        let existing: serde_json::Value =
            serde_json::from_slice(&std::fs::read(directory.join("restore-intent.json"))?)
                .map_err(|_| {
                    SyncError::Storage("replacement restore intent decode failed".into())
                })?;
        if existing != intent {
            return Err(SyncError::Storage(
                "replacement restore retry differs from its saved intent".into(),
            ));
        }
    } else {
        files::atomic_json(&directory.join("restore-intent.json"), &intent)?;
    }
    let store = LastStoreNamespacedStore::open_with_options_and_high_water_data_key(
        &data,
        options,
        high_water_path_for_store_root(&data),
    )
    .map_err(|error| SyncError::Storage(error.to_string()))?;
    store
        .validate_restore_candidate(&manifest)
        .map_err(|error| SyncError::Storage(error.to_string()))?;
    for chunk in manifest.mutable_chunks.iter().chain(&manifest.atom_chunks) {
        let (_, path) = objects
            .iter()
            .find(|(object, _)| {
                object.role == "chunk"
                    && object.sha256 == chunk.sha256
                    && object.bytes == chunk.bytes
            })
            .ok_or_else(|| {
                SyncError::Storage("replacement lacks an exact manifest chunk".into())
            })?;
        store
            .install_backup_chunk_from_file(chunk, path)
            .map_err(|error| SyncError::Storage(error.to_string()))?;
    }
    store
        .verify_integrity()
        .map_err(|error| SyncError::Storage(error.to_string()))?;
    let restored_epoch = store
        .commit_restored_backup_manifest(&manifest)
        .map_err(|error| SyncError::Storage(error.to_string()))?;
    store
        .restore_durability_barrier()
        .await
        .map_err(|error| SyncError::Storage(error.to_string()))?;
    drop(store);
    files::atomic_json(&directory.join("restore-receipt.json"), &intent)?;
    Ok(CloudPurgeRootRestore {
        data,
        manifest,
        restored_epoch,
    })
}

async fn decode_source_metadata(
    root: &CloudReplacementRoot,
    objects: &[(super::protocol::CloudPurgeObject, PathBuf)],
    crypto: &dyn CryptoProvider,
    account_key: &[u8; 32],
) -> SyncResult<(BackupManifest, RecoveryDescriptorV1)> {
    let manifest_object = objects
        .iter()
        .find(|(object, _)| object.relative_key == root.manifest_key && object.role == "manifest")
        .ok_or_else(|| SyncError::Storage("replacement manifest ciphertext absent".into()))?;
    if manifest_object.0.bytes > 64 * 1024 * 1024 {
        return Err(SyncError::Storage(
            "replacement manifest exceeds its metadata bound".into(),
        ));
    }
    let plain = crypto
        .decrypt(&std::fs::read(&manifest_object.1)?)
        .await
        .map_err(|_| SyncError::Crypto("replacement manifest authentication failed".into()))?;
    let manifest: BackupManifest = serde_json::from_slice(&plain)
        .map_err(|_| SyncError::Storage("replacement manifest decode failed".into()))?;
    validate_manifest_chain(None, &manifest)
        .map_err(|error| SyncError::Storage(error.to_string()))?;
    if manifest_sha256_hex(&manifest).map_err(|error| SyncError::Storage(error.to_string()))?
        != root.manifest_sha256
        || manifest.store_uuid != root.store_uuid
        || cloud_db_hash_for_store_uuid(&manifest.store_uuid) != root.logical_root
        || manifest.previous_manifest_sha256.is_some()
        || !manifest.named_holes.is_empty()
        || !manifest.b2_cas_blob_refs.is_empty()
    {
        return Err(SyncError::Storage(
            "replacement manifest identity or independent source proof differs".into(),
        ));
    }
    let layout = objects
        .iter()
        .find(|(object, _)| {
            object.relative_key == root.layout_key
                && object.role == "layout"
                && object.sha256 == root.layout_sha256
        })
        .ok_or_else(|| SyncError::Storage("replacement layout ciphertext absent".into()))?;
    if layout.0.bytes > 65_536 {
        return Err(SyncError::Storage(
            "replacement layout exceeds its metadata bound".into(),
        ));
    }
    let name = format!(
        "lastdb-recovery-v1-{}-{}-{}.enc",
        root.logical_root, root.manifest_sha256, root.layout_sha256
    );
    let descriptor = RecoveryDescriptorV1::open(&name, &std::fs::read(&layout.1)?, account_key)
        .map_err(SyncError::Storage)?;
    if descriptor.store_uuid != manifest.store_uuid
        || descriptor.counter != manifest.counter
        || descriptor.epoch != manifest.epoch
    {
        return Err(SyncError::Storage(
            "replacement layout does not bind the exact manifest".into(),
        ));
    }
    Ok((manifest, descriptor))
}
