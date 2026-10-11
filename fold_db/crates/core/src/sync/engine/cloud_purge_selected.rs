//! Fenced local source preparation and selected-device cloud access.

use super::*;
use crate::storage::laststore::{
    cloud_db_hash_for_store_uuid, manifest_sha256_hex, BackupManifest,
};
use crate::sync::cloud_purge::protocol::{CloudAccount, CloudAccountKind};
use crate::sync::cloud_purge::RetainedSource;
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

/// The runtime retains this guard from the source cut through local adoption.
/// Startup recovery must reacquire it before accepting mutation requests.
pub struct CloudPurgeWriteFence {
    _guard: tokio::sync::OwnedRwLockWriteGuard<()>,
    source_id: String,
}

impl CloudPurgeWriteFence {
    pub(super) fn require_source(&self, source: &str) -> SyncResult<()> {
        if self.source_id != source {
            return Err(SyncError::Storage(
                "selected mutation fence belongs to another source".into(),
            ));
        }
        Ok(())
    }
}

impl SyncEngine {
    pub fn cloud_purge_auth(&self) -> AuthClient {
        self.auth.clone()
    }

    pub async fn acquire_cloud_purge_write_fence(&self) -> SyncResult<CloudPurgeWriteFence> {
        let router = self
            .photograph_mutation_router
            .lock()
            .await
            .clone()
            .ok_or_else(|| {
                SyncError::Storage(
                    "selected source requires a serving mutation admission fence".into(),
                )
            })?;
        if router.current_task_has_mutation_admission() {
            return Err(SyncError::CloudPurgeHeld(
                "cloud adoption waits until the admitted local operation finishes".into(),
            ));
        }
        Ok(CloudPurgeWriteFence {
            _guard: router.fence_mutations().await,
            source_id: self.device_id.clone(),
        })
    }

    pub async fn restore_cloud_purge_replacement(
        &self,
        control: &crate::sync::cloud_purge::protocol::CloudPurgeControl,
        root: &crate::sync::cloud_purge::protocol::CloudReplacementRoot,
        directory: &Path,
    ) -> SyncResult<std::path::PathBuf> {
        let auth = match self.cloud_purge_peer() {
            Some(peer) => self
                .auth
                .for_cloud_adoption_reads(&peer.prepare_adoption(control)?),
            None => self.auth.clone(),
        };
        if control.account.kind == CloudAccountKind::Org {
            let target = self.purge_org_target(&control.account).await?;
            let key = crate::sharing::org_sync_target::e2e_key_bytes(&target)
                .map_err(|error| SyncError::Storage(error.to_string()))?;
            let crypto = crate::crypto::LocalCryptoProvider::from_key(key);
            return crate::sync::cloud_purge::restore_cloud_purge_root(
                &auth, control, root, directory, &crypto, &key,
            )
            .await;
        }
        let key = self.enc_key.as_ref().ok_or_else(|| {
            SyncError::Storage("replacement account encryption key absent".into())
        })?;
        crate::sync::cloud_purge::restore_cloud_purge_root(
            &auth,
            control,
            root,
            directory,
            self.crypto.as_ref(),
            key,
        )
        .await
    }

    pub(super) async fn purge_org_target(
        &self,
        account: &CloudAccount,
    ) -> SyncResult<crate::sharing::org_sync_target::OrgSyncTarget> {
        let store = self.store.open_namespace("org_sync_targets").await?;
        let bytes = store
            .get(format!("org_sync:{}", account.id).as_bytes())
            .await?
            .ok_or_else(|| {
                SyncError::Storage("selected org is not registered on this device".into())
            })?;
        let target: crate::sharing::org_sync_target::OrgSyncTarget = serde_json::from_slice(&bytes)
            .map_err(|_| SyncError::Storage("invalid selected org registry".into()))?;
        if target.org_hash != account.id || !target.active {
            return Err(SyncError::Storage(
                "selected org registry identity differs".into(),
            ));
        }
        Ok(target)
    }

    pub(super) async fn cloud_purge_source_frontier(&self) -> SyncResult<BTreeMap<String, u64>> {
        let mut frontier = self
            .pin_log
            .incorporated_frontier()
            .await
            .as_writer_hwm(Some(&self.device_id));
        let local = super::pin_log::primary_resume_durable_local_frontier(
            self.cursor_store.as_ref(),
            &self.device_id,
        )
        .await
        .map_err(SyncError::Storage)?;
        frontier
            .entry(self.device_id.clone())
            .and_modify(|value| *value = (*value).max(local))
            .or_insert(local);
        Ok(frontier)
    }

    pub async fn prepare_cloud_purge_source_held(
        &self,
        directory: &Path,
        account: CloudAccount,
        fence: &CloudPurgeWriteFence,
        ops: &crate::db_operations::DbOperations,
    ) -> SyncResult<RetainedSource> {
        if fence.source_id != self.device_id {
            return Err(SyncError::Storage(
                "selected mutation fence belongs to another source".into(),
            ));
        }
        if account.kind != CloudAccountKind::Personal {
            return Err(SyncError::Storage(
                "org preparation requires its isolated catalog export".into(),
            ));
        }
        let source = self
            .laststore_backup_source
            .as_ref()
            .ok_or_else(|| SyncError::Storage("purge source requires LastStore".into()))?;
        let packing = self.backup_publish_target.lock().await;
        if packing.is_some() {
            return Err(SyncError::Storage(
                "another snapshot holds the source rewrite lock".into(),
            ));
        }
        self.prepare_backup_restore_frontier().await?;
        let frontier = self.cloud_purge_source_frontier().await?;
        let shares = self.cloud_purge_owned_share_cuts(ops, directory).await?;
        let mut manifest = source
            .cut_backup_manifest_with_cloud_presence_strict(None, None)
            .map_err(|error| SyncError::Storage(error.to_string()))?;
        manifest.deletion_receipts.clear();
        let copy = source
            .atom_photograph_copy_report(&manifest, None, true)
            .map_err(|error| SyncError::Storage(error.to_string()))?;
        if !copy.is_complete() {
            return Err(SyncError::Storage(
                "purge cut lacks exact local atom bytes".into(),
            ));
        }
        let candidates = bind_candidates(
            &manifest,
            source
                .enumerate_backup_publish_target_candidates()
                .map_err(|error| SyncError::Storage(error.to_string()))?,
        )?;
        let key = self.enc_key.as_ref().ok_or_else(|| {
            SyncError::Storage("purge recovery requires the account encryption key".into())
        })?;
        let layout_bytes = sealed_layout(source, &manifest, key)?;
        let raw =
            serde_json::to_vec(&manifest).map_err(|error| SyncError::Storage(error.to_string()))?;
        let encrypted = self
            .crypto
            .encrypt(&raw)
            .await
            .map_err(|_| SyncError::Crypto("purge manifest encryption failed".into()))?;
        let retained = RetainedSource::create(
            directory,
            account,
            crate::sync::cloud_purge::CloudSourceCut {
                local_only: false,
                source_id: self.device_id.clone(),
                manifest,
                manifest_ciphertext: &encrypted,
                layout_bytes: &layout_bytes,
                candidates,
                writer_frontier: frontier,
                org_catalog_ciphertext: None,
                shares,
            },
        )?;
        drop(packing);
        Ok(retained)
    }
}

pub(super) fn bind_candidates(
    manifest: &BackupManifest,
    candidates: Vec<crate::storage::laststore::BackupChunkUploadCandidate>,
) -> SyncResult<Vec<crate::storage::laststore::BackupChunkUploadCandidate>> {
    let refs: BTreeMap<_, _> = manifest
        .mutable_chunks
        .iter()
        .chain(&manifest.atom_chunks)
        .map(|chunk| {
            (
                (
                    chunk.collection.clone(),
                    chunk.shard,
                    chunk.group_id,
                    chunk.chunk_uuid.clone(),
                ),
                chunk.clone(),
            )
        })
        .collect();
    let mut found = BTreeSet::new();
    let mut selected = Vec::new();
    for mut candidate in candidates {
        let coordinate = (
            candidate.chunk.collection.clone(),
            candidate.chunk.shard,
            candidate.chunk.group_id,
            candidate.chunk.chunk_uuid.clone(),
        );
        if let Some(chunk) = refs.get(&coordinate) {
            candidate.chunk = chunk.clone();
            found.insert(coordinate);
            selected.push(candidate);
        }
    }
    if found.len() != refs.len() {
        return Err(SyncError::Storage(
            "fresh cut contains non-local files".into(),
        ));
    }
    Ok(selected)
}

pub(super) fn sealed_layout(
    source: &crate::storage::laststore::LastStoreNamespacedStore,
    manifest: &BackupManifest,
    key: &[u8; 32],
) -> SyncResult<Vec<u8>> {
    let options = source.options();
    let layout = laststore::LayoutDescriptor {
        layout_mode: options.layout_mode,
        shard_bits: options.shard_bits,
        hash_group_bits: options.hash_group_bits,
        hash_algo: options.hash_algo,
        hash_group_key: options.hash_group_key,
        hash_group_partition_fanout: options.hash_group_partition_fanout,
        layout_epoch: options.layout_epoch,
        packaging: options.packaging,
    };
    let digest =
        manifest_sha256_hex(manifest).map_err(|error| SyncError::Storage(error.to_string()))?;
    let descriptor = RecoveryDescriptorV1::new(
        &manifest.store_uuid,
        &cloud_db_hash_for_store_uuid(&manifest.store_uuid),
        layout,
        &digest,
        manifest.counter,
        manifest.epoch,
    )
    .map_err(SyncError::Storage)?;
    descriptor
        .seal(key)
        .map(|(_, bytes)| bytes)
        .map_err(SyncError::Storage)
}
