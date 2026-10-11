//! Independent local-only recovery cut for unchanged-cloud continuation.

use super::*;
use crate::sync::cloud_purge::{
    protocol::{CloudAccount, CloudAccountKind},
    CloudSourceCut, RetainedSource,
};
use std::path::Path;

impl SyncEngine {
    pub async fn retain_cloud_purge_local_source_held(
        &self,
        directory: &Path,
        account: CloudAccount,
        fence: &CloudPurgeWriteFence,
    ) -> SyncResult<RetainedSource> {
        fence.require_source(&self.device_id)?;
        if account.kind != CloudAccountKind::Personal {
            return Err(SyncError::Storage(
                "full local retention requires the local personal account identity".into(),
            ));
        }
        if directory.join("source.json").exists() {
            let source = RetainedSource::reopen(directory)?;
            if !source.local_only
                || source.account != account
                || source.prepared.source_id != self.device_id
            {
                return Err(SyncError::Storage(
                    "local recovery source retry identity differs".into(),
                ));
            }
            return Ok(source);
        }
        let store = self
            .laststore_backup_source
            .as_ref()
            .ok_or_else(|| SyncError::Storage("local recovery requires LastStore".into()))?;
        let packing = self.backup_publish_target.lock().await;
        if packing.is_some() {
            return Err(SyncError::Storage(
                "another snapshot holds the local source rewrite lock".into(),
            ));
        }
        self.prepare_backup_restore_frontier().await?;
        let frontier = self.cloud_purge_source_frontier().await?;
        let (manifest, candidates) = store
            .cut_local_recovery_manifest()
            .map_err(|error| SyncError::Storage(error.to_string()))?;
        let key = self
            .enc_key
            .as_ref()
            .ok_or_else(|| SyncError::Storage("local recovery encryption key absent".into()))?;
        let layout = super::cloud_purge_selected::sealed_layout(store, &manifest, key)?;
        let plain =
            serde_json::to_vec(&manifest).map_err(|error| SyncError::Storage(error.to_string()))?;
        let encrypted =
            self.crypto.encrypt(&plain).await.map_err(|_| {
                SyncError::Crypto("local recovery manifest encryption failed".into())
            })?;
        let source = RetainedSource::create(
            directory,
            account,
            CloudSourceCut {
                local_only: true,
                source_id: self.device_id.clone(),
                manifest,
                manifest_ciphertext: &encrypted,
                layout_bytes: &layout,
                candidates,
                writer_frontier: frontier,
                org_catalog_ciphertext: None,
                shares: Vec::new(),
            },
        )?;
        drop(packing);
        Ok(source)
    }
}
