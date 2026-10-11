//! Durable org source cut from isolated scoped rows.

use super::*;
use crate::sync::cloud_purge::protocol::CloudAccount;
use crate::sync::cloud_purge::{org_export, RetainedSource};
use std::path::Path;

impl SyncEngine {
    pub async fn prepare_cloud_purge_org_source_held(
        &self,
        directory: &Path,
        account: CloudAccount,
        fence: &CloudPurgeWriteFence,
        ops: &crate::db_operations::DbOperations,
    ) -> SyncResult<RetainedSource> {
        fence.require_source(&self.device_id)?;
        let target = self.purge_org_target(&account).await?;
        let key = crate::sharing::org_sync_target::e2e_key_bytes(&target)
            .map_err(|error| SyncError::Storage(error.to_string()))?;
        let node_key = self
            .enc_key
            .as_ref()
            .ok_or_else(|| SyncError::Storage("selected node encryption key absent".into()))?;
        let packing = self.backup_publish_target.lock().await;
        if packing.is_some() {
            return Err(SyncError::Storage(
                "another snapshot holds the source rewrite lock".into(),
            ));
        }
        self.prepare_backup_restore_frontier().await?;
        let job_dir = directory
            .parent()
            .ok_or_else(|| SyncError::Storage("org source job parent absent".into()))?;
        let export_receipt = job_dir.join("org-export.json");
        let isolated = if export_receipt.exists() {
            reopen_export(&export_receipt, &key)?
        } else {
            self.complete_cloud_purge_org_file_closure(ops, &target, fence)
                .await?;
            let export_dir = job_dir.join(format!("org-source-{}", uuid::Uuid::new_v4()));
            let exported = crate::sync::capture::with_capture_suppressed(
                org_export::export_scoped_store_from_ops(ops, &target, &export_dir, node_key, &key),
            )
            .await?;
            save_export(&export_receipt, &export_dir, &exported.catalog)?;
            exported
        };
        isolated.catalog.validate(&target)?;
        let mut manifest = isolated
            .store
            .cut_backup_manifest_with_cloud_presence_strict(None, None)
            .map_err(|error| SyncError::Storage(error.to_string()))?;
        manifest.deletion_receipts.clear();
        let frontier = self.cloud_purge_source_frontier().await?;
        let crypto = crate::crypto::LocalCryptoProvider::from_key(key);
        let manifest_bytes =
            serde_json::to_vec(&manifest).map_err(|error| SyncError::Storage(error.to_string()))?;
        let manifest_cipher = crypto
            .encrypt(&manifest_bytes)
            .await
            .map_err(|_| SyncError::Crypto("org manifest encryption failed".into()))?;
        let layout = super::cloud_purge_selected::sealed_layout(&isolated.store, &manifest, &key)?;
        let candidates = super::cloud_purge_selected::bind_candidates(
            &manifest,
            isolated
                .store
                .enumerate_backup_publish_target_candidates()
                .map_err(|error| SyncError::Storage(error.to_string()))?,
        )?;
        let metadata = serde_json::to_vec(&isolated.catalog)
            .map_err(|error| SyncError::Storage(error.to_string()))?;
        let encrypted = crypto
            .encrypt(&metadata)
            .await
            .map_err(|_| SyncError::Crypto("org catalog encryption failed".into()))?;
        let source = RetainedSource::create(
            directory,
            account,
            crate::sync::cloud_purge::CloudSourceCut {
                local_only: false,
                source_id: self.device_id.clone(),
                manifest,
                manifest_ciphertext: &manifest_cipher,
                layout_bytes: &layout,
                candidates,
                writer_frontier: frontier,
                org_catalog_ciphertext: Some(&encrypted),
                shares: Vec::new(),
            },
        )?;
        drop(packing);
        Ok(source)
    }
}

fn save_export(
    path: &Path,
    directory: &Path,
    catalog: &org_export::OrgCatalogSource,
) -> SyncResult<()> {
    let bytes = serde_json::to_vec(
        &serde_json::json!({"format_version":1,"directory":directory,"catalog":catalog}),
    )
    .map_err(|error| SyncError::Storage(error.to_string()))?;
    let temporary = path.with_file_name(format!(".org-export-{}", uuid::Uuid::new_v4()));
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    use std::io::Write;
    let mut file = options.open(&temporary)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    std::fs::rename(temporary, path)?;
    std::fs::File::open(
        path.parent()
            .ok_or_else(|| SyncError::Storage("org export parent absent".into()))?,
    )?
    .sync_all()?;
    Ok(())
}

fn reopen_export(path: &Path, key: &[u8; 32]) -> SyncResult<org_export::ScopedExport> {
    let metadata = std::fs::symlink_metadata(path)?;
    if !metadata.file_type().is_file() || metadata.len() > 16 * 1024 * 1024 {
        return Err(SyncError::Storage("invalid org export receipt".into()));
    }
    let receipt: serde_json::Value = serde_json::from_slice(&std::fs::read(path)?)
        .map_err(|_| SyncError::Storage("invalid org export receipt".into()))?;
    let directory = receipt
        .get("directory")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| SyncError::Storage("org export directory absent".into()))?;
    let directory = Path::new(directory);
    if directory.parent() != path.parent()
        || !std::fs::symlink_metadata(directory)?.file_type().is_dir()
    {
        return Err(SyncError::Storage(
            "org export source lies outside its job".into(),
        ));
    }
    let catalog = serde_json::from_value(
        receipt
            .get("catalog")
            .cloned()
            .ok_or_else(|| SyncError::Storage("org export catalog absent".into()))?,
    )
    .map_err(|_| SyncError::Storage("invalid org export catalog".into()))?;
    let mut options = crate::storage::LastStoreNamespacedStore::product_hash_group_options();
    options.data_key = Some(*key);
    let store = Arc::new(
        crate::storage::LastStoreNamespacedStore::open_with_options_and_high_water_data_key(
            directory,
            options,
            crate::storage::laststore::high_water_path_for_store_root(directory),
        )
        .map_err(|error| SyncError::Storage(error.to_string()))?,
    );
    store
        .verify_integrity()
        .map_err(|error| SyncError::Storage(error.to_string()))?;
    Ok(org_export::ScopedExport { store, catalog })
}
