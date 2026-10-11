//! Durable inactive source reuse, including legacy randomly minted bundles.

use super::*;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Serialize, Deserialize)]
struct SavedExport {
    format_version: u32,
    catalog: OrgCatalogImport,
    store_path: PathBuf,
}

pub(super) async fn prepare(
    source: &DbOperations,
    target: &OrgSyncTarget,
    directory: &Path,
    node_key: &[u8; 32],
) -> Result<(OrgCatalogImport, DbOperations), String> {
    let receipt = directory.join("export-ready.json");
    if receipt.exists() {
        let metadata = std::fs::symlink_metadata(&receipt).map_err(message)?;
        if !metadata.file_type().is_file() || metadata.len() > 16 * 1024 * 1024 {
            return Err("the preserved org export receipt is invalid".into());
        }
        let saved: SavedExport = serde_json::from_slice(&std::fs::read(&receipt).map_err(message)?)
            .map_err(|_| "the preserved org export receipt is invalid")?;
        if saved.format_version != 1 {
            return Err("unsupported preserved org export receipt".into());
        }
        saved.catalog.validate(target).map_err(message)?;
        let source_path = std::fs::canonicalize(&saved.store_path).map_err(message)?;
        let owner_path = std::fs::canonicalize(directory).map_err(message)?;
        if !source_path.starts_with(&owner_path) || source_path == owner_path {
            return Err("the preserved org source leaves its owned directory".into());
        }
        let mut options = LastStoreNamespacedStore::product_hash_group_options();
        options.data_key = Some(*node_key);
        let raw = Arc::new(
            LastStoreNamespacedStore::open_with_options(&source_path, options).map_err(message)?,
        );
        raw.verify_integrity().map_err(message)?;
        return Ok((saved.catalog, open_raw_export(raw, node_key).await?));
    }
    // A failed incomplete attempt never reached candidate import. Keep it
    // private and create a fresh source; completed sources always reuse above.
    let attempt = directory.join(uuid::Uuid::new_v4().to_string());
    let mut export_target = target.clone();
    export_target.active = true;
    let exported = fold_db::sync::cloud_purge::org_export::export_scoped_store_from_ops(
        source,
        &export_target,
        &attempt,
        node_key,
        node_key,
    )
    .await
    .map_err(message)?;
    let saved = SavedExport {
        format_version: 1,
        catalog: exported.catalog,
        store_path: attempt,
    };
    crate::cloud_purge_runtime::atomic(&receipt, &saved)?;
    Ok((
        saved.catalog,
        open_raw_export(exported.store, node_key).await?,
    ))
}

async fn open_raw_export(
    raw: Arc<LastStoreNamespacedStore>,
    node_key: &[u8; 32],
) -> Result<DbOperations, String> {
    let base: Arc<dyn NamespacedStore> = raw;
    let seam: Arc<dyn NamespacedStore> =
        Arc::new(EncryptingNamespacedStore::with_plaintext_namespaces(
            base,
            Arc::new(LocalCryptoProvider::from_key(*node_key)),
            LASTSTORE_PLAINTEXT_NAMESPACES
                .iter()
                .map(|value| (*value).to_string())
                .collect(),
        ));
    DbOperations::from_namespaced_store_with_atom_and_molecule_keys(
        seam,
        Some(*node_key),
        fold_db::atom::MoleculeKeyCodec::plain(),
        Some(*node_key),
    )
    .await
    .map_err(message)
}
