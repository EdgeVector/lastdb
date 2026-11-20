//! Open a FoldDB at a data directory from an Ed25519 seed.

use std::path::Path;
use std::sync::Arc;

use fold_db::crypto::E2eKeys;
use fold_db::fold_db_core::factory::{create_fold_db_with_options, FoldDbOptions};
use fold_db::fold_db_core::FoldDB;
use fold_db::security::Ed25519KeyPair;
use fold_db::storage::config::DatabaseConfig;

/// Open (or create) the store at `data_dir`, keyed from `seed`.
pub async fn open_store(
    data_dir: &Path,
    seed: &[u8; 32],
    capture_accepted: bool,
) -> Result<Arc<FoldDB>, String> {
    let keys = E2eKeys::from_ed25519_seed(seed).map_err(|e| format!("derive store keys: {e}"))?;
    let signer = Arc::new(
        Ed25519KeyPair::from_secret_key(seed).map_err(|e| format!("derive store signer: {e}"))?,
    );
    std::fs::create_dir_all(data_dir).map_err(|e| format!("create {}: {e}", data_dir.display()))?;
    create_fold_db_with_options(
        &DatabaseConfig::local(data_dir.to_path_buf()),
        &keys,
        signer,
        FoldDbOptions {
            offline_maintenance: false,
            capture_accepted,
            record_log: None,
        },
    )
    .await
    .map_err(|e| format!("open store {}: {e}", data_dir.display()))
}

/// Open a store for a proof run: no accepted-stream capture.
pub async fn open_proof_store(data_dir: &Path, seed: &[u8; 32]) -> Result<Arc<FoldDB>, String> {
    open_store(data_dir, seed, false).await
}
