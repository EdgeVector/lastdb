//! Native local FoldDB construction, at-rest encryption binding, and boot error mapping.

use crate::crypto::E2eKeys;
use crate::db_operations::DbOperations;
use crate::error::{FoldDbError, FoldDbResult};
use crate::fold_db_core::FoldDB;
use crate::security::Ed25519KeyPair;
use crate::storage::config::DatabaseConfig;
use crate::storage::disk_pack::PackNamespacedStore;
use std::path::PathBuf;
use std::sync::Arc;

/// Local construction inputs.
#[derive(Default)]
pub struct FoldDbOptions {
    pub offline_maintenance: bool,
    pub search_outbox_inbox: Option<PathBuf>,
    /// Accepted publication stream. The normal sender sets this for an owned
    /// shareable named database, from creation and before cloud enrollment.
    /// Personal, legacy, received, and replay opens leave it false.
    pub capture_accepted: bool,
}

/// Creates a fully initialized FoldDB instance based on the database configuration.
pub async fn create_fold_db_with_options(
    config: &DatabaseConfig,
    e2e_keys: &E2eKeys,
    signer: Arc<Ed25519KeyPair>,
    options: FoldDbOptions,
) -> FoldDbResult<Arc<FoldDB>> {
    let key = e2e_keys.encryption_key();
    let store =
        PackNamespacedStore::open_keyed(&config.path, Some(&key), options.capture_accepted)?;
    let db_ops = DbOperations::from_namespaced_store_with_content_and_field_keys(store, key)
        .await
        .map_err(|e| FoldDbError::Config(e.to_string()))?;
    let fold_db = FoldDB::initialize(Arc::new(db_ops), signer, options)
        .await
        .map_err(|e| map_boot_decrypt_error(e.to_string()))?;
    Ok(Arc::new(fold_db))
}

fn map_boot_decrypt_error(msg: String) -> FoldDbError {
    let lower = msg.to_ascii_lowercase();
    let decrypt_fail = [
        "aead",
        "aes-gcm",
        "decrypt",
        "wrong encryption key",
        "wrong master key",
        "encryption error",
        "key mismatch",
        "undecryptable",
    ]
    .iter()
    .any(|pat| lower.contains(pat));
    if decrypt_fail {
        FoldDbError::Database(format!(
            "cannot decrypt existing store — wrong master key or incompatible build; \
             refusing to start fresh: {msg}"
        ))
    } else {
        FoldDbError::Config(msg)
    }
}
