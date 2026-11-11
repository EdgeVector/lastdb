//! FoldDB factory — public local constructors and boot error mapping.

mod local;

use crate::crypto::E2eKeys;
use crate::error::{FoldDbError, FoldDbResult};
use crate::fold_db_core::FoldDB;
use crate::security::Ed25519KeyPair;
use crate::storage::config::DatabaseConfig;
use std::path::PathBuf;
use std::sync::Arc;

/// Local construction inputs.
#[derive(Default)]
pub struct FoldDbOptions {
    pub offline_maintenance: bool,
    pub search_outbox_inbox: Option<PathBuf>,
}

/// Creates a fully initialized FoldDB instance based on the database configuration.
pub async fn create_fold_db_with_options(
    config: &DatabaseConfig,
    e2e_keys: &E2eKeys,
    signer: Arc<Ed25519KeyPair>,
    options: FoldDbOptions,
) -> FoldDbResult<Arc<FoldDB>> {
    local::create_local_fold_db(&config.path, e2e_keys, signer, options).await
}

pub(self) fn map_boot_decrypt_error(msg: String) -> FoldDbError {
    let lower = msg.to_ascii_lowercase();
    let decrypt_fail = lower.contains("aead")
        || lower.contains("aes-gcm")
        || lower.contains("decrypt")
        || lower.contains("wrong encryption key")
        || lower.contains("wrong master key")
        || lower.contains("encryption error")
        || lower.contains("key mismatch")
        || lower.contains("undecryptable");
    if decrypt_fail {
        FoldDbError::Database(format!(
            "cannot decrypt existing store — wrong master key or incompatible build; \
             refusing to start fresh: {msg}"
        ))
    } else {
        FoldDbError::Config(msg)
    }
}
