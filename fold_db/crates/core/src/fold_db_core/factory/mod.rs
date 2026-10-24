//! FoldDB factory — public constructors.
//!
//! Layout:
//! - [`local`] — native local construction and at-rest encryption
//! - [`boot`] — boot decryption error context
//!
//! [`create_fold_db`] uses the default local options.
//! [`create_fold_db_with_options`] names the optional encryption and Search inputs.

mod boot;
mod local;

use crate::crypto::keyring::Keyring;
use crate::crypto::E2eKeys;
use crate::error::{FoldDbError, FoldDbResult};
use crate::fold_db_core::FoldDB;
use crate::security::Ed25519KeyPair;
use crate::storage::config::{DatabaseConfig, StorageEngine};
use std::path::PathBuf;
use std::sync::Arc;

use local::create_local_fold_db;

/// Re-export: which key seals Mini personal at-rest data (`e2e_content_key` vs
/// `keyring_store_dek`). Mini production always logs / uses content key.
pub use local::at_rest_provider_label;

/// Local construction inputs. None of these options selects another engine.
#[derive(Default)]
pub struct FoldDbOptions {
    /// An embedded caller can supply a keyring for the CAS storage wrapper.
    /// Mini uses None so that wrapper uses the account content key.
    pub at_rest_keyring: Option<Arc<Keyring>>,
    /// Explicit Search inbox for the node home. None uses the existing
    /// environment-based Search inbox resolution.
    pub search_outbox_inbox: Option<PathBuf>,
}

/// Creates a fully initialized FoldDB instance based on the database configuration.
///
/// Always opens the native packed store. Cloud configuration fails before
/// the store opens; native cloud activation remains disabled.
///
/// `signer` is the Ed25519 keypair used to sign molecule mutations. Callers are
/// responsible for loading and validating it from the node's persistent identity
/// before calling this factory — passing a freshly-generated keypair on every
/// boot would produce signatures that do not match the node's public key.
///
/// For an at-rest keyring or explicit Search inbox, use
/// [`create_fold_db_with_options`].
pub async fn create_fold_db(
    config: &DatabaseConfig,
    e2e_keys: &E2eKeys,
    signer: Arc<Ed25519KeyPair>,
) -> FoldDbResult<Arc<FoldDB>> {
    create_fold_db_with_options(config, e2e_keys, signer, FoldDbOptions::default()).await
}

/// Open the native local engine with explicit local options.
/// Cloud intent is refused before any store or background worker opens.
pub async fn create_fold_db_with_options(
    config: &DatabaseConfig,
    e2e_keys: &E2eKeys,
    signer: Arc<Ed25519KeyPair>,
    options: FoldDbOptions,
) -> FoldDbResult<Arc<FoldDB>> {
    if config.cloud_sync.is_some() {
        return Err(FoldDbError::Config(FoldDB::CLOUD_SYNC_DISABLED.into()));
    }
    StorageEngine::from_env().map_err(FoldDbError::Config)?;

    create_local_fold_db(&config.path, e2e_keys, signer, options).await
}
/// Refuse the old local replay engine until native transfer activation exists.
pub fn create_fold_db_with_local_proof_engine(
    _config: &DatabaseConfig,
    _e2e_keys: &E2eKeys,
    _signer: Arc<Ed25519KeyPair>,
    _search_outbox_inbox: Option<PathBuf>,
) -> FoldDbResult<Arc<FoldDB>> {
    Err(FoldDbError::Config(
        "native LastDB local replay is disabled until its backup and cloud-sync gate is complete"
            .into(),
    ))
}
