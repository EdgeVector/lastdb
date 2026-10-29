//! FoldDB factory — public constructors.
//!
//! Layout:
//! - [`local`] — native local construction and at-rest encryption
//! - [`boot`] — boot decryption error context
//!
//! [`create_fold_db`] uses the default local options.
//! [`create_fold_db_with_options`] names offline-maintenance and Search inputs.

mod boot;
mod local;

use crate::crypto::E2eKeys;
use crate::error::{FoldDbError, FoldDbResult};
use crate::fold_db_core::FoldDB;
use crate::security::Ed25519KeyPair;
use crate::storage::config::{DatabaseConfig, StorageEngine};
use std::path::PathBuf;
use std::sync::Arc;

use local::create_local_fold_db;

/// Local construction inputs. None of these options selects another engine.
#[derive(Default)]
pub struct FoldDbOptions {
    /// Open without periodic writers. The offline owner retains the home lock.
    pub offline_maintenance: bool,
    /// Explicit Search inbox for the node home. None uses the existing
    /// environment-based Search inbox resolution.
    pub search_outbox_inbox: Option<PathBuf>,
}

/// Creates a fully initialized FoldDB instance based on the database configuration.
///
/// Always opens the native packed store.
///
/// `signer` is the Ed25519 keypair used to sign molecule mutations. Callers are
/// responsible for loading and validating it from the node's persistent identity
/// before calling this factory — passing a freshly-generated keypair on every
/// boot would produce signatures that do not match the node's public key.
///
/// For offline maintenance or an explicit Search inbox, use
/// [`create_fold_db_with_options`].
pub async fn create_fold_db(
    config: &DatabaseConfig,
    e2e_keys: &E2eKeys,
    signer: Arc<Ed25519KeyPair>,
) -> FoldDbResult<Arc<FoldDB>> {
    create_fold_db_with_options(config, e2e_keys, signer, FoldDbOptions::default()).await
}

/// Open the native local engine with explicit local options.
pub async fn create_fold_db_with_options(
    config: &DatabaseConfig,
    e2e_keys: &E2eKeys,
    signer: Arc<Ed25519KeyPair>,
    options: FoldDbOptions,
) -> FoldDbResult<Arc<FoldDB>> {
    StorageEngine::from_env().map_err(FoldDbError::Config)?;

    create_local_fold_db(&config.path, e2e_keys, signer, options).await
}
