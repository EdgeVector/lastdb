//! FoldDB factory — public constructors.
//!
//! Layout:
//! - [`local`] — native local construction and at-rest encryption
//! - [`boot`] — boot decrypt proof
//!
//! Two public entry points:
//! - [`create_fold_db`] — simple path (no auth-refresh / no keyring)
//! - [`create_fold_db_with_pool_and_auth_refresh`] — full path for Mini host
//!   (optional auth-refresh callback, optional keyring; pool arg removed with sled)

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

use local::{create_local_fold_db, LocalFoldDbOptions};

/// Re-export: which key seals Mini personal at-rest data (`e2e_content_key` vs
/// `keyring_store_dek`). Mini production always logs / uses content key.
pub use local::at_rest_provider_label;

type AuthRefreshCallback = crate::sync::AuthRefreshCallback;

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
/// For an at-rest keyring, use
/// [`create_fold_db_with_pool_and_auth_refresh`].
pub async fn create_fold_db(
    config: &DatabaseConfig,
    e2e_keys: &E2eKeys,
    signer: Arc<Ed25519KeyPair>,
) -> FoldDbResult<Arc<FoldDB>> {
    create_fold_db_with_pool_and_auth_refresh(config, e2e_keys, signer, None, None).await
}

/// Factory entry with an optional unlocked [`Keyring`] for local encryption.
/// The callback argument remains inert while native cloud activation is off.
///
/// `at_rest_keyring`, when `Some`, swaps the `main`/`metadata` encrypting seam
/// from the single-key [`LocalCryptoProvider`] to the keyring-backed
/// [`KeyringCryptoProvider`] (envelope v2, key_id-stamped, current-key-only).
///
/// **Mini production must pass `None`:** personal at-rest uses the account
/// E2E content key (portable every device; same material as cloud outer seal).
/// See [`local::at_rest_provider_label`] and
/// `design-portable-same-key-at-rest-cloud`. Keyring store DEK is non-Mini /
/// test only. Core never opens the keychain itself.
///
/// The historical `pool` argument (SledPool reuse) was removed with sled.
pub async fn create_fold_db_with_pool_and_auth_refresh(
    config: &DatabaseConfig,
    e2e_keys: &E2eKeys,
    signer: Arc<Ed25519KeyPair>,
    auth_refresh: Option<AuthRefreshCallback>,
    at_rest_keyring: Option<Arc<Keyring>>,
) -> FoldDbResult<Arc<FoldDB>> {
    create_fold_db_with_pool_auth_refresh_and_search_outbox(
        config,
        e2e_keys,
        signer,
        auth_refresh,
        at_rest_keyring,
        None,
        false,
    )
    .await
}

/// Full factory entry with an explicit Search app inbox path.
///
/// Mini `Host::boot(home)` uses this to keep Search outbox delivery scoped to
/// the booted home without publishing `home` through process-global
/// `LASTDB_HOME`. Existing factory callers keep the environment-driven fallback
/// by using [`create_fold_db_with_pool_and_auth_refresh`].
pub async fn create_fold_db_with_pool_auth_refresh_and_search_outbox(
    config: &DatabaseConfig,
    e2e_keys: &E2eKeys,
    signer: Arc<Ed25519KeyPair>,
    _auth_refresh: Option<AuthRefreshCallback>,
    at_rest_keyring: Option<Arc<Keyring>>,
    search_outbox_inbox: Option<PathBuf>,
    _defer_cloud_workers: bool,
) -> FoldDbResult<Arc<FoldDB>> {
    if config.cloud_sync.is_some() {
        return Err(FoldDbError::Config(FoldDB::CLOUD_SYNC_DISABLED.into()));
    }
    resolve_storage_engine(config)?;

    let db = create_local_fold_db(
        &config.path,
        e2e_keys,
        signer,
        LocalFoldDbOptions {
            at_rest_keyring,
            search_outbox_inbox,
        },
    )
    .await?;

    // NOTE: cloud sync config (api_url / user_hash) is NOT mirrored into the
    // node_config store. node_config.json (`database.cloud_sync`) is the
    // single source of truth for cloud intent (design-canonical-cloud-auth L2).

    Ok(db)
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

fn resolve_storage_engine(config: &DatabaseConfig) -> FoldDbResult<StorageEngine> {
    StorageEngine::from_env()
        .map_err(FoldDbError::Config)
        .map(|env_engine| env_engine.unwrap_or(config.engine))
}
