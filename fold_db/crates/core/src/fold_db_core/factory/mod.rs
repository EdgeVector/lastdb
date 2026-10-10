//! FoldDB factory — public constructors.
//!
//! Layout:
//! - [`local`] — local Last Store construction + at-rest seam crypto
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

use local::{create_local_fold_db, LocalFoldDbOptions, LocalSyncSetup};

/// Re-export: which key seals Mini personal at-rest data (`e2e_content_key` vs
/// `keyring_store_dek`). Mini production always logs / uses content key.
pub use local::at_rest_provider_label;

#[cfg(feature = "cloud-sync")]
type AuthRefreshCallback = crate::sync::AuthRefreshCallback;
#[cfg(not(feature = "cloud-sync"))]
type AuthRefreshCallback = ();

/// Creates a fully initialized FoldDB instance based on the database configuration.
///
/// Always opens Last Store for the primary namespaced path. When `cloud_sync`
/// is configured, layers on encrypted S3 sync via the Exemem platform.
///
/// `signer` is the Ed25519 keypair used to sign molecule mutations. Callers are
/// responsible for loading and validating it from the node's persistent identity
/// before calling this factory — passing a freshly-generated keypair on every
/// boot would produce signatures that do not match the node's public key.
///
/// For auth-refresh or at-rest keyring, use
/// [`create_fold_db_with_pool_and_auth_refresh`].
pub async fn create_fold_db(
    config: &DatabaseConfig,
    e2e_keys: &E2eKeys,
    signer: Arc<Ed25519KeyPair>,
) -> FoldDbResult<Arc<FoldDB>> {
    create_fold_db_with_pool_and_auth_refresh(config, e2e_keys, signer, None, None).await
}

/// Full factory entry: auth-refresh callback and unlocked wrapped-DEK
/// [`Keyring`] for the at-rest seam.
///
/// When cloud sync is enabled, `auth_refresh` is invoked on 401 errors to obtain
/// fresh credentials (e.g., by re-registering with the Exemem API using the
/// node's Ed25519 keypair). The sync engine retries once after a successful
/// refresh.
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
    auth_refresh: Option<AuthRefreshCallback>,
    at_rest_keyring: Option<Arc<Keyring>>,
    search_outbox_inbox: Option<PathBuf>,
    defer_cloud_workers: bool,
) -> FoldDbResult<Arc<FoldDB>> {
    let sync_setup = build_sync_setup(config, auth_refresh)?;
    let storage_engine = resolve_storage_engine(config)?;

    let db = create_local_fold_db(
        &config.path,
        e2e_keys,
        signer,
        LocalFoldDbOptions {
            sync_setup,
            at_rest_keyring,
            storage_engine,
            search_outbox_inbox,
            defer_cloud_workers,
        },
    )
    .await?;

    // NOTE: cloud sync config (api_url / user_hash) is NOT mirrored into the
    // node_config store. node_config.json (`database.cloud_sync`) is the
    // single source of truth for cloud intent (design-canonical-cloud-auth L2).

    Ok(db)
}

fn resolve_storage_engine(config: &DatabaseConfig) -> FoldDbResult<StorageEngine> {
    StorageEngine::from_env()
        .map_err(FoldDbError::Config)
        .map(|env_engine| env_engine.unwrap_or(config.engine))
}

#[cfg(feature = "cloud-sync")]
fn build_sync_setup(
    config: &DatabaseConfig,
    auth_refresh: Option<AuthRefreshCallback>,
) -> FoldDbResult<Option<LocalSyncSetup>> {
    if let Some(cloud) = &config.cloud_sync {
        let path_str = config
            .path
            .to_str()
            .ok_or_else(|| FoldDbError::Config("Invalid storage path".to_string()))?;
        let mut setup =
            crate::sync::SyncSetup::from_exemem(&cloud.api_url, &cloud.api_key, path_str);
        setup.auth_refresh = auth_refresh;
        Ok(Some(setup))
    } else {
        Ok(None)
    }
}

#[cfg(not(feature = "cloud-sync"))]
fn build_sync_setup(
    config: &DatabaseConfig,
    _auth_refresh: Option<AuthRefreshCallback>,
) -> FoldDbResult<Option<LocalSyncSetup>> {
    if config.cloud_sync.is_some() {
        return Err(FoldDbError::Config(
            "fold_db was built without the cloud-sync feature".to_string(),
        ));
    }
    Ok(None)
}
