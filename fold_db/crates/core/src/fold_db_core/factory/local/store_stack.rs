use super::{store_seam_crypto, LocalSyncSetup};
use crate::crypto::keyring::Keyring;
use crate::crypto::E2eKeys;
use crate::error::FoldDbResult;
use crate::security::Ed25519KeyPair;
use crate::storage::traits::NamespacedStore;
use crate::storage::LastStoreNamespacedStore;
use crate::storage::StorageEngine;
use crate::storage::{EncryptingNamespacedStore, LASTSTORE_PLAINTEXT_NAMESPACES};
use std::sync::Arc;

pub(super) struct StoreStack {
    pub(super) store: Arc<dyn NamespacedStore>,
    pub(super) enc_store_ref: Option<Arc<EncryptingNamespacedStore>>,
    #[cfg(feature = "cloud-sync")]
    pub(super) mutation_log_capture: Arc<crate::sync::capture::MutationLogCaptureRouter>,
    #[cfg(feature = "cloud-sync")]
    pub(super) sync_engine: Option<Arc<crate::sync::SyncEngine>>,
    #[cfg(feature = "cloud-sync")]
    pub(super) sync_interval_ms: u64,
}

#[cfg_attr(not(feature = "cloud-sync"), allow(clippy::unused_async))]
#[allow(clippy::too_many_arguments)]
pub(super) async fn build_store_stack(
    base_store: Arc<dyn NamespacedStore>,
    laststore_backup_source: Option<Arc<LastStoreNamespacedStore>>,
    sync_setup: Option<LocalSyncSetup>,
    e2e_keys: &E2eKeys,
    signer: Arc<Ed25519KeyPair>,
    at_rest_keyring: Option<&Arc<Keyring>>,
    wrap_at_rest_seam: bool,
    storage_engine: StorageEngine,
    defer_cloud_workers: bool,
) -> FoldDbResult<StoreStack> {
    #[cfg(feature = "cloud-sync")]
    {
        if let Some(setup) = sync_setup {
            return build_syncing_store_stack(
                base_store,
                laststore_backup_source,
                setup,
                e2e_keys,
                signer,
                at_rest_keyring,
                wrap_at_rest_seam,
                storage_engine,
                defer_cloud_workers,
            )
            .await;
        }
    }

    #[cfg(not(feature = "cloud-sync"))]
    let _ = (
        sync_setup,
        signer,
        laststore_backup_source,
        defer_cloud_workers,
    );

    Ok(build_local_store_stack(
        base_store,
        e2e_keys,
        at_rest_keyring,
        wrap_at_rest_seam,
        storage_engine,
    ))
}

/// Build the encrypting at-rest seam over `base_store`.
fn build_encrypting_store(
    base_store: Arc<dyn NamespacedStore>,
    e2e_keys: &E2eKeys,
    at_rest_keyring: Option<&Arc<Keyring>>,
    plaintext_namespaces: &[&str],
) -> Arc<EncryptingNamespacedStore> {
    let crypto = store_seam_crypto(e2e_keys, at_rest_keyring);
    // Never `EncryptingNamespacedStore::new()` here — that constructor uses
    // the empty `PLAINTEXT_NAMESPACES` list and seals catalogs. An empty
    // caller list must still keep LastStore catalog collections plaintext
    // (2026-08-16 BoardCards ENC: brick).
    let list = if plaintext_namespaces.is_empty() {
        LASTSTORE_PLAINTEXT_NAMESPACES
    } else {
        plaintext_namespaces
    };
    Arc::new(EncryptingNamespacedStore::with_plaintext_namespaces(
        base_store,
        crypto,
        list.iter().map(|ns| (*ns).to_string()).collect(),
    ))
}

fn build_local_store_stack(
    base_store: Arc<dyn NamespacedStore>,
    e2e_keys: &E2eKeys,
    at_rest_keyring: Option<&Arc<Keyring>>,
    wrap_at_rest_seam: bool,
    _storage_engine: StorageEngine,
) -> StoreStack {
    #[cfg(feature = "cloud-sync")]
    let mutation_log_capture = Arc::new(crate::sync::capture::MutationLogCaptureRouter::default());
    if !wrap_at_rest_seam {
        #[cfg(feature = "cloud-sync")]
        let serving_store: Arc<dyn NamespacedStore> = Arc::new(
            crate::sync::capture::MutationLogCaptureNamespacedStore::new(
                Arc::clone(&base_store),
                Arc::clone(&mutation_log_capture),
            ),
        );
        #[cfg(not(feature = "cloud-sync"))]
        let serving_store = base_store;
        return StoreStack {
            store: serving_store,
            enc_store_ref: None,
            #[cfg(feature = "cloud-sync")]
            mutation_log_capture,
            #[cfg(feature = "cloud-sync")]
            sync_engine: None,
            #[cfg(feature = "cloud-sync")]
            sync_interval_ms: 0,
        };
    }

    let plaintext_namespaces = LASTSTORE_PLAINTEXT_NAMESPACES;
    let enc_store =
        build_encrypting_store(base_store, e2e_keys, at_rest_keyring, plaintext_namespaces);

    let engine_store = enc_store.clone() as Arc<dyn NamespacedStore>;
    #[cfg(feature = "cloud-sync")]
    let serving_store: Arc<dyn NamespacedStore> = Arc::new(
        crate::sync::capture::MutationLogCaptureNamespacedStore::new(
            engine_store,
            Arc::clone(&mutation_log_capture),
        ),
    );
    #[cfg(not(feature = "cloud-sync"))]
    let serving_store = engine_store;

    StoreStack {
        store: serving_store,
        enc_store_ref: Some(enc_store),
        #[cfg(feature = "cloud-sync")]
        mutation_log_capture,
        #[cfg(feature = "cloud-sync")]
        sync_engine: None,
        #[cfg(feature = "cloud-sync")]
        sync_interval_ms: 0,
    }
}

#[cfg(feature = "cloud-sync")]
#[allow(clippy::too_many_arguments)]
async fn build_syncing_store_stack(
    base_store: Arc<dyn NamespacedStore>,
    laststore_backup_source: Option<Arc<LastStoreNamespacedStore>>,
    setup: LocalSyncSetup,
    e2e_keys: &E2eKeys,
    signer: Arc<Ed25519KeyPair>,
    at_rest_keyring: Option<&Arc<Keyring>>,
    wrap_at_rest_seam: bool,
    _storage_engine: StorageEngine,
    defer_cloud_workers: bool,
) -> FoldDbResult<StoreStack> {
    let mut sync_config = setup.config.clone().unwrap_or_default();
    // Last Store: never dual-write the legacy `{user_hash}/log/{seq}` personal
    // export objects (those conflict with sealed-chunk / mutation-log planes).
    // Continuous product capture is MutationLog (design-lastdb-cloud-sync-
    // mutation-log-first Phase A): durable group-commit log while Cloud Sync
    // is on — not retired store-diff cold export, not pin-freeze-only.
    sync_config.legacy_personal_cloud_sync = false;
    // Product default is MutationLog, overridable by env so capture can be
    // switched off WITHOUT a rebuild.
    //
    // This was previously an unconditional assignment with no override. When
    // the durable pin log grew 138 MiB -> 10.45 GiB in a day on the primary and
    // started feeding daemon SIGKILLs, there was no way to stop capture short
    // of `lastdb cloud off` — which also stops backups, i.e. trading a growing
    // log for zero durability. A product mode with no off switch is a defect in
    // its own right; an operator must be able to disable a misbehaving
    // continuous path without shipping a binary.
    //
    // `LASTDB_CAPTURE_MODE=off` disables continuous capture. Anything else
    // (unset, "mutation_log", unrecognised) keeps the product default, so this
    // can only ever be used deliberately.
    sync_config.capture_mode = match std::env::var("LASTDB_CAPTURE_MODE")
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "off" => {
            tracing::warn!(
                target: "fold_db::sync",
                "LASTDB_CAPTURE_MODE=off — continuous mutation-log capture DISABLED; \
                 cloud durability falls back to sealed snapshots only"
            );
            crate::sync::engine::CaptureMode::Off
        }
        _ => crate::sync::engine::CaptureMode::MutationLog,
    };
    // Hold uploads so one write does not seal its own file. `0` on either
    // env var disables that knob. Unset or unparseable keeps the product
    // default. SyncConfig::default stays 0 so unit tests upload immediately.
    sync_config.mutation_log_coalesce_quiet_ms = parse_coalesce_ms(
        std::env::var("LASTDB_MUTATION_LOG_COALESCE_QUIET_MS")
            .ok()
            .as_deref(),
        1_000,
    );
    sync_config.mutation_log_coalesce_max_ms = parse_coalesce_ms(
        std::env::var("LASTDB_MUTATION_LOG_COALESCE_MAX_MS")
            .ok()
            .as_deref(),
        5_000,
    );
    tracing::info!(
        target: "fold_db::sync::mutation_log",
        quiet_ms = sync_config.mutation_log_coalesce_quiet_ms,
        max_hold_ms = sync_config.mutation_log_coalesce_max_ms,
        "mutation-log coalesce windows"
    );
    let interval_ms = sync_config.sync_interval_ms;
    let enc_store = wrap_at_rest_seam.then(|| {
        let plaintext_namespaces = LASTSTORE_PLAINTEXT_NAMESPACES;
        build_encrypting_store(
            Arc::clone(&base_store),
            e2e_keys,
            at_rest_keyring,
            plaintext_namespaces,
        )
    });
    let engine_store = enc_store.as_ref().map_or_else(
        || Arc::clone(&base_store),
        |enc| enc.clone() as Arc<dyn NamespacedStore>,
    );
    let engine = build_sync_engine(
        setup,
        sync_config,
        e2e_keys,
        signer,
        Arc::clone(&base_store),
        Arc::clone(&engine_store),
        laststore_backup_source,
    );

    // The factory can bootstrap an empty LastStore home with this serving
    // engine before it starts background sync. Load durable cursors first so
    // an existing home does not replay an old tail.
    engine.load_download_cursors().await;

    let mutation_log_capture = Arc::new(crate::sync::capture::MutationLogCaptureRouter::default());
    if !defer_cloud_workers {
        mutation_log_capture.set_engine(Arc::clone(&engine));
    }
    let serving_store: Arc<dyn NamespacedStore> = Arc::new(
        crate::sync::capture::MutationLogCaptureNamespacedStore::new(
            Arc::clone(&engine_store),
            Arc::clone(&mutation_log_capture),
        ),
    );

    Ok(StoreStack {
        store: serving_store,
        enc_store_ref: enc_store,
        mutation_log_capture,
        sync_engine: Some(engine),
        sync_interval_ms: interval_ms,
    })
}

#[cfg(feature = "cloud-sync")]
fn build_sync_engine(
    setup: LocalSyncSetup,
    sync_config: crate::sync::SyncConfig,
    e2e_keys: &E2eKeys,
    signer: Arc<Ed25519KeyPair>,
    base_store: Arc<dyn NamespacedStore>,
    engine_store: Arc<dyn NamespacedStore>,
    laststore_backup_source: Option<Arc<LastStoreNamespacedStore>>,
) -> Arc<crate::sync::SyncEngine> {
    use crate::crypto::{CryptoProvider, LocalCryptoProvider};

    // Cloud outer seal uses the account E2E content key. Mini personal at-rest
    // uses the same content key (`store_seam_crypto` with no keyring — product
    // rule design-portable-same-key-at-rest-cloud). Never use keyring store DEK
    // for cloud envelopes; never wire keyring store DEK on Mini host.
    let sync_crypto: Arc<dyn CryptoProvider> =
        Arc::new(LocalCryptoProvider::from_key(e2e_keys.encryption_key()));
    // trace-egress: propagate (shared with skip-s3 — see
    // docs/observability/egress-classification-notes.md). Connect-timeout the
    // shared client so a black-holed endpoint cannot wedge the TCP handshake;
    // per-request bounds live in S3Client/AuthClient.
    let http = Arc::new(crate::sync::build_shared_http_client());
    let s3 = crate::sync::s3::S3Client::new(http.clone());
    let db_hash = laststore_backup_source
        .as_ref()
        .and_then(|store| match store.cloud_db_hash() {
            Ok(db_hash) => db_hash,
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "could not derive LastStore cloud db_hash; falling back to principal-rooted sync auth requests"
                );
                None
            }
        });
    let auth =
        crate::sync::auth::AuthClient::new(http, setup.auth_url, setup.auth).with_db_hash(db_hash);

    let mut engine = crate::sync::SyncEngine::new_with_laststore_backup_source(
        setup.device_id,
        sync_crypto,
        s3,
        auth,
        engine_store,
        sync_config,
        signer,
        laststore_backup_source,
    );
    if let Some(cb) = setup.auth_refresh {
        engine.set_auth_refresh(cb);
    }
    engine.set_cursor_store(base_store);
    // Seal the download-cursor bookkeeping with the portable E2E content key
    // before it passes through the local at-rest seam.
    engine.set_at_rest_key(e2e_keys.encryption_key());

    // The local factory starts this worker only after boot migrations and
    // cloud photograph bootstrap. Construction must remain inert so an empty
    // home cannot CAS a cut that races restore.
    Arc::new(engine)
}

/// Product coalesce knob. `None` and an unparseable string keep `default_ms`.
/// `"0"` disables that knob.
#[cfg(feature = "cloud-sync")]
pub(crate) fn parse_coalesce_ms(raw: Option<&str>, default_ms: u64) -> u64 {
    match raw {
        Some(raw) => raw.trim().parse::<u64>().unwrap_or(default_ms),
        None => default_ms,
    }
}
