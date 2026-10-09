//! Destination preparation, store open, cloud clients and sync engine wiring.

use super::*;

/// Create the destination home, copy the identity seed and the paused cloud
/// configuration, and return the destination `data` directory.
pub(crate) fn prepare_destination(
    homes: &Homes,
    source: &Source,
    identity: &Identity,
) -> Phase<PathBuf> {
    let target = &homes.target;
    std::fs::create_dir_all(target).map_err(|e| {
        io_failure(
            Stage::PrepareDestination,
            format!("create {}: {e}", target.display()),
        )
    })?;
    write_owner_only_local(
        &target.join(lastdb_node::host::IDENTITY_KEY_FILE),
        &identity.seed,
    )
    .map_err(|detail| io_failure(Stage::PrepareDestination, detail))?;
    let cloud_bytes = std::fs::read(&source.cloud_path).map_err(|e| {
        io_failure(
            Stage::PrepareDestination,
            format!("read {}: {e}", source.cloud_path.display()),
        )
    })?;
    // Every incomplete restore stays Off, including a failed normal tail replay.
    write_owner_only_local(&homes.target_paused_cloud, &cloud_bytes)
        .map_err(|detail| io_failure(Stage::PrepareDestination, detail))?;
    lastdb_node::cloud::mark_cloud_resume_required(target)
        .map_err(|detail| io_failure(Stage::PrepareDestination, detail))?;

    let data_path = target.join("data");
    std::fs::create_dir_all(&data_path).map_err(|e| {
        io_failure(
            Stage::PrepareDestination,
            format!("create {}: {e}", data_path.display()),
        )
    })?;
    Ok(data_path)
}

/// Open the destination LastStore.
///
/// Backup chunks keep the source shard/group addresses. The destination
/// descriptor must therefore match the source descriptor before any chunk
/// is installed. Reading the descriptor does not open or lock the live
/// source store. Existing destination descriptors still win through
/// open_existing_or_with, which keeps mutation-log resume stable.
pub(crate) fn open_destination_store(
    homes: &Homes,
    identity: &Identity,
    remote_descriptor: Option<&RemoteRecovery>,
    data_path: &Path,
) -> Phase<std::sync::Arc<fold_db::storage::LastStoreNamespacedStore>> {
    let frame_aead = env_flag::var_truthy("LASTDB_RESTORE_FRAME_AEAD");
    let opts = if let Some(recovery) = remote_descriptor {
        recovery
            .descriptor
            .to_options(&identity.e2e.encryption_key(), frame_aead)
            .map_err(|detail| {
                let code = if detail == "frame AEAD recovery layout requires opt-in" {
                    Code::FrameAeadOptInRequired
                } else {
                    Code::LayoutMismatch
                };
                RestoreFailure::new(Stage::SourceLayout, code, detail)
            })?
    } else {
        restore_options_from_source_layout(
            &homes.source.join("data"),
            identity.e2e.encryption_key(),
            frame_aead,
        )?
    };
    let high_water = homes.target.join("laststore_high_water.json");
    let (opened, label) = if opts.packaging == laststore::PackagingMode::FrameAead {
        (
            fold_db::storage::LastStoreNamespacedStore::open_with_options_and_high_water_data_key(
                data_path, opts, high_water,
            ),
            "open target LastStore (frame_aead)",
        )
    } else {
        (
            fold_db::storage::LastStoreNamespacedStore::open_with_options_and_high_water(
                data_path, opts, high_water,
            ),
            "open target LastStore",
        )
    };
    let store = opened.map_err(|e| {
        RestoreFailure::new(
            Stage::OpenDestination,
            Code::OperationFailed,
            format!("{label}: {e}"),
        )
    })?;
    Ok(std::sync::Arc::new(store))
}

pub(crate) struct CloudClients {
    pub(crate) runtime: tokio::runtime::Runtime,
    pub(crate) s3: fold_db::sync::s3::S3Client,
    pub(crate) auth: fold_db::sync::auth::AuthClient,
}

pub(crate) fn build_cloud_clients(
    identity: &Identity,
    source_db_hash: &str,
) -> Phase<CloudClients> {
    let runtime = build_runtime()?;
    let http = std::sync::Arc::new(fold_db::sync::build_shared_http_client());
    let s3 = fold_db::sync::s3::S3Client::new(std::sync::Arc::clone(&http));
    // Publish writes `{cloud_db_hash(source store_uuid)}/backup/latest`. A
    // restore AuthClient with no db_hash looks under the authenticated
    // user_hash instead and reports "backup latest pointer missing".
    let auth = fold_db::sync::auth::AuthClient::new(
        http,
        identity.url.clone(),
        fold_db::sync::auth::SyncAuth::ApiKey(identity.api_key.clone()),
    )
    .with_db_hash(Some(source_db_hash.to_string()))
    .without_db_auto_claim();
    Ok(CloudClients { runtime, s3, auth })
}

pub(crate) struct Engine {
    pub(crate) engine: std::sync::Arc<fold_db::sync::SyncEngine>,
    pub(crate) signer: std::sync::Arc<fold_db::security::Ed25519KeyPair>,
}

pub(crate) fn build_engine(
    identity: &Identity,
    clients: &CloudClients,
    store: &std::sync::Arc<fold_db::storage::LastStoreNamespacedStore>,
) -> Phase<Engine> {
    let e2e = &identity.e2e;
    // Cloud outer seal uses the account E2E content key (same key that sealed
    // continuous mutation-log segments). After S0 chunks land, apply the
    // continuous plane so post-S0 history is not write-only.
    let sync_crypto: std::sync::Arc<dyn fold_db::crypto::CryptoProvider> = std::sync::Arc::new(
        fold_db::crypto::LocalCryptoProvider::from_key(e2e.encryption_key()),
    );
    // Mutation-log replay must write through the local at-rest seam, exactly as
    // the daemon wires it (`fold_db_core::factory::local::store_stack`: the
    // engine receives `enc_store`, the cursor store stays raw). Handing the
    // engine the bare LastStore made `stored_replay_value` fall through to its
    // no-key branch and write logical bytes verbatim, so every replayed
    // `main`/`metadata` row landed unsealed on disk while S0 chunks stayed
    // sealed. Catalog namespaces stay plaintext by policy through the same
    // allowlist the factory uses.
    let at_rest_crypto: std::sync::Arc<dyn fold_db::crypto::CryptoProvider> = std::sync::Arc::new(
        fold_db::crypto::LocalCryptoProvider::from_key(e2e.encryption_key()),
    );
    let namespaced: std::sync::Arc<dyn fold_db::storage::NamespacedStore> = std::sync::Arc::new(
        fold_db::storage::EncryptingNamespacedStore::with_plaintext_namespaces(
            std::sync::Arc::clone(store) as _,
            at_rest_crypto,
            fold_db::storage::LASTSTORE_PLAINTEXT_NAMESPACES
                .iter()
                .copied()
                .map(str::to_string)
                .collect(),
        ),
    );
    let signer = std::sync::Arc::new(
        fold_db::security::Ed25519KeyPair::from_secret_key(&identity.seed)
            .map_err(|e| runtime_failure(format!("restore signer from identity seed: {e}")))?,
    );
    let mut engine = fold_db::sync::SyncEngine::new_with_laststore_backup_source(
        "lastdb-restore".to_string(),
        sync_crypto,
        clients.s3.clone(),
        clients.auth.clone(),
        namespaced,
        fold_db::sync::SyncConfig {
            capture_mode: fold_db::sync::engine::CaptureMode::MutationLog,
            legacy_personal_cloud_sync: false,
            ..fold_db::sync::SyncConfig::default()
        },
        std::sync::Arc::clone(&signer),
        Some(std::sync::Arc::clone(store)),
    );
    // The photograph and download-cursor planes carry on-disk `ENC:` envelopes
    // already, so they stay on the raw store and are not sealed twice. Without
    // this the engine store above would become the cursor store by default.
    engine.set_cursor_store(std::sync::Arc::clone(store) as _);
    // Cursor bookkeeping is sealed under the portable content key before it
    // reaches the local seam, and legacy `ENC:` replay values are unwrapped
    // with it. Same key the daemon passes.
    engine.set_at_rest_key(e2e.encryption_key());
    Ok(Engine {
        engine: std::sync::Arc::new(engine),
        signer,
    })
}
