//! The worker-inert production constructor, raw backup source and cut barrier.

use super::{admission, err, NormalSnapshotArgs};
use crate::home::HomeStore;
use fold_db::crypto::{CryptoProvider, LocalCryptoProvider};
use fold_db::storage::laststore::{high_water_path_for_store_root, BackupManifest};
use fold_db::storage::traits::NamespacedStore;
use fold_db::storage::{
    EncryptingNamespacedStore, LastStoreNamespacedStore, LASTSTORE_PLAINTEXT_NAMESPACES,
};
use fold_db::sync::auth::ops::BackupLatestPointer;
use fold_db::sync::auth::{AuthClient, SyncAuth};
use fold_db::sync::s3::S3Client;
use fold_db::sync::{SyncConfig, SyncEngine};
use std::sync::Arc;

pub(super) fn clients(inputs: &admission::Inputs) -> (AuthClient, S3Client) {
    // trace-egress: propagate through the production shared transport.
    let http = Arc::new(fold_db::sync::build_shared_http_client());
    let s3 = S3Client::new(Arc::clone(&http));
    let auth = AuthClient::new(
        http,
        inputs.cloud.api_url.clone(),
        SyncAuth::ApiKey(inputs.cloud.api_key.clone()),
    )
    .with_db_hash(Some(inputs.db_hash.clone()))
    .without_db_auto_claim();
    (auth, s3)
}

pub(super) async fn latest(
    auth: &AuthClient,
    expected: &BackupManifest,
) -> Result<BackupLatestPointer, String> {
    let latest = auth
        .backup_latest_get_optional()
        .await
        .map_err(err)?
        .ok_or("the established normal backup/latest is absent")?
        .latest;
    latest.require_v1_format().map_err(err)?;
    if latest.store_uuid != expected.store_uuid
        || latest.epoch != expected.epoch
        || latest.counter != expected.counter
        || latest.manifest_sha256
            != fold_db::storage::laststore::manifest_sha256_hex(expected).map_err(err)?
    {
        return Err("normal cloud latest differs from the exact local predecessor".into());
    }
    Ok(latest)
}

pub(super) fn open(
    inputs: &admission::Inputs,
) -> Result<(Arc<LastStoreNamespacedStore>, HomeStore), String> {
    let raw = Arc::new(
        LastStoreNamespacedStore::open_with_options_and_high_water(
            &inputs.store_root,
            LastStoreNamespacedStore::product_hash_group_options(),
            high_water_path_for_store_root(&inputs.store_root),
        )
        .map_err(err)?,
    );
    if raw.cloud_db_hash().map_err(err)?.as_ref() != Some(&inputs.db_hash) {
        return Err("writable store opened a different cloud database identity".into());
    }
    let (e2e, _) = lastdb_node::offline_home::load_e2e_keys(&inputs.home)?;
    let base: Arc<dyn NamespacedStore> = Arc::clone(&raw) as _;
    let crypto: Arc<dyn CryptoProvider> =
        Arc::new(LocalCryptoProvider::from_key(e2e.encryption_key()));
    let store = Arc::new(EncryptingNamespacedStore::with_plaintext_namespaces(
        Arc::clone(&base),
        crypto,
        LASTSTORE_PLAINTEXT_NAMESPACES
            .iter()
            .map(|name| (*name).into())
            .collect(),
    ));
    Ok((
        raw,
        HomeStore {
            base,
            store,
            store_root: inputs.store_root.clone(),
            seam: "at-rest-seam",
        },
    ))
}

pub(super) async fn construct(
    args: &NormalSnapshotArgs,
    inputs: &admission::Inputs,
    raw: Arc<LastStoreNamespacedStore>,
    opened: &HomeStore,
    auth: AuthClient,
    s3: S3Client,
) -> Result<SyncEngine, String> {
    let (e2e, signer) = lastdb_node::offline_home::load_e2e_keys(&inputs.home)?;
    let crypto: Arc<dyn CryptoProvider> =
        Arc::new(LocalCryptoProvider::from_key(e2e.encryption_key()));
    let mut engine = SyncEngine::new_with_laststore_backup_source(
        inputs.device_id.clone(),
        crypto,
        s3,
        auth,
        Arc::clone(&opened.store),
        SyncConfig {
            capture_mode: fold_db::sync::engine::CaptureMode::MutationLog,
            legacy_personal_cloud_sync: false,
            ..SyncConfig::default()
        },
        signer,
        Some(Arc::clone(&raw)),
    );
    engine.set_cursor_store(Arc::clone(&opened.base));
    engine.set_at_rest_key(e2e.encryption_key());
    engine.set_backup_manifest_cache_path(lastdb_node::host::backup_manifest_cache_path(
        &inputs.home,
    ));
    let stopped_args = args.clone();
    engine
        .set_photograph_cut_barrier(Arc::new(move || {
            let args = stopped_args.clone();
            let raw = Arc::clone(&raw);
            Box::pin(async move {
                admission::stopped(&args)?;
                raw.restore_durability_barrier().await.map_err(err)?;
                admission::stopped(&args)
            })
        }))
        .await;
    // Do not start the backup uploader, sync coordinator, capture router,
    // materializer, janitors, schema services, or any FoldDB lifecycle here.
    Ok(engine)
}
