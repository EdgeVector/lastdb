//! Read-only preflight for a stopped copy before primary cloud resume.

use fold_db::crypto::{CryptoProvider, E2eKeys, LocalCryptoProvider};
use fold_db::storage::traits::NamespacedStore;
use fold_db::storage::{EncryptingNamespacedStore, LastStoreNamespacedStore};
use std::path::Path;
use std::sync::Arc;

pub(super) async fn plan(copy_home: &Path, primary_home: &Path, json: bool) -> Result<(), String> {
    let copy_home = copy_home
        .canonicalize()
        .map_err(|error| format!("resolve resume plan copy: {error}"))?;
    let primary_home = primary_home
        .canonicalize()
        .map_err(|error| format!("resolve primary home: {error}"))?;
    if copy_home == primary_home {
        return Err("resume plan requires a stopped copy, not the primary home".into());
    }
    let socket = lastdb_uds::uds::socket_path(&copy_home.join("data"));
    if socket.exists() || copy_home.join("data/folddb-full.sock").exists() {
        return Err("resume plan copy has a daemon socket; stop its copy node first".into());
    }
    let config = copy_home.join(lastdb_node::cloud::CLOUD_SYNC_PAUSED_FILE);
    if !config.is_file()
        || copy_home
            .join(lastdb_node::host::CLOUD_SYNC_CONFIG_FILE)
            .exists()
    {
        return Err("resume plan requires one paused cloud configuration in the copy".into());
    }
    let db_hash = fold_db::storage::laststore::read_cloud_db_hash(&copy_home.join("data"))
        .ok_or("resume plan copy has no cloud database identity")?;
    let writer_bytes = std::fs::read(copy_home.join("data/.device_id"))
        .map_err(|error| format!("read copy writer identity: {error}"))?;
    if writer_bytes.is_empty() || writer_bytes.len() > 256 {
        return Err("resume plan copy writer identity is invalid".into());
    }
    let writer = String::from_utf8(writer_bytes)
        .map_err(|_| "resume plan copy writer identity is not UTF-8")?
        .trim()
        .to_string();
    if writer.is_empty() {
        return Err("resume plan copy writer identity is empty".into());
    }
    let seed =
        lastdb_identity::load_seed(&copy_home)?.ok_or("resume plan copy has no identity key")?;
    let keys = E2eKeys::from_ed25519_seed(&seed)
        .map_err(|error| format!("derive copy encryption key: {error}"))?;
    let base: Arc<dyn NamespacedStore> = Arc::new(
        LastStoreNamespacedStore::open_with_data_key_and_high_water(
            &copy_home.join("data"),
            keys.encryption_key(),
            copy_home.join("laststore_high_water.json"),
        )
        .map_err(|error| format!("open resume plan copy: {error}"))?,
    );
    let crypto: Arc<dyn CryptoProvider> =
        Arc::new(LocalCryptoProvider::from_key(keys.encryption_key()));
    let store = EncryptingNamespacedStore::with_plaintext_namespaces(
        base,
        crypto,
        fold_db::storage::LASTSTORE_PLAINTEXT_NAMESPACES
            .iter()
            .map(|name| (*name).to_string())
            .collect(),
    );
    let (url, api_key) = super::load_cloud_creds_from_path(&config, None, None)?;
    let http = Arc::new(fold_db::sync::build_shared_http_client());
    let auth = fold_db::sync::auth::AuthClient::new(
        http,
        url,
        fold_db::sync::auth::SyncAuth::ApiKey(api_key),
    )
    .without_db_auto_claim();
    let inventory =
        fold_db::sync::engine::inspect_primary_resume_plan(&store, &auth, &db_hash, &writer)
            .await
            .map_err(|error| error.to_string())?;
    let report = serde_json::json!({
        "ok": true,
        "cloud_write": false,
        "local_writer_frontier": inventory.local_writer_frontier,
        "cloud_writer_frontier": inventory.cloud_writer_frontier,
        "cloud_log_objects": inventory.cloud_log_objects,
    });
    if json {
        println!("{report}");
    } else {
        println!("Resume plan copy:");
        println!(
            "  local writer frontier: {}",
            inventory.local_writer_frontier
        );
        println!(
            "  cloud writer frontier: {}",
            inventory.cloud_writer_frontier
        );
        println!("  cloud log objects:     {}", inventory.cloud_log_objects);
    }
    Ok(())
}
