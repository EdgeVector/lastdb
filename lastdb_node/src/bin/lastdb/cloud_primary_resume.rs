//! Read-only preflight for a stopped copy before primary cloud resume.

use fold_db::crypto::{CryptoProvider, E2eKeys, LocalCryptoProvider};
use fold_db::storage::traits::NamespacedStore;
use fold_db::storage::{EncryptingNamespacedStore, LastStoreNamespacedStore};
use std::path::Path;
use std::sync::Arc;

/// Canonicalize both homes and require a stopped, paused-cloud copy.
fn checked_copy_home(copy_home: &Path, primary_home: &Path) -> Result<std::path::PathBuf, String> {
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
    Ok(copy_home)
}

/// Read the copy's writer (device) identity file.
fn read_copy_writer(copy_home: &Path) -> Result<String, String> {
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
    Ok(writer)
}

/// Open the copy's encrypted store with the key derived from its identity seed.
fn open_copy_store(copy_home: &Path) -> Result<EncryptingNamespacedStore, String> {
    let seed =
        lastdb_identity::load_seed(copy_home)?.ok_or("resume plan copy has no identity key")?;
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
    Ok(EncryptingNamespacedStore::new(base, crypto))
}

/// Cloud auth client from the copy's paused cloud configuration.
fn copy_cloud_auth(config: &Path) -> Result<fold_db::sync::auth::AuthClient, String> {
    let (url, api_key) = super::load_cloud_creds_from_path(config, None, None)?;
    let http = Arc::new(
        fold_db::sync::build_shared_http_client()
            .map_err(|_| "cloud HTTPS transport unavailable")?,
    );
    Ok(fold_db::sync::auth::AuthClient::new(
        http,
        url,
        fold_db::sync::auth::SyncAuth::ApiKey(api_key),
    )
    .without_db_auto_claim())
}

pub(super) async fn plan(copy_home: &Path, primary_home: &Path, json: bool) -> Result<(), String> {
    let copy_home = checked_copy_home(copy_home, primary_home)?;
    let config = copy_home.join(lastdb_node::cloud::CLOUD_SYNC_PAUSED_FILE);
    let db_hash = fold_db::storage::laststore::read_cloud_db_hash(&copy_home.join("data"))
        .ok_or("resume plan copy has no cloud database identity")?;
    let writer = read_copy_writer(&copy_home)?;
    let store = open_copy_store(&copy_home)?;
    let auth = copy_cloud_auth(&config)?;
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
