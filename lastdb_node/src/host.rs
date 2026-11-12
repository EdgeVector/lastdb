//! Minimal-host boot: identity keyfile, E2E keys, and `FoldDB` construction.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};

use fold_db::crypto::E2eKeys;
use fold_db::fold_db_core::FoldDB;
use fold_db::security::Ed25519KeyPair;
use fold_db::storage::config::DatabaseConfig;

pub use crate::identity::{
    load_or_generate_seed_with_meta, user_hash_from_pubkey, IDENTITY_KEY_FILE,
};

mod recovery;
pub use recovery::LAST_BOOT_ERROR_FILE;
pub(crate) mod owned;

/// The booted minimal host: core database plus node identity and runtime handles.
pub struct Host {
    pub home: PathBuf,
    pub data_dir: PathBuf,
    pub process_start_ts: u64,
    pub db: Arc<FoldDB>,
    pub keypair: Arc<Ed25519KeyPair>,
    pub user_hash: String,
    pub uds_workers: OnceLock<crate::lastdb_uds::UdsWorkerPool>,
    pub byte_backup: Arc<crate::byte_cloud_backup::BackupRuntime>,
    pub(crate) owned: owned::Tables,
    /// Open signed-log receiver stores, keyed by stable database id.
    pub(crate) received: tokio::sync::Mutex<HashMap<String, Arc<FoldDB>>>,
}

impl Host {
    /// Boot the minimal host under `home` (`<home>/data`). Refuses to start fresh over
    /// an existing store if opening or decrypting fails.
    pub async fn boot(home: &Path) -> Result<Self, String> {
        recovery::ensure_node_home(home)
            .map_err(|e| format!("failed to create node home {}: {e}", home.display()))?;
        crate::shutdown_receipt::clear_shutdown_flush_receipt(home)
            .map_err(|e| format!("could not clear prior shutdown proof: {e}"))?;
        let data_dir = home.join("data");
        std::fs::create_dir_all(&data_dir)
            .map_err(|e| format!("failed to create data dir {}: {e}", data_dir.display()))?;

        let existing = data_dir
            .join(fold_db::storage::disk_pack::NODE_MARKER)
            .is_file();
        let (seed, fresh) = load_or_generate_seed_with_meta(home)?;
        if fresh {
            match bip39::Mnemonic::from_entropy(&seed) {
                Ok(m) => recovery::emit_fresh_recovery_phrase(&m.to_string()),
                Err(e) => tracing::warn!(
                    error = %e, "fresh identity written but BIP39 phrase could not be encoded"
                ),
            }
        }
        let keypair = Arc::new(
            Ed25519KeyPair::from_secret_key(&seed)
                .map_err(|e| format!("identity keyfile is not a valid Ed25519 seed: {e}"))?,
        );
        let e2e_keys = E2eKeys::from_ed25519_seed(&seed)
            .map_err(|e| format!("E2E key derivation failed: {e}"))?;

        let inbox = fold_db::db_operations::search_index::search_inbox_dir_for_home(home);
        let db = fold_db::fold_db_core::factory::create_fold_db_with_options(
            &DatabaseConfig::local(data_dir.clone()),
            &e2e_keys,
            Arc::clone(&keypair),
            fold_db::fold_db_core::factory::FoldDbOptions {
                offline_maintenance: false,
                search_outbox_inbox: Some(inbox.clone()),
                capture_accepted: false,
            },
        )
        .await
        .map_err(|e| recovery::map_and_record_boot_error(home, existing, e))?;

        fold_db::db_operations::search_index::queue_policy::prepare(&inbox)
            .map_err(|e| format!("Search queue preparation failed: {e}"))?;
        let owned = owned::Tables::open(home, Arc::clone(&keypair)).await?;
        let user_hash = user_hash_from_pubkey(&keypair.public_key_base64())?;
        recovery::clear_last_boot_error(home);
        tracing::info!(
            target: "lastdbd::boot", user_hash = %user_hash,
            at_rest_provider = "e2e_content_key", "minimal host booted"
        );
        Ok(Self {
            home: home.to_path_buf(),
            data_dir,
            process_start_ts: fold_db::clock::unix_secs(),
            db,
            keypair,
            user_hash,
            uds_workers: OnceLock::new(),
            byte_backup: Arc::new(crate::byte_cloud_backup::BackupRuntime::new()),
            owned,
            received: tokio::sync::Mutex::new(HashMap::new()),
        })
    }

    pub fn public_key(&self) -> String {
        self.keypair.public_key_base64()
    }

    pub fn attach_uds_workers(&self, pool: crate::lastdb_uds::UdsWorkerPool) {
        let _ = self.uds_workers.set(pool);
    }
}

/// Resolve node home: `--data-dir` > `LASTDB_HOME` > `service-home` > `~/.lastdb`.
pub fn resolve_home(data_dir: Option<PathBuf>) -> Result<PathBuf, String> {
    match data_dir {
        Some(dir) => crate::profile::paths::expand_tilde_path(dir),
        None if crate::service_home::explicit_home_override_present() => {
            crate::profile::paths::folddb_home()
        }
        None => crate::service_home::configured_home()?
            .map_or_else(crate::profile::paths::folddb_home, Ok),
    }
}
