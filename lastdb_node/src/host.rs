//! Minimal-host boot, shutdown flush proof, identity keyfile, E2E keys, and `FoldDB` construction.

use std::collections::HashMap;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};

use fold_db::crypto::E2eKeys;
use fold_db::fold_db_core::FoldDB;
use fold_db::security::Ed25519KeyPair;
use fold_db::storage::config::DatabaseConfig;

pub use crate::identity::{
    load_or_generate_seed_with_meta, user_hash_from_pubkey, IDENTITY_KEY_FILE,
};

pub(crate) mod database_sessions;
pub(crate) mod owned;
mod recovery;
pub use recovery::init_daemon_logging;

const SHUTDOWN_FLUSH_READY_FILE: &str = ".shutdown_flush_ready";

pub fn write_shutdown_flush_receipt(home: &Path, pid: u32, start_ts: u64) -> std::io::Result<()> {
    if pid == 0 || start_ts == 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "invalid shutdown flush receipt",
        ));
    }
    let tmp = home.join(format!(
        ".shutdown_flush_ready.tmp.{pid}.{}",
        fold_db::clock::unix_nanos_wide()
    ));
    let bytes = serde_json::to_vec(&serde_json::json!({
        "version": 1u32, "pid": pid, "start_ts": start_ts, "flush_ok": true,
    }))
    .map_err(std::io::Error::other)?;
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&tmp)?;
    f.write_all(&bytes).and_then(|()| f.sync_all())?;
    std::fs::rename(&tmp, home.join(SHUTDOWN_FLUSH_READY_FILE))?;
    std::fs::File::open(home)?.sync_all()
}

pub struct Host {
    pub home: PathBuf,
    pub process_start_ts: u64,
    pub db: Arc<FoldDB>,
    pub keypair: Arc<Ed25519KeyPair>,
    pub user_hash: String,
    pub uds_workers: OnceLock<crate::lastdb_uds::UdsWorkerPool>,
    pub(crate) owned: owned::Tables,
    pub(crate) received: tokio::sync::Mutex<HashMap<String, Arc<FoldDB>>>,
    pub(crate) database_sessions: database_sessions::DatabaseSessions,
}

impl Host {
    pub async fn boot(home: &Path) -> Result<Self, String> {
        Self::boot_inner(home, false).await
    }

    /// Boot a fresh, isolated signed-restore home. Record replay uses the
    /// accepted spool's replay scope, and later local writes can be captured.
    #[doc(hidden)]
    pub async fn boot_for_signed_restore(home: &Path) -> Result<Self, String> {
        if home.join("data").exists() {
            return Err("signed restore capture requires a fresh staged home".to_owned());
        }
        Self::boot_inner(home, true).await
    }

    async fn boot_inner(home: &Path, signed_restore: bool) -> Result<Self, String> {
        recovery::ensure_node_home(home)
            .map_err(|e| format!("failed to create node home {}: {e}", home.display()))?;
        match std::fs::remove_file(home.join(SHUTDOWN_FLUSH_READY_FILE)) {
            Ok(()) => std::fs::File::open(home)
                .and_then(|d| d.sync_all())
                .map_err(|e| format!("could not clear prior shutdown proof: {e}"))?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(format!("could not clear prior shutdown proof: {e}")),
        }
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
                Err(e) => tracing::warn!(error = %e, "BIP39 phrase could not be encoded"),
            }
        }
        let keypair = Arc::new(
            Ed25519KeyPair::from_secret_key(&seed)
                .map_err(|e| format!("identity keyfile is not a valid Ed25519 seed: {e}"))?,
        );
        let e2e_keys = E2eKeys::from_ed25519_seed(&seed)
            .map_err(|e| format!("E2E key derivation failed: {e}"))?;
        let inbox = fold_db::db_operations::search_index::search_inbox_dir_for_home(home);
        let capture_accepted = signed_restore
            || fold_db::storage::disk_backup::accepted::has_spool(&data_dir)
                .map_err(|e| recovery::map_and_record_boot_error(home, existing, e))?
            || crate::cloud::load_saved_credentials(home).map_or_else(
                |e| {
                    tracing::warn!(error = %e, "personal database stays local until cloud credentials load");
                    false
                },
                |s| s.is_some(),
            );
        let opts = fold_db::fold_db_core::factory::FoldDbOptions {
            offline_maintenance: false,
            search_outbox_inbox: Some(inbox.clone()),
            capture_accepted,
        };
        let db = fold_db::fold_db_core::factory::create_fold_db_with_options(
            &DatabaseConfig::local(data_dir),
            &e2e_keys,
            Arc::clone(&keypair),
            opts,
        )
        .await
        .map_err(|e| recovery::map_and_record_boot_error(home, existing, e))?;
        fold_db::db_operations::search_index::queue_policy::prepare(&inbox)
            .map_err(|e| format!("Search queue preparation failed: {e}"))?;
        let owned = owned::Tables::open(home, Arc::clone(&keypair)).await?;
        let user_hash = user_hash_from_pubkey(&keypair.public_key_base64())?;
        let _ = std::fs::remove_file(home.join(recovery::LAST_BOOT_ERROR_FILE));
        tracing::info!(target: "lastdbd::boot", "minimal host booted");
        Ok(Self {
            home: home.to_path_buf(),
            process_start_ts: fold_db::clock::unix_secs(),
            db,
            keypair,
            user_hash,
            uds_workers: OnceLock::new(),
            owned,
            received: tokio::sync::Mutex::new(HashMap::new()),
            database_sessions: database_sessions::DatabaseSessions::default(),
        })
    }

    pub fn public_key(&self) -> String {
        self.keypair.public_key_base64()
    }
}

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
