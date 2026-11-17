//! Minimal-host boot, shutdown flush proof, identity keyfile, E2E keys, and `FoldDB` construction.

use std::collections::HashMap;
use std::io::Write;
#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt as _;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::time::{Duration, Instant};

use cloud_backup_protocol::Id;
use fold_db::crypto::E2eKeys;
use fold_db::fold_db_core::FoldDB;
use fold_db::security::Ed25519KeyPair;
use fold_db::storage::config::DatabaseConfig;
use rand::{rngs::OsRng, RngCore};
use sha2::{Digest, Sha256};
use zeroize::Zeroizing;

pub use crate::profile::{user_hash_from_pubkey, IDENTITY_KEY_FILE};

pub(crate) mod cloud_sync;
mod logging;
pub(crate) mod owned;
mod recovery;
pub(crate) use cloud_sync::{BlockedDelivery, ReplayBlockReason};
pub use logging::init_daemon_logging;

const SHUTDOWN_FLUSH_READY_FILE: &str = ".shutdown_flush_ready";
pub(crate) const SESSION_LIFETIME: Duration = Duration::from_secs(3600);
const MAX_SESSIONS: usize = 1024;

#[derive(Default)]
pub(crate) struct DatabaseSessions(Mutex<HashMap<[u8; 32], SessionGrant>>);

struct SessionGrant {
    id: Id,
    write: bool,
    expires: Instant,
}

impl DatabaseSessions {
    pub(crate) fn issue(&self, id: Id, write: bool) -> Option<String> {
        let mut sessions = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        let now = Instant::now();
        sessions.retain(|_, grant| grant.expires > now);
        if sessions.len() >= MAX_SESSIONS {
            return None;
        }
        let mut random = [0u8; 32];
        OsRng.fill_bytes(&mut random);
        let token = fold_db::hex::hex_lower(random);
        let key: [u8; 32] = Sha256::digest(token.as_bytes()).into();
        sessions.insert(
            key,
            SessionGrant {
                id,
                write,
                expires: now + SESSION_LIFETIME,
            },
        );
        Some(token)
    }

    pub(crate) fn get(&self, token: &str) -> Option<(Id, bool)> {
        if token.len() != 64 || !token.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return None;
        }
        let sessions = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        let key: [u8; 32] = Sha256::digest(token.as_bytes()).into();
        let grant = sessions.get(&key)?;
        (grant.expires > Instant::now()).then(|| (grant.id.clone(), grant.write))
    }
}

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
    pub(crate) booted_with_credentials: bool,
    pub(crate) cloud_sync: cloud_sync::CloudSyncTracker,
    pub(crate) owned: owned::Tables,
    pub(crate) received: tokio::sync::Mutex<HashMap<String, Arc<FoldDB>>>,
    pub(crate) database_sessions: DatabaseSessions,
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

    /// Boot the isolated restore home after verified snapshot files are installed.
    #[doc(hidden)]
    pub async fn boot_for_snapshot_restore(home: &Path) -> Result<Self, String> {
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
            match bip39::Mnemonic::from_entropy(seed.as_ref()) {
                Ok(m) => recovery::emit_fresh_recovery_phrase(&m.to_string()),
                Err(e) => tracing::warn!(error = %e, "BIP39 phrase could not be encoded"),
            }
        }
        let keypair = Arc::new(
            Ed25519KeyPair::from_secret_key(seed.as_ref())
                .map_err(|e| format!("identity keyfile is not a valid Ed25519 seed: {e}"))?,
        );
        let e2e_keys = E2eKeys::from_ed25519_seed(&seed)
            .map_err(|e| format!("E2E key derivation failed: {e}"))?;
        let inbox = fold_db::db_operations::search_index::search_inbox_dir_for_home(home);
        let has_spool = !signed_restore
            && fold_db::storage::disk_backup::accepted::has_spool(&data_dir)
                .map_err(|e| recovery::map_and_record_boot_error(home, existing, e))?;
        let wants_initial = !signed_restore && crate::cloud::first_backup_intent(home);
        let creds_at_boot = has_cloud_credentials(home);
        let connected = !signed_restore && (!has_spool || wants_initial) && creds_at_boot;
        let opts = fold_db::fold_db_core::factory::FoldDbOptions {
            offline_maintenance: false,
            search_outbox_inbox: Some(inbox.clone()),
            capture_accepted: signed_restore || has_spool || connected,
            capture_initial_snapshot: wants_initial && connected,
            track_shared_record_revisions: false,
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
        let cloud_sync = cloud_sync::CloudSyncTracker::from_boot(
            home,
            db.db_ops().native_runtime().captures_accepted(),
        );
        let _ = std::fs::remove_file(home.join(recovery::LAST_BOOT_ERROR_FILE));
        tracing::info!(target: "lastdbd::boot", "minimal host booted");
        Ok(Self {
            home: home.to_path_buf(),
            process_start_ts: fold_db::clock::unix_secs(),
            db,
            keypair,
            user_hash,
            uds_workers: OnceLock::new(),
            booted_with_credentials: creds_at_boot,
            cloud_sync,
            owned,
            received: tokio::sync::Mutex::new(HashMap::new()),
            database_sessions: DatabaseSessions::default(),
        })
    }

    pub fn public_key(&self) -> String {
        self.keypair.public_key_base64()
    }

    /// Flush signed-log receiver stores before the owned sibling stores.
    pub async fn shutdown_received_databases(&self) -> Result<(), String> {
        let stores: Vec<Arc<FoldDB>> = {
            let mut open = self.received.lock().await;
            open.drain().map(|(_, store)| store).collect()
        };
        let mut failed = false;
        for store in stores {
            if store.shutdown().await.is_err() {
                failed = true;
            }
        }
        if failed {
            Err("received database shutdown flush failed".into())
        } else {
            Ok(())
        }
    }
}

pub(crate) fn has_cloud_credentials(home: &Path) -> bool {
    match crate::cloud::load_saved_credentials(home) {
        Ok(saved) => saved.is_some(),
        Err(error) => {
            tracing::warn!(%error, "personal database stays local until cloud credentials load");
            false
        }
    }
}

fn load_or_generate_seed_with_meta(home: &Path) -> Result<(Zeroizing<[u8; 32]>, bool), String> {
    if let Some(seed) = crate::offline_home::read_home_identity_seed(home)? {
        return Ok((seed, false));
    }
    let pair =
        Ed25519KeyPair::generate().map_err(|e| format!("identity generation failed: {e}"))?;
    let seed = Zeroizing::new(pair.secret_key_bytes());
    std::fs::create_dir_all(home)
        .map_err(|e| format!("failed to create node home {}: {e}", home.display()))?;
    let key_path = home.join(IDENTITY_KEY_FILE);
    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create_new(true);
    #[cfg(unix)]
    opts.mode(0o600);
    opts.open(&key_path)
        .and_then(|mut f| f.write_all(seed.as_ref()).and_then(|()| f.sync_all()))
        .map_err(|e| format!("failed to write {}: {e}", key_path.display()))?;
    Ok((seed, true))
}

fn configured_home() -> Result<Option<PathBuf>, String> {
    use crate::profile::paths;
    let path = if let Some(p) = std::env::var_os("LASTDBD_SERVICE_HOME_CONFIG") {
        PathBuf::from(p)
    } else {
        let sub = if cfg!(target_os = "macos") {
            "Library/Application Support/LastDB/lastdbd-service-home"
        } else {
            ".config/lastdb/lastdbd-service-home"
        };
        let h = std::env::var_os("HOME")
            .ok_or("Cannot locate lastdb admin service-home config: HOME is not set")?;
        PathBuf::from(h).join(sub)
    };
    let raw = match std::fs::read_to_string(&path) {
        Ok(r) if r.trim().is_empty() => return Ok(None),
        Ok(r) => r,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => {
            return Err(format!(
                "Failed to read lastdb admin service-home config {}: {e}",
                path.display()
            ));
        }
    };
    let home = paths::expand_tilde_path(raw.trim())?;
    if !home.is_absolute() {
        return Err(format!(
            "lastdbd service home must be absolute after ~ expansion (got {})",
            home.display()
        ));
    }
    let primary = std::env::var_os("HOME").map(|h| PathBuf::from(h).join(paths::LASTDB_DIR));
    if let Some(p) = primary.filter(|p| p.is_dir()) {
        let canon = std::fs::canonicalize(&p).unwrap_or(p);
        if std::fs::canonicalize(&home).unwrap_or_else(|_| home.clone()) == canon {
            return Err(format!(
                "Refusing to configure lastdbd service home as the primary live database home {}. Choose a separate directory for a second node.",
                canon.display()
            ));
        }
    }
    Ok(Some(home))
}

pub fn resolve_home(data_dir: Option<PathBuf>) -> Result<PathBuf, String> {
    match data_dir {
        Some(dir) => crate::profile::paths::expand_tilde_path(dir),
        None if std::env::var_os(crate::profile::paths::LASTDB_HOME_ENV).is_some() => {
            crate::profile::paths::folddb_home()
        }
        None => configured_home()?.map_or_else(crate::profile::paths::folddb_home, Ok),
    }
}
