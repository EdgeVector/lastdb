//! Minimal-host boot: identity keyfile, E2E keys, `FoldDB` construction.
//!
//! `lastdbd` is a fresh-install daemon: its master-key root is a plain
//! `identity.key` seed file under the node home (the keyfile-root decision —
//! no OS keychain, no encrypted-at-rest keyring migration path). The same
//! Ed25519 seed is both the node identity (mutation signer, public key) and
//! the E2E encryption root ([`E2eKeys::from_ed25519_seed`]), which is the
//! documented single-key-root path in fold_db core.
//!
//! **Portable same-key (Tom 2026-07-18):** Mini personal-data at-rest always
//! uses that E2E content key (`design-portable-same-key-at-rest-cloud`).
//!
//! Storage boot is local-only. Explicit byte backups read protected credentials
//! separately; an old cloud configuration never attaches a background engine.

use std::io::{self, IsTerminal};
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};

use fold_db::crypto::E2eKeys;
use fold_db::fold_db_core::FoldDB;
use fold_db::security::Ed25519KeyPair;
use fold_db::storage::config::DatabaseConfig;

/// File name of the identity seed under the node home (32 raw bytes, `0o600`).
/// Canonical definition lives in `lastdb_identity`, the identity crate shared
/// with `fold_db_node` so both binaries resolve the SAME identity from the
/// SAME node home.
pub use crate::identity::IDENTITY_KEY_FILE;

/// The booted minimal host: the core database plus the node identity.
pub struct Host {
    /// Node home directory.
    pub home: PathBuf,
    /// Data directory under [`Self::home`].
    pub data_dir: PathBuf,
    /// Unix epoch seconds when this process started.
    pub process_start_ts: u64,
    /// The live core database (query executor, mutation manager, schema
    /// manager and native index).
    pub db: Arc<FoldDB>,
    /// The node's identity keypair (also the E2E key root).
    pub keypair: Arc<Ed25519KeyPair>,
    /// Derived owner user hash (sha256(pubkey)[..16] hex — the same
    /// derivation every fold surface uses).
    pub user_hash: String,
    /// UDS worker pool (set after boot once accept loops start). Used by
    /// `/api/status` for ops visibility; empty until `attach_uds_workers`.
    pub uds_workers: OnceLock<crate::lastdb_uds::UdsWorkerPool>,
    /// Explicit byte backup admission, worker completion and shutdown pause.
    pub byte_backup: Arc<crate::byte_cloud_backup::BackupRuntime>,
    /// Serializes shared upload, replay and snapshot work for this running home.
    pub shared_sync: crate::backup_runtime::SharedSyncRuntime,
}

/// Derive the canonical user hash from a base64 Ed25519 public key. Canonical
/// implementation lives in `lastdb_identity` (shared with `fold_db_node`) so
/// every binary derives the same owner identity.
pub use crate::identity::{load_or_generate_seed_with_meta, user_hash_from_pubkey};

impl Host {
    fn prepare_boot_home(home: &Path) -> Result<PathBuf, String> {
        ensure_node_home(home)
            .map_err(|e| format!("failed to create node home {}: {e}", home.display()))?;
        crate::shutdown_receipt::clear_shutdown_flush_receipt(home)
            .map_err(|error| format!("could not clear prior shutdown proof: {error}"))?;
        let data_path = home.join("data");
        std::fs::create_dir_all(&data_path)
            .map_err(|e| format!("failed to create data dir {}: {e}", data_path.display()))?;
        Ok(data_path)
    }

    fn load_boot_identity(home: &Path) -> Result<(Arc<Ed25519KeyPair>, E2eKeys, bool), String> {
        let (seed, freshly_generated) = load_or_generate_seed_with_meta(home)?;
        if freshly_generated {
            match bip39::Mnemonic::from_entropy(&seed) {
                Ok(mnemonic) => {
                    emit_fresh_recovery_phrase(&mnemonic.to_string());
                }
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        "fresh identity written but BIP39 phrase could not be encoded"
                    );
                }
            }
        }
        let keypair = Arc::new(
            Ed25519KeyPair::from_secret_key(&seed)
                .map_err(|e| format!("identity keyfile is not a valid Ed25519 seed: {e}"))?,
        );
        let e2e_keys = E2eKeys::from_ed25519_seed(&seed)
            .map_err(|e| format!("E2E key derivation failed: {e}"))?;
        Ok((keypair, e2e_keys, freshly_generated))
    }

    async fn open_boot_db(
        home: &Path,
        data_path: &Path,
        existing_store: bool,
        keypair: &Arc<Ed25519KeyPair>,
        e2e_keys: &E2eKeys,
        search_outbox_inbox: PathBuf,
    ) -> Result<Arc<FoldDB>, String> {
        let config = DatabaseConfig::local(data_path.to_path_buf());
        fold_db::fold_db_core::factory::create_fold_db_with_options(
            &config,
            e2e_keys,
            Arc::clone(keypair),
            fold_db::fold_db_core::factory::FoldDbOptions {
                offline_maintenance: false,
                search_outbox_inbox: Some(search_outbox_inbox),
            },
        )
        .await
        .map_err(|e| map_and_record_boot_error(home, existing_store, e))
    }
    /// Boot the minimal host under `home` (the node-home directory; data
    /// lives at `<home>/data`).
    ///
    /// **Decrypt-failure contract** (incident-lastdbd-0226-wrong-key-fresh-db):
    /// if the data dir already holds an initialized store and decryption fails,
    /// this returns `Err` with a refuse-to-boot message and leaves the store
    /// untouched. It never falls back to serving a fresh empty DB over existing
    /// data, and never uses cloud bootstrap as a heal for a local decrypt
    /// failure.
    pub async fn boot(home: &Path) -> Result<Self, String> {
        let data_path = Self::prepare_boot_home(home)?;
        let search_outbox_inbox =
            fold_db::db_operations::search_index::search_inbox_dir_for_home(home);

        let existing_store = data_dir_has_existing_store(&data_path);

        let (keypair, e2e_keys, _) = Self::load_boot_identity(home)?;

        let db = Self::open_boot_db(
            home,
            &data_path,
            existing_store,
            &keypair,
            &e2e_keys,
            search_outbox_inbox.clone(),
        )
        .await?;
        fold_db::db_operations::search_index::queue_policy::prepare(&search_outbox_inbox)
            .map_err(|error| format!("Search queue preparation failed: {error}"))?;
        let user_hash = user_hash_from_pubkey(&keypair.public_key_base64())?;
        clear_last_boot_error(home);
        // Mini always boots with content-key at-rest (see module docs).
        let at_rest_provider = "e2e_content_key";
        tracing::info!(
            target: "lastdbd::boot",
            user_hash = %user_hash,
            at_rest_provider = %at_rest_provider,
            "minimal host booted"
        );

        Ok(Self {
            home: home.to_path_buf(),
            data_dir: data_path,
            process_start_ts: fold_db::clock::unix_secs(),
            db,
            keypair,
            user_hash,
            uds_workers: OnceLock::new(),
            byte_backup: Arc::new(crate::byte_cloud_backup::BackupRuntime::new()),
            shared_sync: crate::backup_runtime::SharedSyncRuntime::default(),
        })
    }

    /// The node's public key (base64).
    pub fn public_key(&self) -> String {
        self.keypair.public_key_base64()
    }

    /// Attach the UDS worker pool for status surfaces (idempotent; first wins).
    pub fn attach_uds_workers(&self, pool: crate::lastdb_uds::UdsWorkerPool) {
        let _ = self.uds_workers.set(pool);
    }
}

/// Resolve the node home for `lastdbd`: an explicit `--data-dir` wins, then
/// `LASTDB_HOME`, then a persisted `lastdb admin service-home`, then `~/.lastdb`.
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

mod recovery;
pub use recovery::*;
