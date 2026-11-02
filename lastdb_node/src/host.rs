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
use fold_db::storage::config::{DatabaseConfig, StorageEngine};

/// File name of the identity seed under the node home (32 raw bytes, `0o600`).
/// Canonical definition lives in `lastdb_identity`, the identity crate shared
/// with `fold_db_node` so both binaries resolve the SAME identity from the
/// SAME node home.
pub use lastdb_identity::IDENTITY_KEY_FILE;

/// The booted minimal host: the core database plus the node identity.
pub struct Host {
    /// Node home directory.
    pub home: PathBuf,
    /// Data directory under [`Self::home`].
    pub data_dir: PathBuf,
    /// Unix epoch seconds when this process started.
    pub process_start_ts: u64,
    /// In-memory sampler liveness/error state rendered by `/api/status`.
    pub self_metrics: Arc<crate::self_metrics::SamplerRuntimeState>,
    /// Per-request op telemetry (client self-id, latency, schema) for
    /// worst-offender ranking on `/api/status` and `lastdb ops`.
    pub request_telemetry: Arc<crate::request_telemetry::Runtime>,
    /// The live core database (query executor, mutation manager, schema
    /// manager and native index).
    pub db: Arc<FoldDB>,
    /// QoS admission gate: bounds concurrent bulk-lane operations (large blob
    /// reads/writes, e.g. a lastgit pack clone+push loop) so they cannot starve
    /// the reserved interactive lane (fbrain/fkanban) on this one canonical node.
    /// Configured from the environment at boot ([`lastdb_host::QosGate::from_env`]).
    pub qos: lastdb_host::QosGate,
    /// The node's identity keypair (also the E2E key root).
    pub keypair: Arc<Ed25519KeyPair>,
    /// Derived owner user hash (sha256(pubkey)[..16] hex — the same
    /// derivation every fold surface uses).
    pub user_hash: String,
    /// UDS worker pool (set after boot once accept loops start). Used by
    /// `/api/status` for ops visibility; empty until `attach_uds_workers`.
    pub uds_workers: OnceLock<lastdb_uds::UdsWorkerPool>,
    /// Local short-TTL mutation doorbell (not cloud-synced). Apps long-poll
    /// `/api/local-watch` instead of idle-polling keyed tables.
    pub local_outbox: Arc<crate::local_outbox::LocalOutbox>,
    /// Bounded ordered persistence lane for durable app change-feed hints.
    /// Requests reserve capacity before product commit and submit after it.
    pub(crate) change_feed_queue: crate::change_feed_queue::ChangeFeedQueue,
    /// Admission gate for *blocking* `/api/local-watch` waiters. A parked
    /// long-poll holds a UDS worker for its whole timeout and takes no QoS
    /// permit, so without this the pool can be fully occupied by sleeping
    /// threads while [`Self::qos`] still reports the node idle. Sized to half
    /// the worker pool by [`Host::attach_uds_workers`].
    pub watch_gate: Arc<crate::watch_gate::WatchGate>,
    /// Explicit byte backup admission, worker completion and shutdown pause.
    pub byte_backup: Arc<crate::byte_cloud_backup::BackupRuntime>,
    /// This process's own boot identity (pid/build/start_ts/restart_cause),
    /// set once right after the session ledger records this session's start.
    /// `/api/system/boot-identity` must read this, never the shared per-home
    /// ledger file directly — a different process sharing this home can
    /// append a newer row there
    /// (papercut-lastdb-primary-boot-identity-stale-phantom-pid-20260927).
    pub own_boot_identity: OnceLock<crate::session_ledger::SessionRecord>,
}

/// Derive the canonical user hash from a base64 Ed25519 public key. Canonical
/// implementation lives in `lastdb_identity` (shared with `fold_db_node`) so
/// every binary derives the same owner identity.
pub use lastdb_identity::{load_or_generate_seed_with_meta, user_hash_from_pubkey};

mod boot;

impl Host {
    /// True while a node task can still write after the sampler stops.
    pub fn background_writes_in_flight(&self) -> bool {
        self.self_metrics.background_writes_in_flight()
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
            cloud_sync = false,
            at_rest_provider = %at_rest_provider,
            "minimal host booted"
        );

        let change_feed_queue = crate::change_feed_queue::ChangeFeedQueue::new(
            db.db_ops().change_feed().clone(),
            Arc::clone(db.pending_tasks()),
        );

        Ok(Self {
            home: home.to_path_buf(),
            data_dir: data_path,
            process_start_ts: fold_db::clock::unix_secs(),
            self_metrics: Arc::new(crate::self_metrics::SamplerRuntimeState::default()),
            request_telemetry: Arc::new(crate::request_telemetry::Runtime::default()),
            db,
            qos: lastdb_host::QosGate::from_env(),
            keypair,
            user_hash,
            uds_workers: OnceLock::new(),
            local_outbox: crate::local_outbox::LocalOutbox::shared(
                crate::local_outbox::DEFAULT_TTL,
                crate::local_outbox::DEFAULT_MAX_EVENTS,
            ),
            change_feed_queue,
            watch_gate: crate::watch_gate::WatchGate::shared_from_env(),
            byte_backup: Arc::new(crate::byte_cloud_backup::BackupRuntime::new()),
            own_boot_identity: OnceLock::new(),
        })
    }

    /// The node's public key (base64).
    pub fn public_key(&self) -> String {
        self.keypair.public_key_base64()
    }

    /// Attach the UDS worker pool for status surfaces (idempotent; first wins).
    ///
    /// Also sizes [`Self::watch_gate`] from the pool: the whole point of the
    /// watcher cap is that it is derived from — and strictly below — the number
    /// of workers a parked long-poll can consume.
    pub fn attach_uds_workers(&self, pool: lastdb_uds::UdsWorkerPool) {
        self.watch_gate.configure_for_workers(pool.workers());
        let _ = self.uds_workers.set(pool);
    }
}

/// The minimal daemon drives the SAME shared owner-socket handlers
/// (`lastdb_host::handlers`) the full node does. Its [`HostNode`] surface is the
/// core DB + identity plus the QoS admission gate that keeps bulk blob traffic
/// (lastgit pack clone+push) from starving interactive fbrain/fkanban traffic:
/// [`acquire_op_permit`](HostNode::acquire_op_permit) admits through
/// [`Host::qos`].
#[async_trait::async_trait]
impl lastdb_host::HostNode for Host {
    type OpPermit = lastdb_host::QosPermit;

    fn fold_db(&self) -> &Arc<FoldDB> {
        &self.db
    }

    fn public_key(&self) -> String {
        self.keypair.public_key_base64()
    }

    async fn acquire_op_permit(
        &self,
        lane: lastdb_host::Lane,
    ) -> Result<Self::OpPermit, lastdb_host::ReadBusy> {
        self.qos.acquire(lane).await
    }

    async fn wait_for_background_tasks(&self, timeout: std::time::Duration) -> bool {
        self.db.wait_for_background_tasks(timeout).await
    }
}

/// Resolve the node home for `lastdbd`: an explicit `--data-dir` wins, then
/// `LASTDB_HOME`, then a persisted `lastdbd service-home`, then `~/.lastdb`.
pub fn resolve_home(data_dir: Option<PathBuf>) -> Result<PathBuf, String> {
    match data_dir {
        Some(dir) => folddb_profile::paths::expand_tilde_path(dir),
        None if crate::service_home::explicit_home_override_present() => {
            folddb_profile::paths::folddb_home()
        }
        None => crate::service_home::configured_home()?
            .map_or_else(folddb_profile::paths::folddb_home, Ok),
    }
}

mod recovery;
pub use recovery::*;
