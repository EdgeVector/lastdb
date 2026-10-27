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
//! uses that E2E content key — never a per-install keyring store DEK. Host passes
//! `at_rest_keyring: None` (`design-portable-same-key-at-rest-cloud`).
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

/// Historical credential file name, also read by explicit backup enrollment.
pub const CLOUD_SYNC_CONFIG_FILE: &str = "cloud_sync.json";

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

/// Whether stderr may receive a raw recovery phrase.
///
/// CI logs and cargo-test captures are retained. Recovery material must never
/// appear there, even for ephemeral test identities
/// (`papercut-fold-ci-test-recovery-phrase-leaks-to-logs-20260817`).
pub fn should_emit_recovery_phrase() -> bool {
    if cfg!(test) {
        return false;
    }
    if std::env::var_os("CI").is_some() {
        return false;
    }
    if std::env::var_os("LASTDB_SUPPRESS_RECOVERY_PHRASE").is_some() {
        return false;
    }
    io::stderr().is_terminal()
}

/// Lines written when a fresh identity is minted. `emit_secret=false` is the
/// test/CI/non-TTY path and must never contain the mnemonic.
pub fn format_recovery_phrase_notice(mnemonic: &str, emit_secret: bool) -> String {
    if emit_secret {
        format!(
            "\n\
             === LastDB recovery phrase (save this; only shown once) ===\n\
             {mnemonic}\n\
             ===========================================================\n\
             This phrase is your account root: identity, content encryption, \
             HashKey blind, and RangeKey OPE. Enabling cloud later reuses it — \
             no re-encryption of molecule keys.\n"
        )
    } else {
        "=== LastDB recovery phrase generated (redacted; not printed in test/CI/non-TTY) ===\n"
            .to_string()
    }
}

/// Print a freshly generated recovery phrase, or a redacted stand-in.
pub fn emit_fresh_recovery_phrase(mnemonic: &str) {
    let emit_secret = should_emit_recovery_phrase();
    eprint!("{}", format_recovery_phrase_notice(mnemonic, emit_secret));
    if emit_secret {
        tracing::info!("fresh identity generated; recovery phrase printed on stderr");
    } else {
        tracing::info!("fresh identity generated; recovery phrase redacted (test/CI/non-TTY)");
    }
}

/// File under the node home recording the last boot failure message so
/// `lastdb status` / operators can see why a KeepAlive-restarted service is
/// not serving (without digging through rotated logs).
pub const LAST_BOOT_ERROR_FILE: &str = "last_boot_error.txt";

/// macOS Spotlight opt-out marker. An empty file with this name excludes the
/// directory it sits in — and everything below it — from Spotlight indexing.
pub const SPOTLIGHT_EXCLUSION_FILE: &str = ".metadata_never_index";

/// Create `home` if needed and keep it out of the macOS Spotlight index.
///
/// Every path that can materialize a *live* node home routes through this, so a
/// home is never left indexable just because `cloud connect` created it before
/// the first [`Host::boot`].
pub fn ensure_node_home(home: &Path) -> io::Result<()> {
    std::fs::create_dir_all(home)?;
    mark_never_indexed(home);
    Ok(())
}

/// Write [`SPOTLIGHT_EXCLUSION_FILE`] at the node-home root. Idempotent; never
/// truncates an existing marker, so an operator can put notes in it.
///
/// Two reasons this is a default rather than a tuning knob:
///
/// * **At-rest hygiene.** The run files are encrypted, but some files in the
///   home are plain text, for example the Search inbox batches. Spotlight
///   copies indexed content into a system-wide index that sits *outside* the
///   home and outside any LastDB access control.
/// * **Read-path cost.** Each commit writes a new run file and background
///   merges rewrite them, and every new file is fresh re-index work against the
///   same disk the read path uses.
///
/// Best-effort by construction: an unwritable home is reported by the
/// `create_dir_all` in [`ensure_node_home`], and a missing marker must never
/// fail a boot.
///
/// Deliberately *not* called for `restore --into` destinations: those homes
/// pick the marker up on their first real boot.
#[cfg(target_os = "macos")]
pub fn mark_never_indexed(home: &Path) {
    let marker = home.join(SPOTLIGHT_EXCLUSION_FILE);
    match std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&marker)
    {
        Ok(_) => tracing::info!(
            path = %marker.display(),
            "excluded node home from Spotlight indexing"
        ),
        Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {}
        Err(e) => tracing::warn!(
            target: "lastdb_node::host",
            path = %marker.display(),
            error = %e,
            "could not exclude node home from Spotlight indexing; \
             plain text files in the home may be copied into the system index"
        ),
    }
}

/// No-op off macOS: Spotlight is the only indexer this marker speaks to.
/// Porting seam for a Windows Search / tracker3 equivalent.
#[cfg(not(target_os = "macos"))]
pub fn mark_never_indexed(_home: &Path) {}

/// The layout descriptor that the deleted LastStore engine wrote into its root
/// on first open. A home from that engine still has it, so the check below
/// still refuses to start fresh over one.
const LASTSTORE_LAYOUT_FILE: &str = "laststore-layout-v1";

/// The node marker. Every native node home has it, in disk format v1 and v2.
const NODE_MARKER_FILE: &str = "laststore-pack-node-v1";

/// Whether `<home>/data` already holds a store (a layout file, a node marker, or a collection).
/// Used to refuse cloud-bootstrap-as-heal and to keep error messages precise.
///
/// Recognises the layouts the node wrote or writes: the LastStore layout file,
/// the node marker (disk format v1 and v2), or any collection directory under
/// `<data>/data`. The refuse-to-start-fresh contract below rests on this
/// structural answer, not on the storage layer's error text.
pub fn data_dir_has_existing_store(data_path: &Path) -> bool {
    // LastStore: `<data>/laststore-layout-v1` + `<data>/data/<collection>/…`.
    if data_path.join(LASTSTORE_LAYOUT_FILE).is_file() {
        return true;
    }
    // A disk format v2 home has the node marker and no layout file.
    if data_path.join(NODE_MARKER_FILE).is_file() {
        return true;
    }
    let collections = data_path.join("data");
    std::fs::read_dir(&collections)
        .is_ok_and(|entries| entries.filter_map(Result::ok).any(|e| e.path().is_dir()))
}

fn is_decrypt_boot_failure(msg: &str) -> bool {
    let lower = msg.to_ascii_lowercase();
    lower.contains("cannot decrypt existing store")
        || lower.contains("aead")
        || lower.contains("aes-gcm")
        || lower.contains("decrypt")
        || lower.contains("wrong encryption key")
        || lower.contains("wrong master key")
        || lower.contains("encryption error")
        || lower.contains("undecryptable")
}

/// Map a core boot error into the loud refuse-to-boot message required by
/// incident-lastdbd-0226-wrong-key-fresh-db, and persist it for operators.
fn map_and_record_boot_error(
    home: &Path,
    existing_store: bool,
    err: impl std::fmt::Display,
) -> String {
    let raw = err.to_string();
    let msg = if is_decrypt_boot_failure(&raw) {
        if raw.contains("cannot decrypt existing store") {
            raw
        } else {
            format!(
                "cannot decrypt existing store — wrong master key or incompatible build; \
                 refusing to start fresh: {raw}"
            )
        }
    } else if existing_store {
        // Structural, not lexical: `is_decrypt_boot_failure` is a substring
        // heuristic over someone else's error text, so a re-worded storage
        // error would silently demote a wrong-key boot over real data to a
        // generic failure. If the home held data before this process touched
        // it, any boot failure is a refuse-to-start-fresh event regardless of
        // how the underlying layer phrased it.
        format!(
            "cannot open existing store — refusing to start fresh over existing data at {}: {raw}",
            home.display()
        )
    } else {
        format!("core database boot failed: {raw}")
    };
    record_last_boot_error(home, &msg);
    msg
}

fn record_last_boot_error(home: &Path, msg: &str) {
    let path = home.join(LAST_BOOT_ERROR_FILE);
    let body = format!("{}\n", msg.lines().next().unwrap_or(msg));
    if let Err(e) = std::fs::write(&path, body) {
        tracing::warn!(
            target: "lastdbd::boot",
            error = %e,
            path = %path.display(),
            "failed to write last_boot_error.txt"
        );
    }
}

fn clear_last_boot_error(home: &Path) {
    let path = home.join(LAST_BOOT_ERROR_FILE);
    let _ = std::fs::remove_file(path);
}

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
        let at_rest_provider = fold_db::fold_db_core::factory::at_rest_provider_label(None);
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
    fn fold_db(&self) -> &Arc<FoldDB> {
        &self.db
    }

    fn public_key(&self) -> String {
        self.keypair.public_key_base64()
    }

    fn owner_user_hash(&self) -> String {
        self.user_hash.clone()
    }

    async fn acquire_op_permit(
        &self,
        lane: lastdb_host::Lane,
    ) -> Result<Box<dyn lastdb_host::ReadPermit>, lastdb_host::ReadBusy> {
        self.qos
            .acquire(lane)
            .await
            .map(lastdb_host::QosPermit::boxed)
    }

    async fn wait_for_background_tasks(&self, timeout: std::time::Duration) -> bool {
        self.db.wait_for_background_tasks(timeout).await
    }
}

/// Resolve the node home for `lastdbd`: an explicit `--data-dir` wins, then
/// `LASTDB_HOME` / `FOLDDB_HOME`, then a persisted `lastdbd service-home`, then
/// the shared honor-both default (`~/.lastdb` → `~/.folddb` → fresh `~/.lastdb`).
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
