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
//! uses that E2E content key — never a per-install keyring store DEK. Cloud
//! outer seal uses the same content key. Host always passes
//! `at_rest_keyring: None` (`design-portable-same-key-at-rest-cloud`).
//!
//! Cloud sync ships in the binary but stays DORMANT until configured: when
//! `<home>/cloud_sync.json` deserializes into a
//! [`CloudSyncConfig`](fold_db::storage::config::CloudSyncConfig) the factory
//! layers encrypted S3 sync on, exactly as the full node does; when the file
//! is absent the daemon runs local-only and makes no outbound connections.

use std::io::{self, IsTerminal};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};

use fold_db::crypto::E2eKeys;
use fold_db::fold_db_core::FoldDB;
use fold_db::security::Ed25519KeyPair;
use fold_db::storage::config::{CloudSyncConfig, DatabaseConfig, StorageEngine};

/// File name of the identity seed under the node home (32 raw bytes, `0o600`).
/// Canonical definition lives in `lastdb_identity`, the identity crate shared
/// with `fold_db_node` so both binaries resolve the SAME identity from the
/// SAME node home.
pub use lastdb_identity::IDENTITY_KEY_FILE;

/// File name of the opt-in cloud-sync configuration under the node home.
/// Absent file → sync dormant, zero outbound network activity.
pub const CLOUD_SYNC_CONFIG_FILE: &str = "cloud_sync.json";

/// Per-home opt-in for the owner route that arms org cloud heads.
/// The personal cloud backup uses `cloud_sync.json` and does not depend on this file.
pub const ORG_SYNC_REGISTRATION_POLICY_FILE: &str = "org_sync_registration.json";

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct OrgSyncRegistrationPolicy {
    allow_registration: bool,
}

/// Read the org registration policy on each request, so DEV can opt in without
/// a daemon restart. Missing or invalid policy never grants registration.
pub fn org_sync_registration_allowed(home: &Path) -> Result<bool, String> {
    let path = home.join(ORG_SYNC_REGISTRATION_POLICY_FILE);
    let raw = match std::fs::read(&path) {
        Ok(raw) => raw,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(e) => return Err(format!("cannot read {}: {e}", path.display())),
    };
    let policy: OrgSyncRegistrationPolicy =
        serde_json::from_slice(&raw).map_err(|e| format!("invalid {}: {e}", path.display()))?;
    Ok(policy.allow_registration)
}

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
    /// manager, native index; encrypted sync layered on when configured).
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
    /// Single-flight state for the bounded atom reverse-edge migration.
    pub(crate) atom_ref_backfill: Arc<crate::atom_ref_backfill::AtomRefBackfillRuntime>,
    /// One owner resume job can run while the durable marker keeps cloud Off.
    pub(crate) primary_resume_job_running: Arc<std::sync::atomic::AtomicBool>,
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

/// Write `bytes` to `path` with owner-only permissions, refusing to clobber an
/// existing file (two racing first boots must not silently overwrite the
/// winner's identity).
pub(crate) fn write_owner_only(path: &Path, bytes: &[u8]) -> io::Result<()> {
    use std::io::Write as _;
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)?;
    f.write_all(bytes)?;
    f.sync_all()
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
/// * **At-rest hygiene.** LastStore catalogs are plaintext by design, on the
///   assumption that they stay inside the data dir. Spotlight copies indexed
///   content into a system-wide index that sits *outside* the AES-256-GCM
///   at-rest envelope and outside any LastDB access control.
/// * **Read-path cost.** Hash-group segments are rewritten constantly by
///   compaction and eviction, and every rewrite is fresh re-index work against
///   the same disk the read path is already scanning hard.
///
/// Best-effort by construction: an unwritable home is reported by the
/// `create_dir_all` in [`ensure_node_home`], and a missing marker must never
/// fail a boot.
///
/// Deliberately *not* called for `migrate-hash-group --into` /
/// `restore --into` destinations: `refuse_non_fresh_migration_home` requires a
/// completely empty destination, and those homes pick the marker up on their
/// first real boot anyway.
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
             plaintext catalogs may be copied into the system index"
        ),
    }
}

/// No-op off macOS: Spotlight is the only indexer this marker speaks to.
/// Porting seam for a Windows Search / tracker3 equivalent.
#[cfg(not(target_os = "macos"))]
pub fn mark_never_indexed(_home: &Path) {}

/// Read the opt-in cloud-sync configuration, if present.
fn load_cloud_sync(home: &Path) -> Result<Option<CloudSyncConfig>, String> {
    load_cloud_sync_path(&home.join(CLOUD_SYNC_CONFIG_FILE))
}

fn load_cloud_sync_path(path: &Path) -> Result<Option<CloudSyncConfig>, String> {
    if !path.exists() {
        return Ok(None);
    }
    let raw = std::fs::read(path).map_err(|e| format!("failed to read {}: {e}", path.display()))?;
    let config: CloudSyncConfig = serde_json::from_slice(&raw)
        .map_err(|e| format!("{} is not a valid cloud-sync config: {e}", path.display()))?;
    Ok(Some(config))
}

/// The layout descriptor LastStore writes into its root on first open.
/// Present even for a restored-but-still-empty home, which is why it is checked
/// before the collection walk.
const LASTSTORE_LAYOUT_FILE: &str = "laststore-layout-v1";

/// Whether `<home>/data` already holds a store (Last Store collections or leftover files).
/// Used to refuse cloud-bootstrap-as-heal and to keep error messages precise.
///
/// Must recognise the layout the node actually writes today. Until 2026-07-28
/// this looked only for sled's `conf`+`db` pair, which LastStore has never
/// written — so it answered `false` for every real Mini home, including the
/// primary, and the refuse-to-start-fresh contract below was left resting
/// entirely on substring-matching the storage layer's error text.
pub fn data_dir_has_existing_store(data_path: &Path) -> bool {
    // LastStore: `<data>/laststore-layout-v1` + `<data>/data/<collection>/…`.
    if data_path.join(LASTSTORE_LAYOUT_FILE).is_file() {
        return true;
    }
    let collections = data_path.join("data");
    if let Ok(entries) = std::fs::read_dir(&collections) {
        if entries.filter_map(Result::ok).any(|e| e.path().is_dir()) {
            return true;
        }
    }

    // Legacy sled homes predate the LastStore cutover but must still count as
    // "there is data here" — this is the branch that refuses a fresh boot over
    // a home someone restored from an old backup.
    let conf = data_path.join("conf");
    let db = data_path.join("db");
    if !(conf.is_file() && db.exists()) {
        return false;
    }
    std::fs::metadata(&db).map_or(true, |m| m.len() > 0)
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

/// Mark this home as supporting ENB and ENZ codec formats.
/// This prevents unsupported openers from treating these formats as plaintext.
fn write_codec_format_requirement(home: &Path) {
    let path = home.join(CODEC_FORMAT_REQUIREMENT_FILE);
    match std::fs::write(&path, "") {
        Ok(_) => {
            tracing::debug!(
                target: "lastdb_node::host",
                path = %path.display(),
                "marked home as supporting ENB and ENZ codec formats"
            );
        }
        Err(e) => {
            tracing::warn!(
                target: "lastdb_node::host",
                error = %e,
                path = %path.display(),
                "could not write codec format requirement marker"
            );
        }
    }
}

/// Where the backup keep set is mirrored: the manifest a publish committed.
///
/// `lastdb cloud backup-gc` diffs the cloud listing against this file and
/// refuses when it is absent, so every writer and reader must agree on one
/// location. Callers used to spell the name inline in three places.
pub fn backup_manifest_cache_path(home: &std::path::Path) -> PathBuf {
    home.join(BACKUP_MANIFEST_CACHE_FILE)
}

/// File name of the keep set. See [`backup_manifest_cache_path`].
pub const BACKUP_MANIFEST_CACHE_FILE: &str = "laststore_backup_manifest.json";

/// Marker file indicating this home supports ENB and ENZ codec formats.
pub const CODEC_FORMAT_REQUIREMENT_FILE: &str = "lastdb-codec-enb-enz-supported-v1";

impl Host {
    /// True while a node task can still write after the sampler stops.
    pub fn background_writes_in_flight(&self) -> bool {
        self.self_metrics.background_writes_in_flight() || self.atom_ref_backfill.in_flight()
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
        ensure_node_home(home)
            .map_err(|e| format!("failed to create node home {}: {e}", home.display()))?;
        if crate::cloud::cloud_backup_source_copy_path(home).exists() {
            return Err("a stopped backup copy cannot boot as a daemon".into());
        }
        crate::session_ledger::clear_shutdown_flush_receipt(home)
            .map_err(|error| format!("could not clear prior shutdown proof: {error}"))?;
        if crate::cloud::cloud_sync_file_state(home) == "off" {
            crate::cloud::mark_cloud_resume_required(home)?;
        }
        let data_path = home.join("data");
        std::fs::create_dir_all(&data_path)
            .map_err(|e| format!("failed to create data dir {}: {e}", data_path.display()))?;
        let search_outbox_inbox =
            fold_db::db_operations::search_index::search_inbox_dir_for_home(home);

        let existing_store = data_dir_has_existing_store(&data_path);

        let (seed, freshly_generated) = load_or_generate_seed_with_meta(home)?;
        if freshly_generated {
            // Same BIP39 path as `lastdbd connect` invite: phrase ↔ identity.key
            // ↔ E2E content/index/ope keys. Print once so cloud enroll later is
            // restore-from-phrase, not a second local encode dance.
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

        let cloud_sync = load_cloud_sync(home)?;
        let sync_configured = cloud_sync.is_some();
        let resume_required =
            sync_configured && crate::cloud::cloud_resume_required_path(home).exists();

        // Last Store only. Legacy sled personal bootstrap is gone — restore
        // uses `lastdb restore --into`. Auth-refresh still wires 401 self-heal
        // when cloud_sync.json is present.
        let storage_engine = StorageEngine::from_env()
            .map_err(|e| map_and_record_boot_error(home, existing_store, e))?
            .unwrap_or(StorageEngine::Laststore);

        let db = if let Some(cloud) = cloud_sync {
            tracing::info!(
                target: "lastdbd::cloud",
                "LastStore boot: skipping legacy personal bootstrap; restore uses `lastdb restore --into`"
            );

            let refresh = crate::cloud::auth_refresh_callback(
                home.to_path_buf(),
                cloud.api_url.clone(),
                Arc::clone(&keypair),
            );
            let cloud = load_cloud_sync(home)?.unwrap_or(cloud);
            let config = DatabaseConfig {
                path: data_path.clone(),
                engine: storage_engine,
                cloud_sync: Some(cloud),
            };
            // Product rule: Mini personal at-rest == account E2E content key
            // (portable every device). Never pass a store keyring DEK here.
            fold_db::fold_db_core::factory::create_fold_db_with_pool_auth_refresh_and_search_outbox(
                &config,
                &e2e_keys,
                Arc::clone(&keypair),
                Some(refresh),
                None, // at_rest_keyring: Mini = e2e_content_key only
                Some(search_outbox_inbox),
                resume_required,
            )
            .await
            .map_err(|e| map_and_record_boot_error(home, existing_store, e))?
        } else {
            let config = DatabaseConfig {
                path: data_path.clone(),
                engine: storage_engine,
                cloud_sync: None,
            };
            // Local-only Mini path also uses content-key at-rest (no keyring).
            fold_db::fold_db_core::factory::create_fold_db_with_pool_auth_refresh_and_search_outbox(
                &config,
                &e2e_keys,
                Arc::clone(&keypair),
                None,
                None,
                Some(search_outbox_inbox),
                false,
            )
            .await
            .map_err(|e| map_and_record_boot_error(home, existing_store, e))?
        };

        let user_hash = user_hash_from_pubkey(&keypair.public_key_base64())?;
        // Mirror the keep set wherever a cut publishes, not only on the
        // operator snapshot route. `lastdb cloud backup-gc` refuses without
        // this file, and the operator route can only write it if a caller runs
        // that verb in the window right after a drain lands — a window nothing
        // schedules and, on a home whose cut takes hours, one that was missed
        // for weeks while the cloud namespace accumulated orphans.
        if let Some(engine) = db.sync_engine() {
            engine.set_backup_manifest_cache_path(backup_manifest_cache_path(home));
        }
        clear_last_boot_error(home);
        write_codec_format_requirement(home);
        // Mini always boots with content-key at-rest (see module docs).
        let at_rest_provider = fold_db::fold_db_core::factory::at_rest_provider_label(None);
        tracing::info!(
            target: "lastdbd::boot",
            user_hash = %user_hash,
            cloud_sync = sync_configured,
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
            atom_ref_backfill: Arc::new(crate::atom_ref_backfill::AtomRefBackfillRuntime::default()),
            primary_resume_job_running: Arc::new(std::sync::atomic::AtomicBool::new(false)),
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

    fn atom_content_key(&self) -> Option<[u8; 32]> {
        let bytes = std::fs::read(self.home.join(IDENTITY_KEY_FILE)).ok()?;
        if bytes.len() != 32 {
            return None;
        }
        let mut seed = [0u8; 32];
        seed.copy_from_slice(&bytes);
        fold_db::crypto::E2eKeys::from_ed25519_seed(&seed)
            .ok()
            .map(|keys| keys.encryption_key())
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
