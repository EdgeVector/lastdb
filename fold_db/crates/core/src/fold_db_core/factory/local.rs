//! Local FoldDB construction (Last Store only) and at-rest seam wiring.

mod boot_wiring;
mod migrations;
mod store_stack;

use crate::crypto::keyring::{KeyPurpose, Keyring};
use crate::crypto::keyring_provider::KeyringCryptoProvider;
use crate::crypto::{CryptoProvider, E2eKeys, LocalCryptoProvider};
use crate::error::{FoldDbError, FoldDbResult};
use crate::fold_db_core::fold_db::FoldDbInit;
use crate::fold_db_core::FoldDB;
use crate::security::Ed25519KeyPair;
use crate::storage::node_config_store::NodeConfigStore;
use crate::storage::{LastStoreNamespacedStore, StorageEngine};
use laststore::{HashGroupKey, LastStoreOptions};
use std::sync::Arc;

use super::boot::{assert_critical_namespaces_decryptable, map_boot_decrypt_error};
use boot_wiring::{open_db_operations, verify_boot_decrypt_integrity, wire_sync_engine};

pub(super) type LocalSyncSetup = crate::sync::SyncSetup;

/// Ops-facing label for which key seals personal at-rest data on the store seam.
///
/// - `"e2e_content_key"` — account-portable E2E content key (Mini production;
///   same key cloud log/snapshot outer seal uses).
/// - `"keyring_store_dek"` — per-install keyring store DEK (non-Mini / test /
///   optional future; **not portable** across devices).
pub fn at_rest_provider_label(at_rest_keyring: Option<&Arc<Keyring>>) -> &'static str {
    match at_rest_keyring {
        Some(_) => "keyring_store_dek",
        None => "e2e_content_key",
    }
}

/// Build the [`CryptoProvider`] that seals/opens the at-rest
/// [`EncryptingNamespacedStore`] seam (the `main` / `metadata` namespaces).
///
/// # Product rule (Mini — Tom 2026-07-18)
///
/// **LastDB Mini personal data always uses the account E2E content key**
/// (`at_rest_keyring = None` → [`LocalCryptoProvider`] on
/// [`E2eKeys::encryption_key`]). That key is the same on every device that
/// shares the identity seed, and is the same material cloud uses for outer
/// seal. Mini host **must not** pass a store keyring DEK
/// (`design-portable-same-key-at-rest-cloud`).
///
/// # Keyring branch (non-Mini / tests only)
///
/// - **With an unlocked keyring** → [`KeyringCryptoProvider`] under the
///   keyring's *store*-purpose active DEK (envelope v2, `key_id`-stamped).
///   That DEK is **per-install and not portable**; cloud still seals with
///   the content key. Do not enable this on Mini personal data.
/// - **Without a keyring** → content-key [`LocalCryptoProvider`] (Mini path).
pub(super) fn store_seam_crypto(
    e2e_keys: &E2eKeys,
    at_rest_keyring: Option<&Arc<Keyring>>,
) -> Arc<dyn CryptoProvider> {
    match at_rest_keyring {
        Some(keyring) => Arc::new(KeyringCryptoProvider::new(
            Arc::clone(keyring),
            KeyPurpose::Store,
        )),
        None => Arc::new(LocalCryptoProvider::from_key(e2e_keys.encryption_key())),
    }
}

/// Creates a local FoldDB (Last Store) with optional S3 sync.
///
/// When `sync_setup` is provided, sync is constructed beside the local store.
/// The hot read/write path remains local-only.
///
/// **Mini:** at-rest seals with the account E2E content key (portable every
/// device — same as cloud). **Do not** pass a keyring store DEK on Mini.
/// Keyring DEK remains available only for non-Mini/tests; cloud outer seal
/// always uses the content key regardless.
pub(super) struct LocalFoldDbOptions {
    pub(super) sync_setup: Option<LocalSyncSetup>,
    pub(super) at_rest_keyring: Option<Arc<Keyring>>,
    pub(super) storage_engine: StorageEngine,
    pub(super) search_outbox_inbox: Option<std::path::PathBuf>,
    pub(super) defer_cloud_workers: bool,
}

pub(super) async fn create_local_fold_db(
    path: &std::path::Path,
    e2e_keys: &E2eKeys,
    signer: Arc<Ed25519KeyPair>,
    options: LocalFoldDbOptions,
) -> FoldDbResult<Arc<FoldDB>> {
    let LocalFoldDbOptions {
        sync_setup,
        at_rest_keyring,
        storage_engine,
        search_outbox_inbox,
        defer_cloud_workers,
    } = options;
    let path_str = path
        .to_str()
        .ok_or_else(|| FoldDbError::Config("Invalid storage path".to_string()))?;

    // StorageEngine is Laststore-only (sled removed 2026-07-22). The value is
    // still threaded for DatabaseConfig serde continuity; there is no
    // multi-engine branch.
    create_local_fold_db_inner(
        path,
        path_str,
        e2e_keys,
        signer,
        sync_setup,
        at_rest_keyring,
        storage_engine,
        search_outbox_inbox,
        defer_cloud_workers,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn create_local_fold_db_inner(
    path: &std::path::Path,
    path_str: &str,
    e2e_keys: &E2eKeys,
    signer: Arc<Ed25519KeyPair>,
    sync_setup: Option<LocalSyncSetup>,
    at_rest_keyring: Option<Arc<Keyring>>,
    storage_engine: StorageEngine,
    search_outbox_inbox: Option<std::path::PathBuf>,
    defer_cloud_workers: bool,
) -> FoldDbResult<Arc<FoldDB>> {
    // An empty cloud-connected home must restore S and its writer tail before
    // steady sync can apply new log entries. Capture this before LastStore open
    // creates layout metadata that makes the directory non-empty.
    let local_proof_engine = sync_setup.as_ref().is_some_and(|s| s.local_proof_engine);
    let bootstrap_from_cloud = sync_setup.is_some()
        && !defer_cloud_workers
        && !local_proof_engine
        && laststore_is_empty_home(path);

    if crate::storage::disk_pack::requested(path) && sync_setup.is_some() {
        return Err(FoldDbError::Config(
            "native disk homes require their complete backup and cloud-sync gate before activation"
                .into(),
        ));
    }

    let primary_store = open_primary_store(path, e2e_keys)?;
    let base_store = Arc::clone(&primary_store.store);

    // Create the config store for runtime node configuration.
    // Pass the E2E encryption key so sensitive fields (node identity
    // private key) are encrypted at rest via AES-256-GCM.
    let config_store =
        open_node_config_store(Arc::clone(&base_store), Some(e2e_keys.encryption_key())).await?;

    // Use the sync_setup provided by the caller. The host is responsible for
    // loading the API key from the per-device credentials file.

    let laststore_backup_source = primary_store.laststore.clone();

    // The signer was loaded and validated by the caller (in production,
    // from the node's persistent identity). We share the same Arc with
    // both SyncEngine (for signing merged molecules during replay) and
    // FoldDB (used by MutationManager for signing local writes) so
    // merged writes trace to the same node identity as direct writes.

    let store_stack = store_stack::build_store_stack(
        base_store,
        laststore_backup_source,
        sync_setup,
        e2e_keys,
        Arc::clone(&signer),
        at_rest_keyring.as_ref(),
        primary_store.wrap_at_rest_seam,
        storage_engine,
        defer_cloud_workers,
    )
    .await?;
    let store = Arc::clone(&store_stack.store);
    let enc_store_ref = store_stack.enc_store_ref.clone();

    // Migrations must finish before writers. The backup uploader starts later,
    // after set_sync_engine and both photograph bootstrap blocks, so an empty
    // cloud home cannot CAS a cut that races restore.
    migrations::run_boot_migrations(enc_store_ref.as_deref()).await;

    let db_ops = open_db_operations(store, &primary_store, e2e_keys).await?;

    verify_boot_decrypt_integrity(enc_store_ref.as_deref(), &primary_store).await?;

    let fold_db = FoldDB::initialize_from_init(FoldDbInit {
        db_ops: Arc::new(db_ops),
        db_path: path_str.to_string(),
        signer,
        search_outbox_inbox,
        mutation_log_capture: Some(Arc::clone(&store_stack.mutation_log_capture)),
        packing_slots: store_stack
            .sync_engine
            .as_ref()
            .map(|engine| engine.packing_slots()),
    })
    .await
    .map_err(|e| map_boot_decrypt_error(e.to_string()))?;

    fold_db.set_config_store(config_store);

    if let Some(engine) = store_stack.sync_engine {
        if local_proof_engine {
            // No cloud: attach for capture and replay only. No bootstrap, no
            // backup uploader, no sync loop.
            fold_db.set_sync_engine(engine).await;
            return Ok(Arc::new(fold_db));
        }
        wire_sync_engine(
            &fold_db,
            engine,
            defer_cloud_workers,
            bootstrap_from_cloud,
            store_stack.sync_interval_ms,
        )
        .await?;
    }

    Ok(Arc::new(fold_db))
}

struct OpenPrimaryStore {
    store: Arc<dyn crate::storage::traits::NamespacedStore>,
    laststore: Option<Arc<LastStoreNamespacedStore>>,
    wrap_at_rest_seam: bool,
}

async fn open_node_config_store(
    base_store: Arc<dyn crate::storage::traits::NamespacedStore>,
    identity_key: Option<[u8; 32]>,
) -> FoldDbResult<NodeConfigStore> {
    NodeConfigStore::with_namespaced_store(base_store, identity_key)
        .await
        .map_err(|e| FoldDbError::Config(format!("Failed to open config store: {e}")))
}

/// Resident warm-set budget (bytes) for hash-group shard handles, overridable
/// via `LASTDB_HASH_GROUP_WARM_BYTES` (positive integer, bytes). Unset, invalid,
/// or non-positive falls back to the product preset default (256 MiB from
/// [`LastStoreOptions::hash_group`]). A HashGroup prefix/range scan visits every
/// group shard; when the scanned working set exceeds this budget, shards are
/// evicted and re-loaded on the next read, turning scans into repeated full
/// shard parses (the 0.23.1 CPU-pinning read regression). Raising this keeps
/// the scanned groups resident. Mirrors the `LASTDB_*` env-tunable idiom used
/// elsewhere (e.g. `LASTDB_UDS_HANDLER_TIMEOUT_SECS`).
///
/// This bound is for non-logical collections (indexes, schema_index,
/// atom_ref_edges_v2, keep_small, metadata, cas_blobs). Tips and atoms take
/// the unpublished loader. It does not size the logical resident set; that
/// cap is [`crate::resident::RESIDENT_KEY_CAP`].
fn hash_group_warm_bytes_from_env(default: u64) -> u64 {
    env_flag::var_parsed::<u64>("LASTDB_HASH_GROUP_WARM_BYTES")
        .filter(|&n| n > 0)
        .unwrap_or(default)
}

/// Budget (bytes) for the hash-group **key-index cache**, overridable via
/// `LASTDB_HASH_GROUP_KEY_CACHE_BYTES`. Unset or invalid falls back to the
/// product preset default (64 MiB from [`LastStoreOptions::hash_group`]);
/// an explicit `0` disables the cache.
///
/// This is the companion to `LASTDB_HASH_GROUP_WARM_BYTES` and trades a little
/// memory for walk latency the warm budget cannot buy. A keys-only pass visits
/// *every* group in the collection, and rebuilding one group's index costs a
/// full segment read + decrypt + parse — even though the ids are a small
/// fraction of that segment. Retaining the ids of evicted groups lets repeat
/// walks (`kanban list`, `lastgit list`) skip the read entirely while their
/// bodies stay evicted.
fn hash_group_key_cache_bytes_from_env(default: u64) -> u64 {
    env_flag::var_or("LASTDB_HASH_GROUP_KEY_CACHE_BYTES", default)
}

/// Fraction of the process's fd limit the warm set may hold as group handles.
///
/// The rest is for everything else the node has open at once: two control
/// sockets plus their accepted connections, the log files, cloud-backup chunk
/// enumeration, file-blob staging, and the transient opens of compaction and
/// sidecar writes. 60% leaves that room while still allowing a large resident
/// working set.
const WARM_HANDLE_FD_FRACTION: u64 = 60;

/// Soft `RLIMIT_NOFILE` for this process, if it can be read.
#[cfg(unix)]
fn process_fd_limit() -> Option<u64> {
    // SAFETY: `getrlimit` writes a fully-initialized `rlimit` on success and
    // reads nothing from the zeroed input.
    let mut lim = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut lim) } != 0 {
        return None;
    }
    // `RLIM_INFINITY` would make the derived cap meaningless; treat it as
    // "no usable limit" and fall back to the crate default.
    if lim.rlim_cur == libc::RLIM_INFINITY {
        return None;
    }
    // `libc::rlim_t` is `u64` on the targets this ships to, so the conversion is
    // a no-op here — kept because the type is platform-defined and a target
    // where it is not `u64` should fail the conversion, not truncate silently.
    #[allow(clippy::useless_conversion)]
    u64::try_from(lim.rlim_cur).ok().filter(|&n| n > 0)
}

#[cfg(not(unix))]
fn process_fd_limit() -> Option<u64> {
    None
}

/// Resident **handle** cap for hash-group shard handles, overridable via
/// `LASTDB_HASH_GROUP_WARM_MAX_HANDLES` (`0` disables the cap).
///
/// This is a file-descriptor budget, and it is not the same budget as
/// `LASTDB_HASH_GROUP_WARM_BYTES`. A resident group keeps its append handle
/// open; only eviction closes it. With many small groups the fd ceiling arrives
/// long before the byte ceiling, and `collections x groups` on this node exceeds
/// any reasonable fd limit — so bytes alone cannot bound descriptors.
///
/// Unset derives the cap from the process's own soft `RLIMIT_NOFILE`, because a
/// constant cannot be right across a 256-fd shell and an 8,192-fd LaunchAgent.
/// If the limit is unreadable or unlimited, the LastStore preset default stands.
///
/// Why it exists: 2026-07-30T07:48:30Z the primary's UDS accept loop began
/// failing with `EMFILE` and every client — brain, kanban, situations, lastgit —
/// saw an empty reply for ~ten minutes, while the process stayed up, sampled
/// metrics normally, and held RSS far under its memory guard. It had 8,167 group
/// handles open against a limit of 8,192, with resident bytes at 2.75 of 4 GiB.
fn hash_group_warm_max_handles_from_env(default: usize) -> usize {
    if let Ok(raw) = std::env::var("LASTDB_HASH_GROUP_WARM_MAX_HANDLES") {
        if let Ok(n) = raw.trim().parse::<usize>() {
            return n;
        }
    }
    match process_fd_limit() {
        Some(limit) => usize::try_from(limit * WARM_HANDLE_FD_FRACTION / 100)
            .ok()
            .filter(|&n| n > 0)
            .unwrap_or(default),
        None => default,
    }
}

/// Whether hash-group id **sidecars** are persisted, overridable via
/// `LASTDB_HASH_GROUP_KEY_SIDECAR` (`1`/`true`/`on`/`yes` to force on,
/// `0`/`false`/`off`/`no` to force off). Unset or unrecognized keeps the
/// product preset default (on for plain hash-group homes).
///
/// The third knob in the same family. `LASTDB_HASH_GROUP_WARM_BYTES` keeps
/// group *bodies* resident and `LASTDB_HASH_GROUP_KEY_CACHE_BYTES` keeps their
/// *ids* in memory once seen — but both die with the process, so the first keys
/// pass after a restart still reads every segment just to recover ids. The
/// sidecar is that tier on disk. It is advisory and revalidated against the
/// segments on every read, so turning it off can only cost the cold walk time,
/// never correctness. Ignored on frame-AEAD homes, which never get one.
fn hash_group_key_sidecar_from_env(default: bool) -> bool {
    env_flag::var_parse("LASTDB_HASH_GROUP_KEY_SIDECAR").unwrap_or(default)
}

/// HARD cap: largest cold hash group the store will load whole (on-disk
/// bytes), overridable via `LASTDB_MAX_COLD_GROUP_LOAD_BYTES`. Unset, invalid
/// or `0` keeps the LastStore default, a flat 2 GiB (1 GiB before
/// 2026-09-25). A plain byte count: "2GiB" does not parse.
///
/// The fourth knob in the family, and the only one that refuses work. On
/// 2026-09-21 the primary's `metadata/0/g/025` held 39 GB of superseded
/// `keep_small:meters` snapshots; the first point write after boot loaded
/// the group whole, the process crossed its 16 GiB memory guard, and the
/// daemon restarted every 7-17 minutes. Over the cap a load now fails the
/// one request with `ColdGroupTooLarge` (naming the group) instead of the
/// process. Per group, so sharded planes are unaffected: the next-largest
/// group on that primary was 79 MB (`cas_blobs`).
fn max_cold_group_load_bytes_from_env(default: u64) -> u64 {
    env_flag::var_or("LASTDB_MAX_COLD_GROUP_LOAD_BYTES", default)
}

/// SOFT cap, overridable via `LASTDB_SOFT_COLD_GROUP_LOAD_BYTES` (plain byte
/// count; unset, invalid or `0` keeps the LastStore default, 1 GiB). A cold
/// group past it still loads and logs `LASTSTORE_COLD_GROUP_OVER_SOFT_CAP`;
/// the plane compactors reclaim it under the backup publish-target lock.
/// Tom, 2026-09-25: a group that only piled up superseded copies must not
/// make the node unbootable or unupgradeable.
fn soft_cold_group_load_bytes_from_env(default: u64) -> u64 {
    env_flag::var_or("LASTDB_SOFT_COLD_GROUP_LOAD_BYTES", default)
}

mod open_store;
use open_store::{laststore_is_empty_home, open_primary_store};
