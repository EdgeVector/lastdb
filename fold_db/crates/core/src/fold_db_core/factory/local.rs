//! Local FoldDB construction (Last Store only) and at-rest seam wiring.

mod migrations;
mod store_stack;

use crate::crypto::keyring::{KeyPurpose, Keyring};
use crate::crypto::keyring_provider::KeyringCryptoProvider;
use crate::crypto::{CryptoProvider, E2eKeys, LocalCryptoProvider};
use crate::db_operations::DbOperations;
use crate::error::{FoldDbError, FoldDbResult};
use crate::fold_db_core::fold_db::FoldDbInit;
use crate::fold_db_core::FoldDB;
use crate::security::Ed25519KeyPair;
use crate::storage::node_config_store::NodeConfigStore;
use crate::storage::{LastStoreNamespacedStore, StorageEngine};
use laststore::{HashGroupKey, LastStoreOptions};
use std::sync::Arc;

use super::boot::{assert_critical_namespaces_decryptable, map_boot_decrypt_error};

#[cfg(feature = "cloud-sync")]
pub(super) type LocalSyncSetup = crate::sync::SyncSetup;

#[cfg(not(feature = "cloud-sync"))]
pub(super) struct LocalSyncSetup;

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
    #[cfg(feature = "cloud-sync")]
    let bootstrap_from_cloud =
        sync_setup.is_some() && !defer_cloud_workers && laststore_is_empty_home(path);

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

    // Plain hash-group: seal atom content field. Frame-AEAD protects via
    // packaging. Segment-log uses whole-value ENC seam instead.
    let atom_content_key = match (
        primary_store
            .laststore
            .as_ref()
            .map(|s| s.options().packaging),
        primary_store
            .laststore
            .as_ref()
            .map(|s| s.options().layout_mode),
    ) {
        (Some(laststore::PackagingMode::Plain), Some(laststore::LayoutMode::HashGroup)) => {
            Some(e2e_keys.encryption_key())
        }
        _ => None,
    };
    // HashKey blind + RangeKey OPE (design-lastdb-hashkey-blind-v1 /
    // design-lastdb-rangekey-ope-v1): product default is blind+OPE when env is
    // unset (fresh Mini). Explicit plain via env for tests. Keys always come
    // from identity E2E root so first boot writes encrypted storage keys.
    let hash_key_encoding = crate::atom::HashKeyEncoding::from_env_or_default();
    let range_key_encoding = crate::atom::RangeKeyEncoding::from_env_or_default();
    tracing::info!(
        ?hash_key_encoding,
        ?range_key_encoding,
        "molecule key storage encoding (LASTDB_HASH_KEY_ENCODING / LASTDB_RANGE_KEY_ENCODING; default blind_v1+ope_v1)"
    );
    let hash_key_codec = crate::atom::MoleculeKeyCodec::with_encodings(
        hash_key_encoding,
        range_key_encoding,
        Some(e2e_keys.index_key()),
        Some(e2e_keys.ope_key()),
    );
    let db_ops = DbOperations::from_namespaced_store_with_atom_and_molecule_keys(
        store,
        atom_content_key,
        hash_key_codec,
        Some(e2e_keys.encryption_key()),
    )
    .await
    .map_err(|e| FoldDbError::Config(e.to_string()))?;

    // Boot-time decrypt integrity gate (incident-lastdbd-0226-wrong-key-fresh-db):
    // if the local store already holds user namespaces, every critical
    // encrypted namespace must open cleanly under the current key. A single
    // undecryptable row used to surface later as empty board history while the
    // daemon kept serving and writing under the same home — refuse instead.
    if let Some(enc) = enc_store_ref.as_deref() {
        use crate::storage::traits::NamespacedStore;
        let namespaces = enc.list_namespaces().await.unwrap_or_default();
        let has_user_data = namespaces.iter().any(|ns| ns != "__sled__default");
        if has_user_data {
            assert_critical_namespaces_decryptable(enc).await?;
        }
    } else if let Some(laststore) = primary_store.laststore.as_deref() {
        laststore.verify_integrity().map_err(|e| {
            map_boot_decrypt_error(format!("laststore frame/footer verification failed: {e}"))
        })?;
    }

    let fold_db = FoldDB::initialize_from_init(FoldDbInit {
        db_ops: Arc::new(db_ops),
        db_path: path_str.to_string(),
        signer,
        search_outbox_inbox,
        #[cfg(feature = "cloud-sync")]
        mutation_log_capture: Some(Arc::clone(&store_stack.mutation_log_capture)),
        #[cfg(feature = "cloud-sync")]
        packing_slots: store_stack
            .sync_engine
            .as_ref()
            .map(|engine| engine.packing_slots()),
    })
    .await
    .map_err(|e| map_boot_decrypt_error(e.to_string()))?;

    fold_db.set_config_store(config_store);

    #[cfg(feature = "cloud-sync")]
    if let Some(engine) = store_stack.sync_engine {
        if defer_cloud_workers {
            engine.set_backup_only_mode().await;
            // The active configuration plus a durable resume marker means
            // local writes after this boot must enter the tail. The backup-only
            // interlock still refuses upload and no worker starts below.
            engine.set_cloud_sync_disabled(false).await;
            fold_db.set_sync_engine(Arc::clone(&engine)).await;
        } else {
            fold_db.set_sync_engine(Arc::clone(&engine)).await;
        }
        let bootstrap_result: FoldDbResult<()> = async {
            if bootstrap_from_cloud {
                engine.bootstrap_all().await.map_err(|e| {
                    FoldDbError::Config(format!("cloud photograph bootstrap failed: {e}"))
                })?;
            }
            // Restore org (+ share) cloud-sync targets registered on prior boots.
            if let Err(e) = fold_db.apply_org_sync_targets_from_store().await {
                tracing::warn!(
                    error = %e,
                    "failed to re-apply org sync targets from store at boot"
                );
            }
            if bootstrap_from_cloud {
                engine.bootstrap_targets_from(1).await.map_err(|e| {
                    FoldDbError::Config(format!("scoped cloud bootstrap failed: {e}"))
                })?;
            }
            Ok(())
        }
        .await;
        match bootstrap_result {
            Ok(()) => {
                if !defer_cloud_workers {
                    engine.start_laststore_backup_uploader();
                    fold_db.start_sync(store_stack.sync_interval_ms);
                }
            }
            Err(err) => {
                engine.stop_laststore_backup_uploader();
                return Err(err);
            }
        }
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

fn open_primary_store(
    path: &std::path::Path,
    e2e_keys: &E2eKeys,
) -> FoldDbResult<OpenPrimaryStore> {
    // Packaging modes:
    // - **plain** (default new hash-group): structural group files are
    //   plaintext; atom *content* is field-sealed. No LastStore data_key /
    //   frame AEAD → restart-safe open tails.
    // - **frame_aead** (legacy LSF1): keep data_key; no nested ENC seam.
    // - **segment_log** keyless: value-level ENC seam for legacy homes.
    let high_water_path = laststore_high_water_path(path);
    // Product runtime defaults always include the hash-group warm budget
    // (256 MiB). Durable layout descriptor / detect still override layout_mode,
    // packaging, and group bits via `open_existing_or_with` — so legacy
    // segment_log homes stay segment_log, but hash-group reopens keep eviction
    // ON after the first write. New homes use `LastStoreOptions::hash_group`.
    // The warm budget is tunable at startup via `LASTDB_HASH_GROUP_WARM_BYTES`
    // (see `hash_group_warm_bytes_from_env`): a HashGroup prefix/range scan
    // visits every group shard, so if the scanned working set exceeds the
    // budget the store re-loads each group per read (0.23.1 scan-thrash that
    // pins CPU). Raising the budget keeps those shards resident. It does not
    // size the logical resident set.
    let needs_data_key = laststore_needs_data_key(path);
    // Runtime-only rollout guard. Durable placement still comes from the
    // home's descriptor; never silently rekey an existing home here.
    let reads_require_partition =
        env_flag::var_parse("LASTDB_READS_REQUIRE_PARTITION").unwrap_or(false);
    let store = if needs_data_key {
        let base = LastStoreOptions::hash_group_frame_aead(e2e_keys.encryption_key());
        let warm_bytes = hash_group_warm_bytes_from_env(base.hash_group_warm_bytes);
        let key_cache_bytes = hash_group_key_cache_bytes_from_env(base.hash_group_key_cache_bytes);
        // No `LASTDB_HASH_GROUP_KEY_SIDECAR` here on purpose. An id sidecar is
        // plaintext, and frame-AEAD exists so the group files are not; LastStore
        // refuses to write one for this packaging regardless. Reading the env
        // var on this branch would only suggest the knob does something.
        let max_handles = hash_group_warm_max_handles_from_env(base.hash_group_warm_max_handles);
        let cold_group_cap = max_cold_group_load_bytes_from_env(base.max_cold_group_load_bytes);
        let soft_cold_group_cap =
            soft_cold_group_load_bytes_from_env(base.soft_cold_group_load_bytes);
        let opts = base
            .with_reads_require_partition(reads_require_partition)
            .with_hash_group_key(HashGroupKey::PartitionPrefix)
            .with_hash_group_warm_bytes(warm_bytes)
            .with_hash_group_key_cache_bytes(key_cache_bytes)
            .with_hash_group_warm_max_handles(max_handles)
            .with_max_cold_group_load_bytes(cold_group_cap)
            .with_soft_cold_group_load_bytes(soft_cold_group_cap);
        Arc::new(
            LastStoreNamespacedStore::open_with_options_and_high_water_data_key(
                path,
                opts,
                high_water_path,
            )?,
        )
    } else {
        let base = LastStoreOptions::hash_group();
        let warm_bytes = hash_group_warm_bytes_from_env(base.hash_group_warm_bytes);
        let key_cache_bytes = hash_group_key_cache_bytes_from_env(base.hash_group_key_cache_bytes);
        let key_sidecar = hash_group_key_sidecar_from_env(base.hash_group_key_sidecar);
        let max_handles = hash_group_warm_max_handles_from_env(base.hash_group_warm_max_handles);
        let cold_group_cap = max_cold_group_load_bytes_from_env(base.max_cold_group_load_bytes);
        let soft_cold_group_cap =
            soft_cold_group_load_bytes_from_env(base.soft_cold_group_load_bytes);
        let opts = base
            .with_reads_require_partition(reads_require_partition)
            .with_hash_group_key(HashGroupKey::PartitionPrefix)
            .with_hash_group_warm_bytes(warm_bytes)
            .with_hash_group_key_cache_bytes(key_cache_bytes)
            .with_hash_group_key_sidecar(key_sidecar)
            .with_hash_group_warm_max_handles(max_handles)
            .with_max_cold_group_load_bytes(cold_group_cap)
            .with_soft_cold_group_load_bytes(soft_cold_group_cap);
        Arc::new(LastStoreNamespacedStore::open_with_options_and_high_water(
            path,
            opts,
            high_water_path,
        )?)
    };
    // Frame-AEAD packaging: frames are the only envelope — no nested value ENC.
    // Plain packaging (segment_log *or* hash_group): keep the value-level ENC
    // dual-read seam so legacy `ENC:…` rows (pre–content-field-seal, and any
    // migrated journal data) still decrypt. Atom content-field seal is layered
    // on top for new plain hash-group atom writes and dual-reads open.
    //
    // Won't-undo: hash_group + wrap=false made CoW migrate of Tom's primary
    // fail boot with `Serialization error: expected value at line 1 column 1`
    // when TypedKvStore tried to parse ciphertext as JSON.
    let wrap_at_rest_seam = !needs_data_key;
    Ok(OpenPrimaryStore {
        store: store.clone() as Arc<dyn crate::storage::traits::NamespacedStore>,
        laststore: Some(store),
        wrap_at_rest_seam,
    })
}

/// Mini layout: `$HOME/data` store root → `$HOME/laststore_high_water.json`.
/// Tests / ad-hoc roots keep the marker beside the store path.
///
/// Delegates to the storage layer so the writer of the marker and the readers
/// of it (status durability, `lastdb` CLI) resolve one path by one rule.
fn laststore_high_water_path(path: &std::path::Path) -> std::path::PathBuf {
    crate::storage::laststore::high_water_path_for_store_root(path)
}

/// Open with frame AEAD `data_key` only for **frame-AEAD** packaging homes
/// (LSF1 magic and/or descriptor `packaging=frame_aead`).
///
/// Fresh/empty homes and plain hash-group packaging stay **keyless**. Atom body
/// secrecy is field-level content seal, not group-file frames.
fn laststore_needs_data_key(path: &std::path::Path) -> bool {
    // Layout descriptor wins even for empty restored homes. `lastdb restore`
    // opens with a data_key and may write `packaging=frame_aead` before any
    // collection chunks land; treating empty-as-plaintext then fails with
    // "packaging=frame_aead requires data_key" on first Mini boot.
    let layout = path.join("laststore-layout-v1");
    if let Ok(text) = std::fs::read_to_string(&layout) {
        if text.lines().any(|l| l.trim() == "packaging=frame_aead") {
            return true;
        }
    }
    if laststore_is_empty_home(path) {
        return false;
    }
    if laststore::home_has_frame_aead_segments(path) {
        return true;
    }
    false
}

fn laststore_is_empty_home(path: &std::path::Path) -> bool {
    let collections = path.join("data");
    if !collections.exists() {
        return true;
    }
    let Ok(rd) = std::fs::read_dir(&collections) else {
        return true;
    };
    let mut saw_any = false;
    for entry in rd.filter_map(Result::ok) {
        let coll = entry.path();
        if !coll.is_dir() {
            continue;
        }
        saw_any = true;
    }
    !saw_any
}
