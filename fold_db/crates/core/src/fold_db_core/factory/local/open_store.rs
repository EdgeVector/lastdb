//! Local physical store selection and legacy layout detection.
use super::*;

pub(super) fn open_primary_store(
    path: &std::path::Path,
    e2e_keys: &E2eKeys,
) -> FoldDbResult<OpenPrimaryStore> {
    let high_water_path = laststore_high_water_path(path);
    let converted = laststore::disk_pack::resolve_conversion(path)
        .map_err(|error| FoldDbError::Config(format!("storage conversion: {error}")))?;
    if let Some(selected) = converted.as_ref().filter(|selected| selected.native) {
        let store = crate::storage::disk_pack::PackNamespacedStore::open_home(&selected.path)?;
        return Ok(OpenPrimaryStore {
            store: Arc::new(store),
            laststore: None,
            wrap_at_rest_seam: true,
        });
    }
    let path = converted
        .as_ref()
        .map_or(path, |selected| selected.path.as_path());
    if converted.is_none() && crate::storage::disk_pack::requested(path) {
        return open_native_store(path);
    }
    open_legacy_store(path, e2e_keys, high_water_path)
}

fn open_legacy_store(
    path: &std::path::Path,
    e2e_keys: &E2eKeys,
    high_water_path: std::path::PathBuf,
) -> FoldDbResult<OpenPrimaryStore> {
    // Packaging modes:
    // - **plain** (default new hash-group): structural group files are
    //   plaintext; atom *content* is field-sealed. No LastStore data_key /
    //   frame AEAD → restart-safe open tails.
    // - **frame_aead** (legacy LSF1): keep data_key; no nested ENC seam.
    // - **segment_log** keyless: value-level ENC seam for legacy homes.
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

fn open_native_store(path: &std::path::Path) -> FoldDbResult<OpenPrimaryStore> {
    let store = crate::storage::disk_pack::PackNamespacedStore::open(path)?;
    Ok(OpenPrimaryStore {
        store: Arc::new(store),
        laststore: None,
        wrap_at_rest_seam: true,
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

pub(super) fn laststore_is_empty_home(path: &std::path::Path) -> bool {
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
