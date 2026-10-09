//! Restore source-layout options, path normalization and fresh-home refusals.

use super::*;

pub(crate) fn restore_options_from_source_layout(
    source_data: &Path,
    data_key: [u8; 32],
    frame_aead_enabled: bool,
) -> Result<laststore::LastStoreOptions, RestoreFailure> {
    use RestoreFailureCode as Code;
    use RestoreFailureStage as Stage;

    let descriptor = laststore::describe_home(source_data)
        .map_err(|e| {
            RestoreFailure::new(
                Stage::SourceLayout,
                Code::OperationFailed,
                format!(
                    "read source LastStore layout {}: {e}",
                    source_data.display()
                ),
            )
        })?
        .ok_or_else(|| {
            RestoreFailure::new(
                Stage::SourceLayout,
                Code::OperationFailed,
                format!(
                    "source LastStore layout descriptor is missing at {}; refusing to guess restore placement",
                    source_data.display()
                ),
            )
        })?;

    let mut opts = match descriptor.layout_mode {
        laststore::LayoutMode::SegmentLog => laststore::LastStoreOptions::segment_log(),
        laststore::LayoutMode::HashGroup => laststore::LastStoreOptions::hash_group(),
    };
    opts.shard_bits = descriptor.shard_bits;
    opts.hash_group_bits = descriptor.hash_group_bits;
    opts.hash_algo = descriptor.hash_algo;
    opts.hash_group_key = descriptor.hash_group_key;
    opts.hash_group_partition_fanout = descriptor.hash_group_partition_fanout;
    opts.layout_epoch = descriptor.layout_epoch;
    opts.packaging = descriptor.packaging;

    match descriptor.packaging {
        laststore::PackagingMode::Plain => {
            if frame_aead_enabled {
                return Err(RestoreFailure::new(
                    Stage::SourceLayout,
                    Code::LayoutMismatch,
                    "source LastStore uses plain packaging; refusing LASTDB_RESTORE_FRAME_AEAD layout mismatch",
                ));
            }
            opts.data_key = None;
        }
        laststore::PackagingMode::FrameAead => {
            if !frame_aead_enabled {
                return Err(RestoreFailure::new(
                    Stage::SourceLayout,
                    Code::FrameAeadOptInRequired,
                    "source LastStore uses frame_aead packaging; set LASTDB_RESTORE_FRAME_AEAD=1",
                ));
            }
            opts.data_key = Some(data_key);
            opts.hash_group_key_sidecar = false;
        }
    }

    Ok(opts)
}

pub(crate) fn expand_home_path(path: &Path) -> Result<PathBuf, String> {
    let s = path.to_string_lossy();
    if s == "~" {
        return std::env::var_os("HOME")
            .map(PathBuf::from)
            .ok_or_else(|| "cannot resolve ~".to_string());
    }
    if let Some(rest) = s.strip_prefix("~/") {
        return std::env::var_os("HOME")
            .map(PathBuf::from)
            .map(|home| home.join(rest))
            .ok_or_else(|| "cannot resolve ~".to_string());
    }
    Ok(path.to_path_buf())
}

pub(crate) fn refuse_same_home(source_home: &Path, target_home: &Path) -> Result<(), String> {
    let canonical_source = source_home
        .canonicalize()
        .map_err(|e| format!("canonicalize {}: {e}", source_home.display()))?;
    let canonical_target = if target_home.exists() {
        target_home
            .canonicalize()
            .map_err(|e| format!("canonicalize {}: {e}", target_home.display()))?
    } else {
        target_home
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .canonicalize()
            .map_err(|e| format!("canonicalize parent of {}: {e}", target_home.display()))?
            .join(target_home.file_name().unwrap_or_default())
    };
    if canonical_source == canonical_target {
        return Err("refusing to restore into the source/primary home".to_string());
    }
    Ok(())
}

pub(crate) fn refuse_overlapping_restore_cache(
    cache_home: &Path,
    target_home: &Path,
) -> Result<(), String> {
    let cache = cache_home
        .canonicalize()
        .map_err(|_| "restore chunk cache home is unavailable")?;
    let target = if target_home.exists() {
        target_home.canonicalize()
    } else {
        target_home
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .canonicalize()
            .map(|parent| parent.join(target_home.file_name().unwrap_or_default()))
    }
    .map_err(|_| "restore destination parent is unavailable")?;
    if target.starts_with(&cache) || cache.starts_with(&target) {
        return Err("restore chunk cache and destination must be disjoint homes".into());
    }
    Ok(())
}

/// `lastdb connect` creates `data/.device_id` after it reads the phrase.
/// Permit that one file, but reject any LastStore source data or high-water.
pub(crate) fn refuse_recovery_home_with_store_data(home: &Path) -> Result<(), String> {
    let refuse = || {
        "remote recovery home must contain credentials only, with no source database".to_string()
    };
    if home.join("laststore_high_water.json").exists() {
        return Err(refuse());
    }
    let data = home.join("data");
    if !data.exists() {
        return Ok(());
    }
    if !data.is_dir() {
        return Err(refuse());
    }
    for entry in data
        .read_dir()
        .map_err(|error| format!("read recovery home data: {error}"))?
    {
        let entry = entry.map_err(|error| format!("read recovery home data entry: {error}"))?;
        if entry.file_name() != ".device_id"
            || !entry
                .file_type()
                .map_err(|error| format!("read recovery home entry type: {error}"))?
                .is_file()
        {
            return Err(refuse());
        }
    }
    Ok(())
}

pub(crate) fn dest_has_committed_s0(target_home: &Path) -> bool {
    let path = target_home.join("laststore_high_water.json");
    let Ok(raw) = std::fs::read_to_string(&path) else {
        return false;
    };
    let Ok(value) = serde_json::from_str::<serde_json::Value>(&raw) else {
        return false;
    };
    value
        .get("backup_manifest_counter")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0)
        > 0
}

pub(crate) fn refuse_non_fresh_restore_home(target_home: &Path) -> Result<(), String> {
    let data = target_home.join("data");
    if data.exists()
        && data
            .read_dir()
            .map_err(|e| format!("read {}: {e}", data.display()))?
            .next()
            .is_some()
    {
        return Err(format!(
            "refusing to restore into non-empty data dir {}",
            data.display()
        ));
    }
    let high_water = target_home.join("laststore_high_water.json");
    if high_water.exists() {
        return Err(format!(
            "refusing to restore over existing high-water marker {}",
            high_water.display()
        ));
    }
    Ok(())
}

pub(crate) fn refuse_non_fresh_migration_home(target_home: &Path) -> Result<(), String> {
    if !target_home.exists() {
        return Ok(());
    }
    let mut entries = target_home
        .read_dir()
        .map_err(|e| format!("read {}: {e}", target_home.display()))?;
    if entries.next().is_some() {
        return Err(format!(
            "refusing to migrate into non-empty destination home {}",
            target_home.display()
        ));
    }
    Ok(())
}

pub(crate) fn laststore_home_has_chunk_layout(store_root: &Path) -> bool {
    let Ok(collections) = std::fs::read_dir(store_root.join("data")) else {
        return false;
    };
    for collection in collections.filter_map(Result::ok) {
        let Ok(shards) = std::fs::read_dir(collection.path()) else {
            continue;
        };
        for shard in shards.filter_map(Result::ok) {
            let shard_path = shard.path();
            if shard_path.join("tail").is_dir() || shard_path.join("chunks").is_dir() {
                return true;
            }
            let Ok(groups) = std::fs::read_dir(shard_path.join("g")) else {
                continue;
            };
            for group in groups.filter_map(Result::ok) {
                let group_path = group.path();
                if group_path.join("tail").is_dir() || group_path.join("chunks").is_dir() {
                    return true;
                }
            }
        }
    }
    false
}

pub(crate) fn write_owner_only_local(path: &Path, bytes: &[u8]) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| format!("create {}: {e}", parent.display()))?;
    }
    let tmp = path.with_file_name(format!(
        ".{}.tmp-{}",
        path.file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("restore"),
        std::process::id()
    ));
    let _ = std::fs::remove_file(&tmp);
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options
        .open(&tmp)
        .map_err(|e| format!("write {}: {e}", tmp.display()))?;
    file.write_all(bytes)
        .map_err(|e| format!("write {}: {e}", tmp.display()))?;
    file.sync_all()
        .map_err(|e| format!("sync {}: {e}", tmp.display()))?;
    std::fs::rename(&tmp, path).map_err(|e| format!("rename to {}: {e}", path.display()))?;
    Ok(())
}
