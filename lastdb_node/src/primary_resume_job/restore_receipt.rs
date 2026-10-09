use super::Receipt;
use serde::Deserialize;
use std::fs;
use std::path::Path;

const READY_FILE: &str = ".normal_latest_restore_ready";
const DEGRADED_CHECK_FILE: &str = ".fresh_backup_degraded_check.json";
const MAX_READY_BYTES: u64 = 4096;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RestoreReady {
    version: u32,
    ok: bool,
    db_hash: String,
    store_uuid: String,
    manifest_sha256: String,
    counter: u64,
    restore_mode: String,
    cloud_sync_off: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DegradedCheck {
    version: u32,
    manifest_sha256: String,
    source_missing_atom_groups: u64,
    owner_accepted_local_damage: bool,
    healthy_key_reads: u64,
    healthy_keys_match: bool,
}

pub(super) fn require_fresh_restore(
    primary_home: &Path,
    restore_home: &str,
    receipt: &Receipt,
) -> Result<(), String> {
    let cut = receipt.cut.as_ref().ok_or("fresh backup cut is absent")?;
    if !receipt.fresh_from_local || cut.manifest.previous_latest.is_some() {
        return Err("fresh backup receipt has the wrong mode".into());
    }
    let target =
        fs::canonicalize(restore_home).map_err(|error| format!("open restore home: {error}"))?;
    let primary =
        fs::canonicalize(primary_home).map_err(|error| format!("open primary home: {error}"))?;
    if target.starts_with(&primary) || primary.starts_with(&target) || !target.join("data").is_dir()
    {
        return Err("source-free restore requires a separate complete home".into());
    }
    let path = target.join(READY_FILE);
    let meta = fs::symlink_metadata(&path)
        .map_err(|error| format!("read source-free restore receipt metadata: {error}"))?;
    if !meta.file_type().is_file() || meta.len() > MAX_READY_BYTES {
        return Err("source-free restore receipt is not a small regular file".into());
    }
    let ready: RestoreReady = serde_json::from_slice(
        &fs::read(&path).map_err(|error| format!("read source-free restore receipt: {error}"))?,
    )
    .map_err(|error| format!("decode source-free restore receipt: {error}"))?;
    if ready.version != 1
        || !ready.ok
        || !ready.cloud_sync_off
        || ready.restore_mode != "replay_tail"
        || ready.store_uuid != cut.manifest.store_uuid
        || ready.db_hash
            != fold_db::storage::laststore::cloud_db_hash_for_store_uuid(&cut.manifest.store_uuid)
        || ready.counter != cut.manifest.counter
        || !ready
            .manifest_sha256
            .eq_ignore_ascii_case(&cut.manifest.manifest_sha256)
    {
        return Err("source-free restore receipt does not match the fresh backup".into());
    }
    if receipt.accept_local_damage {
        require_owner_degraded_check(&target, receipt)?;
    }
    Ok(())
}

fn require_owner_degraded_check(target: &Path, receipt: &Receipt) -> Result<(), String> {
    let path = target.join(DEGRADED_CHECK_FILE);
    let meta = fs::symlink_metadata(&path)
        .map_err(|error| format!("read owner damage check metadata: {error}"))?;
    if !meta.file_type().is_file() || meta.len() > MAX_READY_BYTES {
        return Err("owner damage check is not a small regular file".into());
    }
    let check: DegradedCheck = serde_json::from_slice(
        &fs::read(&path).map_err(|error| format!("read owner damage check: {error}"))?,
    )
    .map_err(|error| format!("decode owner damage check: {error}"))?;
    let cut = receipt.cut.as_ref().ok_or("fresh backup cut is absent")?;
    if check.version != 2
        || !check
            .manifest_sha256
            .eq_ignore_ascii_case(&cut.manifest.manifest_sha256)
        || check.source_missing_atom_groups != cut.manifest.local_missing_atom_groups
        || !check.owner_accepted_local_damage
        || check.healthy_key_reads == 0
        || !check.healthy_keys_match
    {
        return Err("owner damage check does not match the restored backup".into());
    }
    Ok(())
}
