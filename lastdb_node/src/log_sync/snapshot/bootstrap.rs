//! Preserve the first personal snapshot intent and resume its signed publication.

use super::{registry, CloudLog, Manifest};
use crate::cloud::{FIRST_BACKUP_INTENT, FIRST_BACKUP_INTENT_FILE};
use crate::host::Host;
use crate::log_sync::disk;
use cloud_backup_protocol::{bytes_sha256, canonical_bytes, CloudScope, Digest};
use fold_db::storage::disk_pack::PackRuntime;
use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Bootstrap {
    snapshot_id: Digest,
    scope: CloudScope,
    environment: String,
    device: String,
    native_frontier: u64,
}

pub(crate) fn should_stage_initial_snapshot(home: &Path) -> bool {
    if explicit_intent(home) {
        return true;
    }
    registry::databases(home)
        .first()
        .is_some_and(|owner| owner.kind == "personal" && first_path(home, &owner.id).is_file())
}

fn explicit_intent(home: &Path) -> bool {
    disk::read_limited(
        &home.join(FIRST_BACKUP_INTENT_FILE),
        FIRST_BACKUP_INTENT.len(),
    )
    .is_ok_and(|bytes| bytes.as_deref() == Some(FIRST_BACKUP_INTENT))
}

pub(super) fn first_path(home: &Path, id: &str) -> PathBuf {
    home.join("log-sync").join(id).join("snapshot-first")
}

pub(in crate::log_sync) async fn ensure_initial(
    host: &Arc<Host>,
    log: &CloudLog,
    database: &registry::Database,
) -> Result<(), String> {
    if database.native || database.kind != "personal" || !explicit_intent(&host.home) {
        return Ok(());
    }
    let runtime = host.db.db_ops().native_runtime();
    if !runtime.accepted_status().snapshot_required {
        discard(&runtime, &host.home);
        return Ok(());
    }
    super::publish(host, log, database, None).await
}

pub(super) fn start(
    home: &Path,
    log: &CloudLog,
    manifest: &Manifest,
    native_frontier: u64,
) -> Result<(), String> {
    let snapshot_id = bytes_sha256(&canonical_bytes(manifest).map_err(|error| error.to_string())?);
    let saved = Bootstrap {
        snapshot_id,
        scope: log.scope.clone(),
        environment: log.environment().to_owned(),
        device: log.device().as_str().to_owned(),
        native_frontier,
    };
    let bytes = canonical_bytes(&saved).map_err(|error| error.to_string())?;
    let path = bootstrap_path(home, log.database_id());
    let parent = path
        .parent()
        .ok_or("cannot locate the initial snapshot record")?;
    disk::write_private(&path, &bytes)
        .and_then(|()| File::open(parent)?.sync_all())
        .map_err(|_| "cannot record the initial snapshot before publishing".into())
}

pub(super) fn resume(
    runtime: &PackRuntime,
    home: &Path,
    log: &CloudLog,
    cloud: Option<&(Digest, Manifest)>,
) -> Result<bool, String> {
    let Some((id, _)) =
        cloud.filter(|(_, manifest)| manifest.through.get() == 0 && manifest.name.is_none())
    else {
        return Ok(false);
    };
    let Some(bytes) = disk::read_limited(&bootstrap_path(home, log.database_id()), 4096)
        .map_err(|_| "cannot read the initial snapshot record")?
    else {
        return Ok(false);
    };
    let saved: Bootstrap =
        serde_json::from_slice(&bytes).map_err(|_| "cannot read the initial snapshot record")?;
    if &saved.snapshot_id != id
        || saved.scope != log.scope
        || saved.environment != log.environment()
        || saved.device != log.device().as_str()
    {
        return Ok(false);
    }
    finish(runtime, home, saved.native_frontier)?;
    Ok(true)
}

pub(super) fn finish(runtime: &PackRuntime, home: &Path, frontier: u64) -> Result<(), String> {
    runtime
        .note_accepted_snapshot(frontier)
        .map_err(|error| format!("{error:?}"))?;
    discard(runtime, home);
    Ok(())
}

fn discard(runtime: &PackRuntime, home: &Path) {
    if let Err(error) = release(runtime, home) {
        tracing::warn!(%error, "could not retire the confirmed initial snapshot");
    }
}

fn release(runtime: &PackRuntime, home: &Path) -> Result<(), String> {
    runtime
        .release_initial_snapshot()
        .map_err(|error| error.to_string())?;
    match fs::remove_file(home.join(FIRST_BACKUP_INTENT_FILE)) {
        Ok(()) => File::open(home)
            .and_then(|file| file.sync_all())
            .map_err(|err| err.to_string()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.to_string()),
    }
}

fn bootstrap_path(home: &Path, id: &str) -> PathBuf {
    home.join("log-sync")
        .join(id)
        .join("snapshot-bootstrap.json")
}
