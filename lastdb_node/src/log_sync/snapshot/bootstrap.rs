//! Preserve the first personal snapshot intent and resume its signed publication.

use super::{install_base, registry, transport, CloudLog, Manifest, RestoredBase};
use crate::host::Host;
use crate::log_sync::{disk, signals, wire::NativeCut};
use cloud_backup_protocol::{bytes_sha256, canonical_bytes, CloudScope, Digest, Id};
use fold_db::storage::disk_pack::PackRuntime;
use mutation_log::LogError;
use serde::{Deserialize, Serialize};
use std::path::Path;
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

pub(in crate::log_sync) async fn ensure_initial(
    host: &Arc<Host>,
    log: &CloudLog,
    database: &registry::Database,
) -> Result<(), String> {
    if database.native
        || database.kind != "personal"
        || !signals::has_first_backup_intent(&host.home)
    {
        return Ok(());
    }
    let runtime = host.db.db_ops().native_runtime();
    if !runtime.captures_accepted() {
        return Err("local capture disabled until the enrolled node restarts".into());
    }
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
    let path = signals::snapshot_bootstrap_path(home, log.database_id());
    disk::write_private(&path, &bytes)
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
    let Some(bytes) = disk::read_limited(
        &signals::snapshot_bootstrap_path(home, log.database_id()),
        4096,
    )
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

pub(super) fn complete_or_await_upload(
    runtime: &PackRuntime,
    nonce: Option<&str>,
) -> Result<bool, String> {
    if runtime.accepted_status().head > 0 {
        return if nonce.is_some() {
            Err(super::AWAIT_LOCAL_UPLOAD.into())
        } else {
            Ok(false)
        };
    }
    Ok(true)
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
    signals::clear_first_backup_intent(home)
}

pub(super) async fn newer_cut(
    log: &CloudLog,
    manifest: Option<&Manifest>,
) -> Result<Option<NativeCut>, String> {
    if manifest.is_some_and(|saved| saved.name.is_some()) {
        return Ok(None);
    }
    let after = manifest.map_or(0, |saved| saved.through.get());
    match log.list_with_native_reset(after).await {
        Ok(Err(cut)) => Ok(Some(cut)),
        Ok(Ok(_)) | Err(LogError::CursorExpired { .. } | LogError::Unauthorized) => Ok(None),
        Err(error) => Err(error.to_string()),
    }
}

pub(in crate::log_sync) async fn restore_cut(
    home: &Path,
    log: &CloudLog,
    cut: &NativeCut,
) -> Result<RestoredBase, String> {
    let database = Id::try_from(log.database_id().to_owned()).map_err(|error| error.to_string())?;
    let (_, manifest) = transport::for_restore(log, Some(&cut.native_snapshot_id))
        .await?
        .ok_or("the requested signed snapshot is unavailable")?;
    if manifest.through != cut.snapshot_through || manifest.name.is_some() {
        return Err("snapshot does not cover the requested personal database cutoff".into());
    }
    install_base(home, &database, log, &cut.native_snapshot_id, &manifest).await
}
