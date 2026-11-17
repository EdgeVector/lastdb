//! Snapshot file inventory, layout installation, and initial personal snapshot bootstrap.

use std::collections::BTreeSet;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek};
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::path::Path;
use std::sync::Arc;

use cloud_backup_protocol::v2::Destination;
use cloud_backup_protocol::{
    bytes_sha256, canonical_bytes, CloudScope, DecimalU64, Digest, Id, RelativePath,
};
use fold_db::storage::disk_pack::{
    snapshot::DatabaseSnapshot, PackRuntime, CAS_DIR, NODE_MARKER, RUN_DIR,
};
use mutation_log::LogError;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

use super::{install_base, transport, CloudLog, Database, Manifest, RestoredBase};
use crate::host::Host;
use crate::log_sync::{disk, session as signals, wire::NativeCut};

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct FileEntry {
    pub path: RelativePath,
    pub destination: Destination,
    pub bytes: DecimalU64,
    pub sha256: Digest,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Bootstrap {
    snapshot_id: Digest,
    scope: CloudScope,
    environment: String,
    device: String,
    native_frontier: u64,
}

pub(super) fn inventory(cut: &DatabaseSnapshot) -> Result<Vec<FileEntry>, String> {
    let files = cut.files().map_err(|error| error.to_string())?;
    let mut entries = Vec::with_capacity(files.len());
    for (path, len) in files {
        let name = path.to_str().ok_or("snapshot path is not UTF-8")?;
        let destination = provider(name)?;
        let mut file = cut.open_file(&path).map_err(|error| error.to_string())?;
        let mut hash = Sha256::new();
        let bytes = std::io::copy(&mut (&mut file).take(len + 1), &mut hash)
            .map_err(|error| error.to_string())?;
        file.rewind().map_err(|error| error.to_string())?;
        if bytes != len {
            return Err("snapshot source file length changed".into());
        }
        entries.push(FileEntry {
            path: name
                .to_owned()
                .try_into()
                .map_err(|_| "invalid snapshot path")?,
            destination,
            bytes: DecimalU64::new(bytes),
            sha256: digest(hash)?,
        });
    }
    validate(&entries)?;
    Ok(entries)
}

pub(super) fn digest(hash: Sha256) -> Result<Digest, String> {
    Digest::try_from(fold_db::hex::hex_lower(hash.finalize())).map_err(|error| error.to_string())
}

pub(super) fn validate(files: &[FileEntry]) -> Result<(), String> {
    let mut paths = BTreeSet::new();
    for entry in files {
        if provider(entry.path.as_str())? != entry.destination || !paths.insert(entry.path.as_str())
        {
            return Err("invalid or duplicate snapshot file".into());
        }
    }
    if !paths.contains(NODE_MARKER)
        || [RUN_DIR, CAS_DIR].iter().any(|root| {
            ["CURRENT", "laststore-run-home-v2"]
                .iter()
                .any(|file| !paths.contains(format!("{root}/{file}").as_str()))
        })
    {
        return Err("snapshot is missing native database metadata".into());
    }
    Ok(())
}

fn provider(path: &str) -> Result<Destination, String> {
    if path == NODE_MARKER {
        return Ok(Destination::R2);
    }
    let (root, name) = path.split_once('/').ok_or("invalid snapshot path")?;
    let allowed = matches!(name, "CURRENT" | "laststore-run-home-v2")
        || name
            .strip_prefix("runs/")
            .and_then(|run| run.strip_suffix(".ldrun"))
            .is_some_and(|id| uuid::Uuid::parse_str(id).is_ok_and(|uuid| uuid.to_string() == id));
    if !allowed {
        return Err("unexpected snapshot file".into());
    }
    match root {
        RUN_DIR => Ok(Destination::R2),
        CAS_DIR => Ok(Destination::B2),
        _ => Err("unexpected snapshot directory".into()),
    }
}

pub(super) fn create_layout(destination: &Path) -> std::io::Result<()> {
    fs::DirBuilder::new().mode(0o700).create(destination)?;
    for name in [RUN_DIR, CAS_DIR] {
        let home = destination.join(name);
        fs::DirBuilder::new().mode(0o700).create(&home)?;
        fs::DirBuilder::new()
            .mode(0o700)
            .create(home.join("runs"))?;
        create_file(&home.join("LOCK"))?.sync_all()?;
    }
    Ok(())
}

pub(super) fn create_file(path: &Path) -> std::io::Result<File> {
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
}

pub(super) fn sync_layout(destination: &Path) -> std::io::Result<()> {
    for name in [RUN_DIR, CAS_DIR] {
        File::open(destination.join(name).join("runs"))?.sync_all()?;
        File::open(destination.join(name))?.sync_all()?;
    }
    File::open(destination)?.sync_all()
}

pub(in crate::log_sync) async fn ensure_initial(
    host: &Arc<Host>,
    log: &CloudLog,
    database: &Database,
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
