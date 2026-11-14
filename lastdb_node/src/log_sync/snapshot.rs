//! Publish a held database checkpoint through the signed cloud database.

mod files;
mod transport;

use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use cloud_backup_protocol::{bytes_sha256, canonical_bytes, CloudScope, DecimalU64, Digest, Id};
use fold_db::storage::disk_pack::NativeCloudWorkGuard;
use serde::{Deserialize, Serialize};

use crate::host::owned;
use crate::host::owned::Resolved;
use crate::host::Host;
use fold_db::security::Ed25519KeyPair;

use super::{cloud::CloudLog, registry, session};

pub(super) const AWAIT_INCOMING: &str = "snapshot awaits incoming signed updates";
pub(super) const AWAIT_LOCAL_UPLOAD: &str = "snapshot awaits publication of local signed writes";

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    through: DecimalU64,
    name: Option<String>,
    upload_id: Id,
    files: Vec<files::FileEntry>,
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

/// Publish snapshots for a database already selected by its stable id.
async fn publish(
    host: &Arc<Host>,
    log: &CloudLog,
    database: &registry::Database,
    nonce: &str,
) -> Result<(), String> {
    let _work = NativeCloudWorkGuard::acquire().map_err(|error| error.to_string())?;
    let (resolved, sibling) = host
        .owned
        .registered_store(log.database_id())
        .map_err(|error| error.status().1)?;
    let store = match (resolved, sibling) {
        (Resolved::Owner, None) => Arc::clone(&host.db),
        (Resolved::Owned, Some(store)) => store,
        _ => return Err("snapshot database is unavailable locally".into()),
    };
    let runtime = store.db_ops().native_runtime();
    let through = session::load_cursor(&host.home, log.database_id())?;
    let cloud_head = super::owner_recovery::head(log).await?;
    let previous = transport::current(log).await?;
    let personal = database.kind == "personal";
    if personal
        && through == 0
        && cloud_head == 0
        && runtime.accepted_status().snapshot_required
        && finish_bootstrap(&runtime, &host.home, log, previous.as_ref())?
    {
        return remember(&host.home, log.database_id(), 0, 0, nonce);
    }
    if cloud_head != through {
        return Err(AWAIT_INCOMING.into());
    }
    let cut = runtime
        .capture_synced_snapshot()
        .await
        .map_err(|error| format!("cannot freeze the database files: {error}"))?
        .ok_or(AWAIT_LOCAL_UPLOAD)?;
    let bootstrap = personal
        && through == 0
        && previous.is_none()
        && (cut.needs_initial_coverage || first_path(&host.home, log.database_id()).is_file());
    if cut.needs_initial_coverage && !bootstrap {
        return Err("initial snapshot requires a new personal cloud database".into());
    }
    let (cut, files) = tokio::task::spawn_blocking(move || {
        let files = files::inventory(&cut).map_err(|_| "cannot read the frozen database files")?;
        Ok::<_, String>((cut, files))
    })
    .await
    .map_err(|_| "snapshot inventory stopped")??;
    let manifest = Manifest {
        through: DecimalU64::new(through),
        name: if database.native {
            Some(
                database
                    .name
                    .clone()
                    .ok_or("snapshot database has no local name")?,
            )
        } else {
            None
        },
        upload_id: Id::try_from(uuid::Uuid::new_v4().to_string())
            .map_err(|error| error.to_string())?,
        files,
    };
    if bootstrap {
        start_bootstrap(&host.home, log, &manifest, cut.native_frontier)?;
    }
    let id = match previous.as_ref() {
        Some((id, saved))
            if manifest.through == saved.through
                && manifest.name == saved.name
                && manifest.files == saved.files =>
        {
            id.clone()
        }
        _ => transport::publish(log, previous.as_ref().map(|(id, _)| id), &cut, &manifest).await?,
    };
    let entries = confirmed(log, through, &id, personal).await?;
    if bootstrap {
        runtime
            .note_accepted_snapshot(cut.native_frontier)
            .map_err(|error| format!("{error:?}"))?;
    }
    remember(&host.home, log.database_id(), through, entries, nonce)?;
    tracing::info!(target: "lastdbd::log_sync", database_id = %database.id, snapshot = %id.as_str(), "snapshot published");
    Ok(())
}

fn start_bootstrap(
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
    super::disk::write_private(&path, &bytes)
        .and_then(|()| File::open(parent)?.sync_all())
        .map_err(|_| "cannot record the initial snapshot before publishing".into())
}

fn finish_bootstrap(
    runtime: &fold_db::storage::disk_pack::PackRuntime,
    home: &Path,
    log: &CloudLog,
    cloud: Option<&(Digest, Manifest)>,
) -> Result<bool, String> {
    let Some((id, _)) = cloud.filter(|(_, manifest)| manifest.through.get() == 0) else {
        return Ok(false);
    };
    let Some(bytes) = super::disk::read_limited(&bootstrap_path(home, log.database_id()), 4096)
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
    runtime
        .note_accepted_snapshot(saved.native_frontier)
        .map_err(|error| format!("{error:?}"))?;
    Ok(true)
}

async fn confirmed(
    log: &CloudLog,
    through: u64,
    id: &Digest,
    personal: bool,
) -> Result<u64, String> {
    let (saved_id, manifest) = transport::current(log)
        .await?
        .ok_or("signed snapshot read is empty")?;
    let head = super::owner_recovery::head(log).await?;
    if &saved_id != id || manifest.through.get() != through || head != through {
        return Err("signed snapshot read is not the log head".into());
    }
    // Every personal delivery number counts one accepted edit, even if its
    // body later moves out of the live log. Do not page from position zero.
    if personal {
        return Ok(through.min(LOG_ENTRY_REPORT_CAP));
    }
    super::owner_recovery::count_entries(log, LOG_ENTRY_REPORT_CAP).await
}

fn remember(
    home: &Path,
    database: &str,
    through: u64,
    entries: u64,
    nonce: &str,
) -> Result<(), String> {
    let report =
        format!("through={through}\nhead={through}\nlog_entries={entries}\nrequest={nonce}\n");
    super::disk::write_private(&result_path(home, database), report.as_bytes())
        .map_err(|_| "cannot record the signed snapshot read")?;
    Ok(())
}

/// Queue a direct cloud snapshot of one owned database.
pub fn request(home: &std::path::Path, selector: &str) -> Result<(String, String), String> {
    let databases = registry::databases(home);
    let selected = databases
        .iter()
        .find(|db| db.id == selector || db.name.as_deref() == Some(selector))
        .ok_or("database is not owned by this node")?;
    Id::try_from(selected.id.clone()).map_err(|e| e.to_string())?;
    let nonce = uuid::Uuid::new_v4().simple().to_string();
    let body = format!("snapshot-v1\n{nonce}\n");
    std::fs::remove_file(result_path(home, &selected.id)).ok();
    super::disk::write_private(&request_path(home, &selected.id), body.as_bytes())
        .map_err(|_| "cannot save the snapshot request")?;
    Ok((selected.id.clone(), nonce))
}

pub(super) fn pending(home: &Path, database: &str) -> bool {
    request_path(home, database).exists()
}

pub(super) async fn requested(
    host: &Arc<Host>,
    log: &CloudLog,
    database: &registry::Database,
) -> Result<(), String> {
    let path = request_path(&host.home, log.database_id());
    let request = request_bytes(host, log.database_id())?;
    let Some(request) = request else {
        return Ok(());
    };
    let nonce = request_nonce(&request)?;
    let failure = path.with_file_name("snapshot-error");
    if let Err(error) = publish(host, log, database, &nonce).await {
        super::disk::write_private(&failure, error.as_bytes())
            .map_err(|_| "cannot save the snapshot failure")?;
        return Err(error);
    }
    match std::fs::remove_file(failure) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(_) => return Err("cannot clear the previous snapshot failure".into()),
    }
    if request_bytes(host, log.database_id())?.as_deref() == Some(request.as_slice()) {
        std::fs::remove_file(path).map_err(|_| "cannot retire the completed snapshot request")?;
    }
    Ok(())
}

fn request_bytes(host: &Host, database: &str) -> Result<Option<Vec<u8>>, String> {
    super::disk::read_limited(&request_path(&host.home, database), 64)
        .map_err(|_| "cannot read snapshot request".into())
}

fn request_nonce(bytes: &[u8]) -> Result<String, String> {
    let text =
        std::str::from_utf8(bytes).map_err(|_| "snapshot request has an unsupported version")?;
    let mut lines = text.lines();
    if lines.next() != Some("snapshot-v1") {
        return Err("snapshot request has an unsupported version".into());
    }
    match lines.next() {
        Some(nonce)
            if nonce.len() == 32
                && nonce.bytes().all(|byte| byte.is_ascii_hexdigit())
                && lines.next().is_none() =>
        {
            Ok(nonce.to_owned())
        }
        _ => Err("snapshot request has an unsupported version".into()),
    }
}

const LOG_ENTRY_REPORT_CAP: u64 = 9;

fn request_path(home: &std::path::Path, id: &str) -> std::path::PathBuf {
    home.join("log-sync").join(id).join("snapshot-request")
}

fn result_path(home: &Path, id: &str) -> std::path::PathBuf {
    home.join("log-sync").join(id).join("snapshot-result")
}

fn first_path(home: &Path, id: &str) -> std::path::PathBuf {
    home.join("log-sync").join(id).join("snapshot-first")
}

fn bootstrap_path(home: &Path, id: &str) -> PathBuf {
    home.join("log-sync")
        .join(id)
        .join("snapshot-bootstrap.json")
}

/// Restore the snapshot before booting the isolated destination.
pub async fn restore_base(
    home: &Path,
    source: &Path,
    scope: &cloud_backup_protocol::CloudScope,
    database: &Id,
    seed: &[u8; 32],
) -> Result<Option<RestoredBase>, String> {
    let probe =
        Arc::new(Ed25519KeyPair::from_secret_key(seed).map_err(|_| "invalid recovery key")?);
    let selected = super::http::selected_service(source)?;
    let endpoint = super::http::Endpoint::open(&selected.api_url, selected.label)?;
    let device = session::device_id(home)?;
    let mut log = CloudLog::open_offline(
        endpoint,
        scope.clone(),
        database.clone(),
        &device,
        probe,
        selected.label,
    )?;
    if let Some(revision) = super::members::revision(&log).await {
        log.member_revision = revision;
    }
    let Some((id, manifest)) = transport::current(&log).await? else {
        return Ok(None);
    };
    let name = manifest.name.as_deref();
    let data = data_path(home, database, name.is_some());
    if let Some(parent) = data.parent() {
        std::fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    }
    transport::download(&log, &id, &manifest, &data).await?;
    owned::prepare_snapshot_restore(home, database.as_str().to_owned(), name)?;
    Ok(Some(RestoredBase {
        through: manifest.through.get(),
        name: name.unwrap_or("personal").to_owned(),
        member_revision: log.member_revision,
    }))
}

fn data_path(home: &Path, database: &Id, named: bool) -> PathBuf {
    if named {
        home.join("databases").join(database.as_str()).join("data")
    } else {
        home.join("data")
    }
}

/// A cloud-delivery position whose database files were installed locally.
pub struct RestoredBase {
    /// Last log position already included in the restored files.
    pub through: u64,
    /// Local database name to use after installing the files.
    pub name: String,
    pub(super) member_revision: u64,
}
