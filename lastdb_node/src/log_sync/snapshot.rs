//! Publish a held database checkpoint through the signed cloud database.

mod files;
mod transport;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use cloud_backup_protocol::{DecimalU64, Id};
use fold_db::storage::disk_pack::NativeCloudWorkGuard;
use serde::{Deserialize, Serialize};

use crate::host::owned;
use crate::host::owned::Resolved;
use crate::host::Host;
use fold_db::security::Ed25519KeyPair;

use super::{cloud::CloudLog, registry, session};

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    through: DecimalU64,
    name: Option<String>,
    upload_id: Id,
    files: Vec<files::FileEntry>,
}

/// Publish snapshots for a database already selected by its stable id.
async fn publish(
    host: &Arc<Host>,
    log: &CloudLog,
    database: &registry::Database,
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
    let status = runtime.accepted_status();
    if !runtime.captures_accepted() || status.gap {
        return Err("snapshot needs intact local write capture".into());
    }
    let through = session::load_cursor(&host.home, log.database_id())?;
    if super::owner_recovery::head(log).await? != through {
        return Err("snapshot awaits incoming signed updates".into());
    }
    let previous = transport::current(log).await?;
    let cut = runtime
        .capture_database_snapshot()
        .await
        .map_err(|_| "cannot freeze the database files")?;
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
    if database.kind == "personal" {
        runtime
            .note_accepted_snapshot(cut.native_frontier)
            .map_err(|error| format!("{error:?}"))?;
        runtime
            .acknowledge_accepted(cut.accepted_head)
            .map_err(|error| format!("{error:?}"))?;
    }
    tracing::info!(target: "lastdbd::log_sync", database_id = %database.id, snapshot = %id.as_str(), "snapshot published");
    Ok(())
}

/// Queue a direct cloud snapshot of one owned database.
pub fn request(home: &std::path::Path, selector: &str) -> Result<String, String> {
    let databases = registry::databases(home);
    let selected = databases
        .iter()
        .find(|db| db.id == selector || db.name.as_deref() == Some(selector))
        .ok_or("database is not owned by this node")?;
    Id::try_from(selected.id.clone()).map_err(|e| e.to_string())?;
    super::disk::write_private(&request_path(home, &selected.id), b"snapshot-v1\n")
        .map_err(|_| "cannot save the snapshot request")?;
    Ok(selected.id.clone())
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
    let request =
        super::disk::read_limited(&path, 32).map_err(|_| "cannot read snapshot request")?;
    let Some(request) = request else {
        return Ok(());
    };
    if request != b"snapshot-v1\n" {
        return Err("snapshot request has an unsupported version".into());
    }
    if let Err(error) = publish(host, log, database).await {
        super::disk::write_private(&path.with_file_name("snapshot-error"), error.as_bytes())
            .map_err(|_| "cannot save the snapshot failure")?;
        return Err(error);
    }
    match std::fs::remove_file(path.with_file_name("snapshot-error")) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(_) => return Err("cannot clear the previous snapshot failure".into()),
    }
    std::fs::remove_file(path).map_err(|_| "cannot retire the completed snapshot request")?;
    Ok(())
}

fn request_path(home: &std::path::Path, id: &str) -> std::path::PathBuf {
    home.join("log-sync").join(id).join("snapshot-request")
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
