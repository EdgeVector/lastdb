//! Publish a held database checkpoint through the signed cloud database.

mod archive;
mod transport;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use cloud_backup_protocol::v2::blobs::BlobPath;
use cloud_backup_protocol::{DecimalU64, Digest, Id};
use fold_db::storage::disk_pack::NativeCloudWorkGuard;
use serde::{Deserialize, Serialize};

use crate::host::owned;
use crate::host::Host;
use crate::log_envelope::DatabaseContentKey;
use fold_db::security::Ed25519KeyPair;

use super::{cloud::CloudLog, registry, session};

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    through: DecimalU64,
    bytes: DecimalU64,
    sha256: Digest,
    path: BlobPath,
}

/// Publish snapshots for a database already selected by its stable id.
async fn publish(host: &Arc<Host>, log: &CloudLog) -> Result<(), String> {
    let _work = NativeCloudWorkGuard::acquire().map_err(|e| e.to_string())?;
    let (store, key) = DatabaseContentKey::local_store(host, log.database_id())
        .map_err(|e| e.to_string())?
        .ok_or("snapshot database is unavailable locally")?;
    let runtime = Arc::clone(store.db_ops().native_runtime());
    let status = runtime.accepted_status();
    if !runtime.captures_accepted() || status.gap {
        return Err("snapshot needs an intact local write capture".into());
    }
    let through = session::load_cursor(&host.home, log.database_id())?;
    let previous = transport::current(log).await?;
    let cut = runtime
        .capture_database_snapshot()
        .await
        .map_err(|e| e.to_string())?;
    let head = cut.accepted_head;
    let frontier = cut.native_frontier;
    let bytes = tokio::task::spawn_blocking(move || archive::encode(&cut, &key, through))
        .await
        .map_err(|_| "snapshot encoder stopped")??;
    let manifest = transport::publish(log, previous.as_ref(), through, bytes).await?;
    if manifest.through.get() != through {
        return Err("cloud returned a different snapshot position".into());
    }
    runtime
        .note_accepted_snapshot(frontier)
        .map_err(|e| format!("{e:?}"))?;
    runtime
        .acknowledge_accepted(head)
        .map_err(|e| format!("{e:?}"))?;
    Ok(())
}

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

pub(super) async fn requested(host: &Arc<Host>, log: &CloudLog) -> Result<(), String> {
    let path = request_path(&host.home, log.database_id());
    let request =
        super::disk::read_limited(&path, 32).map_err(|_| "cannot read snapshot request")?;
    let Some(request) = request else {
        return Ok(());
    };
    if request != b"snapshot-v1\n" {
        return Err("snapshot request has an unsupported version".into());
    }
    publish(host, log).await?;
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
    name: &str,
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
    let Some(manifest) = transport::current(&log).await? else {
        return Ok(None);
    };
    let payload = transport::download(&log, &manifest).await?;
    let through = manifest.through.get();
    let personal =
        DatabaseContentKey::personal(seed, database.as_str()).map_err(|e| e.to_string())?;
    let (key, name) = if archive::opens(&payload, &personal, through) {
        (personal, None)
    } else {
        (
            DatabaseContentKey::owned(seed, database.as_str()).map_err(|e| e.to_string())?,
            Some(name),
        )
    };
    let data = data_path(home, database, name.is_some());
    if let Some(parent) = data.parent() {
        std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    }
    archive::decode(&payload, &key, through, &data)?;
    owned::prepare_snapshot_restore(home, database.as_str().to_owned(), name)?;
    Ok(Some(RestoredBase {
        through,
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
    pub through: u64,
    pub name: String,
    pub(super) member_revision: u64,
}
