//! Install the publisher's current snapshot into a received database.
//!
//! The files are the publisher's snapshot, sealed with the database content
//! key. This downloads them with the owner transport, opens the receiver
//! store, and adopts the restored author. The mutation log then replays from
//! the snapshot position. A database with no published snapshot stays empty
//! and replays from position 0.

use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use cloud_backup_protocol::Id;
use fold_db::storage::disk_pack::NODE_MARKER;

use crate::host::Host;
use crate::profile::private_fs;
use crate::registered_share::{self, RegisteredShare};
use crate::share_cache::MANAGED_SHARE_DIRECTORY;

use super::super::cloud::CloudLog;
use super::super::session;
use super::transport;
use super::Manifest;

const ATTEMPTS: u8 = 4;
const INCOMING: &str = "log-incoming";
const PREVIOUS: &str = "log-previous";
const ADOPTED: &str = "snapshot-adopted";
/// Sits beside the snapshot files so a swap cannot separate the position.
const THROUGH: &str = "lastdb-snapshot-through";

#[derive(serde::Serialize, serde::Deserialize)]
struct Placed {
    through: u64,
}

pub(in crate::log_sync) async fn ensure_base(
    host: &Host,
    share: &RegisteredShare,
    log: &CloudLog,
) -> Result<(), String> {
    let dir = parent(host, &share.id)?;
    let live = dir.join("log");
    if has_marker(&live) {
        remove_tree(&dir.join(INCOMING))?;
        remove_tree(&dir.join(PREVIOUS))?;
        return finish(host, &share.id, &dir).await;
    }
    if has_marker(&dir.join(INCOMING)) && read_through(&dir.join(INCOMING))?.is_some() {
        promote(&dir)?;
        return finish(host, &share.id, &dir).await;
    }
    remove_tree(&dir.join(INCOMING))?;
    if stage(log, &dir.join(INCOMING)).await?.is_some() {
        promote(&dir)?;
        finish(host, &share.id, &dir).await
    } else {
        private_fs::write_private(&dir.join(registered_share::SNAPSHOT_ABSENT), b"1")
            .map_err(|error| error.to_string())
    }
}

/// The service dropped the cursor. Download the current snapshot beside the
/// live store, then replace the store and replay from the new position.
pub(in crate::log_sync) async fn restart(
    host: &Host,
    database_id: &str,
    log: &CloudLog,
) -> Result<(), String> {
    let id = Id::try_from(database_id.to_owned()).map_err(|_| "database id was refused")?;
    let dir = parent(host, &id)?;
    let incoming = dir.join(INCOMING);
    remove_tree(&incoming)?;
    if stage(log, &incoming).await?.is_none() {
        return Err("the publisher has no snapshot".into());
    }
    remove_file(&dir.join(ADOPTED))?;
    release_store(host, database_id).await?;
    replace_live(&dir)?;
    finish(host, &id, &dir).await
}

async fn stage(log: &CloudLog, dest: &Path) -> Result<Option<Manifest>, String> {
    let mut attempt = 0u8;
    loop {
        attempt += 1;
        let current = match transport::current(log).await {
            Ok(current) => current,
            Err(error) if superseded(&error) && attempt < ATTEMPTS => continue,
            Err(error) => return Err(error),
        };
        let Some((id, manifest)) = current else {
            return Ok(None);
        };
        remove_tree(dest)?;
        match transport::download(log, &id, &manifest, dest).await {
            Ok(()) => {
                write_through(dest, manifest.through.get())?;
                return Ok(Some(manifest));
            }
            Err(error) if superseded(&error) && attempt < ATTEMPTS => {
                remove_tree(dest)?;
            }
            Err(error) => {
                remove_tree(dest)?;
                return Err(error);
            }
        }
    }
}

async fn finish(host: &Host, id: &Id, dir: &Path) -> Result<(), String> {
    let live = dir.join("log");
    let Some(through) = read_through(&live)? else {
        return Ok(());
    };
    remove_file(&dir.join(registered_share::SNAPSHOT_ABSENT))?;
    if is_file(&dir.join(ADOPTED)) {
        if session::load_cursor(&host.home, id.as_str())? < through {
            session::store_cursor(&host.home, id.as_str(), through)?;
        }
        return Ok(());
    }
    let store = registered_share::receiver_store(host, id)
        .await
        .map_err(|error| error.to_string())?;
    super::super::note_receiver(id.as_str(), &store);
    store
        .db_ops()
        .native_runtime()
        .adopt_restored_snapshot(through == 0)
        .await
        .map_err(|error| error.to_string())?;
    if session::load_cursor(&host.home, id.as_str())? < through {
        session::store_cursor(&host.home, id.as_str(), through)?;
    }
    private_fs::write_private(&dir.join(ADOPTED), b"1").map_err(|error| error.to_string())
}

async fn release_store(host: &Host, database_id: &str) -> Result<(), String> {
    let mut open = host.received.lock().await;
    if let Some(store) = open.remove(database_id) {
        store.shutdown().await.map_err(|error| error.to_string())?;
    }
    Ok(())
}

fn replace_live(dir: &Path) -> Result<(), String> {
    let live = dir.join("log");
    let incoming = dir.join(INCOMING);
    let previous = dir.join(PREVIOUS);
    if read_through(&incoming)?.is_none() {
        return Err("downloaded snapshot is incomplete".into());
    }
    remove_tree(&previous)?;
    if metadata(&live)?.is_some() {
        std::fs::rename(&live, &previous).map_err(|error| error.to_string())?;
    }
    if let Err(error) = std::fs::rename(&incoming, &live) {
        let _ = std::fs::rename(&previous, &live);
        return Err(error.to_string());
    }
    remove_tree(&previous)
}

fn promote(dir: &Path) -> Result<(), String> {
    let live = dir.join("log");
    if has_marker(&live) {
        return Err("received database already has files".into());
    }
    remove_tree(&live)?;
    std::fs::rename(dir.join(INCOMING), live).map_err(|error| error.to_string())
}

fn parent(host: &Host, id: &Id) -> Result<PathBuf, String> {
    let home = host
        .home
        .canonicalize()
        .map_err(|_| "received database directory is missing".to_owned())?;
    let dir = home.join(MANAGED_SHARE_DIRECTORY).join(id.as_str());
    if !is_dir(&dir) {
        return Err("received database directory is missing".into());
    }
    Ok(dir)
}

fn write_through(dest: &Path, through: u64) -> Result<(), String> {
    let bytes = serde_json::to_vec(&Placed { through }).map_err(|error| error.to_string())?;
    private_fs::write_private(&dest.join(THROUGH), &bytes).map_err(|error| error.to_string())
}

fn read_through(dest: &Path) -> Result<Option<u64>, String> {
    match private_fs::read_limited(&dest.join(THROUGH), 256) {
        Ok(None) => Ok(None),
        Ok(Some(bytes)) => serde_json::from_slice::<Placed>(&bytes)
            .map(|placed| Some(placed.through))
            .map_err(|_| "snapshot position is invalid".into()),
        Err(error) => Err(error.to_string()),
    }
}

fn superseded(error: &str) -> bool {
    error.contains("snapshot.current")
}

fn has_marker(path: &Path) -> bool {
    is_file(&path.join(NODE_MARKER))
}

fn is_file(path: &Path) -> bool {
    metadata(path)
        .ok()
        .flatten()
        .is_some_and(|meta| meta.is_file())
}

fn is_dir(path: &Path) -> bool {
    metadata(path)
        .ok()
        .flatten()
        .is_some_and(|meta| meta.is_dir())
}

fn metadata(path: &Path) -> Result<Option<std::fs::Metadata>, String> {
    match std::fs::symlink_metadata(path) {
        Ok(meta) if meta.file_type().is_symlink() => Err("snapshot path is a link".into()),
        Ok(meta) => Ok(Some(meta)),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.to_string()),
    }
}

fn remove_tree(path: &Path) -> Result<(), String> {
    if metadata(path)?.is_none() {
        return Ok(());
    }
    std::fs::remove_dir_all(path).map_err(|error| error.to_string())
}

fn remove_file(path: &Path) -> Result<(), String> {
    match metadata(path)? {
        None => Ok(()),
        Some(_) => std::fs::remove_file(path).map_err(|error| error.to_string()),
    }
}
