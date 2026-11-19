//! Replay signed mutation-log entries into their registered stores.
//!
//! Resolve the registered store by database id. A received log page calls
//! [`replay_delivered`], flushes the receiver store, and returns only after
//! that flush so the caller can save its position.
//!
//! Each entry is a `log_envelope` around one accepted body. That body is a
//! schema put, a schema drop, a record, a file, an inline file, or a
//! contribution. An external file is the uploaded envelope named by
//! `file.object`. Replay opens that envelope with the same binding, checks
//! the marker, and seals it with `seal_record_file`. A marker that requires
//! bytes and has no object is refused. Schema put and drop use the opened
//! store's schema manager and keep the captured schema id. The content key
//! comes from the selected local store or the stored registration.

mod accepted_record;
mod apply;
mod chain;
mod journal;
mod owned;
mod page;
mod payload;
mod types;

pub(crate) use payload::{AcceptedBody, FileObject, FileSection};
pub(crate) use types::{IncorporatedFrom, RestoreError, SnapshotEntry, SnapshotFileRequest};

pub(crate) use owned::replay_owned;
pub(crate) use page::DeliveredEntry;

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::sync::Arc;

use cloud_backup_protocol::Id;
use fold_db::fold_db_core::FoldDB;

use crate::host::Host;
use crate::log_envelope::DatabaseContentKey;

/// Apply one signed log page into its registered store.
///
/// The page is durable when this returns.
pub(crate) async fn replay_delivered(
    host: &Host,
    database_id: &str,
    publisher: &str,
    delivered: &[DeliveredEntry],
    files: &mut crate::log_sync::SnapshotFiles,
) -> Result<(), RestoreError> {
    page::replay(host, database_id, publisher, delivered, files).await
}

struct RestoreTarget {
    key: DatabaseContentKey,
    place: Place,
    private: bool,
}

impl RestoreTarget {
    fn decoder<'a>(&'a self, database_id: &'a str) -> chain::EntryDecoder<'a> {
        chain::EntryDecoder {
            key: &self.key,
            database_id,
            private: self.private,
        }
    }
}

enum Place {
    Owned(Arc<FoldDB>),
    Received,
}

fn resolve_target(host: &Host, database_id: &str) -> Result<RestoreTarget, RestoreError> {
    require_id(database_id)?;
    match DatabaseContentKey::local_store(host, database_id)
        .map_err(|_| RestoreError::Scope("database registry"))?
    {
        Some((store, key)) => Ok(RestoreTarget {
            key,
            place: Place::Owned(store),
            private: host
                .owned
                .is_private_id(database_id)
                .map_err(|_| RestoreError::Scope("database registry"))?,
        }),
        None => Ok(RestoreTarget {
            key: received_key(host, database_id)?,
            place: Place::Received,
            private: false,
        }),
    }
}

/// True when sync has stored `log-sync/<id>/prefix.json` after a durable pull
/// or native recovery, including an empty log with no snapshot.
pub(crate) fn prefix_durable(home: &std::path::Path, database_id: &str) -> bool {
    if database_id.is_empty() || database_id.contains(['/', '\\']) {
        return false;
    }
    let path = home.join("log-sync").join(database_id).join("prefix.json");
    let Ok(metadata) = fs::symlink_metadata(&path) else {
        return false;
    };
    if !metadata.is_file() || metadata.permissions().mode() & 0o022 != 0 || metadata.len() > 4096 {
        return false;
    }
    let Ok(bytes) = fs::read(&path) else {
        return false;
    };
    serde_json::from_slice::<LogPrefix>(&bytes)
        .ok()
        .is_some_and(|marker| marker.database_id == database_id)
}

#[derive(serde::Deserialize)]
struct LogPrefix {
    database_id: String,
}

fn require_id(database_id: &str) -> Result<(), RestoreError> {
    let id =
        Id::try_from(database_id.to_owned()).map_err(|_| RestoreError::Scope("database id"))?;
    if id.as_str() == database_id {
        Ok(())
    } else {
        Err(RestoreError::Scope("database id"))
    }
}

/// Load the content key stored for this received database.
///
/// An invitation registration opens the sealed invitation. A link registration
/// uses the stored key. A publication registration has no signed log, so it
/// does not get this store.
fn received_key(host: &Host, database_id: &str) -> Result<DatabaseContentKey, RestoreError> {
    let id =
        Id::try_from(database_id.to_owned()).map_err(|_| RestoreError::Scope("database id"))?;
    let share = crate::registered_share::resolve(&host.home, database_id)
        .map_err(|_| RestoreError::Scope("database registry"))?
        .ok_or(RestoreError::Scope("named database"))?;
    if share.id.as_str() != database_id || share.stream().stream_id.as_str() != database_id {
        return Err(RestoreError::Scope("database id"));
    }
    if !share.signed_log {
        return Err(RestoreError::Scope("database store"));
    }
    let material = crate::registered_share::content_material(&host.home, &id)
        .map_err(|_| RestoreError::Scope("named database"))?;
    let key = DatabaseContentKey::from_stored(database_id, &material)
        .map_err(|_| RestoreError::Scope("database store"))?;
    if key.database_id() != database_id {
        return Err(RestoreError::Scope("database id"));
    }
    Ok(key)
}

async fn open_store(
    host: &Host,
    database_id: &str,
    target: &RestoreTarget,
) -> Result<Arc<FoldDB>, RestoreError> {
    match &target.place {
        Place::Owned(store) => Ok(Arc::clone(store)),
        Place::Received => received_store(host, database_id).await,
    }
}

async fn received_store(host: &Host, database_id: &str) -> Result<Arc<FoldDB>, RestoreError> {
    let id =
        Id::try_from(database_id.to_owned()).map_err(|_| RestoreError::Scope("database id"))?;
    let store = crate::registered_share::receiver_store(host, &id)
        .await
        .map_err(|_| RestoreError::Scope("database store"))?;
    crate::log_sync::note_receiver(database_id, &store);
    Ok(store)
}
