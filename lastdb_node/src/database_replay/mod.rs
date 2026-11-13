//! Install one database snapshot chain into its registered store.
//!
//! Resolve the registered store by database id. Verify the chain before
//! opening a new receiver store. The caller acknowledges the cloud cursor
//! after the installed prefix and any delivered suffix are durable.
//!
//! A received log page calls [`replay_delivered`]. It uses this same decode
//! and apply path, flushes the receiver store, and does not rewrite a
//! snapshot journal. The caller acknowledges only after that flush.
//!
//! Each entry is a `log_envelope` around one accepted body. That body is a
//! schema put, a schema drop, a record, a file, an inline file, or a
//! contribution. An external file is the uploaded envelope named by
//! `file.object`. Install opens that envelope with the same binding, checks
//! the marker, and seals it with `seal_record_file`. A marker that requires
//! bytes and has no object is refused. Schema put and drop use the opened
//! store's schema manager and keep the captured schema id. The content key
//! comes from the selected local store or its verified share invitation.

mod accepted_record;
mod apply;
mod chain;
mod decode;
mod journal;
mod owned;
mod page;
mod payload;
mod types;
mod wire;

pub use payload::{AcceptedBody, FileObject, FileSection};
pub use types::{
    IncorporatedFrom, RestoreError, SnapshotEntry, SnapshotFileRequest, SnapshotFileSource,
    SnapshotInstall, SnapshotManifest, SnapshotObject, SnapshotPart,
};
pub use wire::{manifest_from_json, part_from_json};

pub(crate) use owned::replay_owned;
pub(crate) use page::DeliveredEntry;

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::sync::Arc;

use cloud_backup_protocol::Id;
use fold_db::fold_db_core::FoldDB;
use fold_db::storage::disk_pack::NODE_MARKER;

use crate::log_envelope::DatabaseContentKey;
use crate::Host;

use journal::Resume;

/// Apply one verified snapshot chain to the database `request` selects.
///
/// The returned prefix is durable. It is not an acknowledgement.
pub(crate) async fn install(
    host: &Host,
    request: &SnapshotInstall,
    files: &mut (dyn SnapshotFileSource + Send),
    local_device: Option<&str>,
) -> Result<u64, RestoreError> {
    let target = resolve_target(host, &request.database_id)?;
    let chain = chain::order(
        &request.database_id,
        &request.snapshot_ref,
        &request.objects,
    )?;
    let decoder = target.decoder(&request.database_id);
    decode::check_chain(decoder, &chain)?;
    let mut journal = journal::load(&host.home, &request.database_id)?
        .unwrap_or_else(|| journal::Journal::fresh(&request.database_id, &request.snapshot_ref));
    let plan = journal::plan(&journal, request, &chain)?;
    let store = open_store(
        host,
        &request.database_id,
        &target,
        matches!(&plan, Resume::Continue { .. }),
    )
    .await?;
    if let Resume::Continue { retarget } = plan {
        if retarget {
            journal.complete = false;
            journal.tip_ref.clone_from(&request.snapshot_ref);
            journal::save(&host.home, &journal)?;
        }
        let owner = hex::encode(host.keypair.public_key_bytes());
        let mut applier = apply::SnapshotApplier {
            store: &store,
            decoder,
            home: &host.home,
            files,
            owner: match &target.place {
                Place::Owned(_) if target.private => Some((&owner, local_device)),
                _ => None,
            },
        };
        let replay = applier.replay(&chain, &mut journal);
        store
            .db_ops()
            .native_runtime()
            .replay_accepted(replay)
            .await?;
        return finish(store.as_ref(), &host.home, request, &chain, &mut journal).await;
    }
    prefix(&chain)
}

/// Apply one signed log page into its registered store.
///
/// The page is durable when this returns. A snapshot journal is left as it is.
pub(crate) async fn replay_delivered(
    host: &Host,
    database_id: &str,
    publisher: &str,
    delivered: &[DeliveredEntry],
    files: &mut (dyn SnapshotFileSource + Send),
) -> Result<(), RestoreError> {
    page::replay(host, database_id, publisher, delivered, files).await
}

struct RestoreTarget {
    key: DatabaseContentKey,
    place: Place,
    private: bool,
}

impl RestoreTarget {
    fn decoder<'a>(&'a self, database_id: &'a str) -> decode::EntryDecoder<'a> {
        decode::EntryDecoder {
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

/// True after a complete snapshot install, or after a durable log-only prefix.
///
/// An incomplete snapshot journal stays false. A missing journal is ready
/// only when sync has stored `log-sync/<id>/prefix.json` after a successful
/// pull and ack, including an empty log with no snapshot.
pub(crate) fn prefix_durable(home: &std::path::Path, database_id: &str) -> bool {
    match journal::load(home, database_id) {
        Ok(Some(journal)) if journal.complete && journal.database_id == database_id => true,
        Ok(None) => log_only_prefix(home, database_id),
        Ok(Some(_)) | Err(_) => false,
    }
}

fn log_only_prefix(home: &std::path::Path, database_id: &str) -> bool {
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

/// Check the stored invitation. The store opens only after the chain check.
///
/// A publication registration has no signed log, so it does not get this
/// store. The personal store and `databases/<id>/data` are not substitutes.
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
    let opened = crate::registered_share::verified_invitation(&host.home, &id)
        .map_err(|_| RestoreError::Scope("named database"))?;
    let key = DatabaseContentKey::from_invitation(&opened)
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
    create: bool,
) -> Result<Arc<FoldDB>, RestoreError> {
    match &target.place {
        Place::Owned(store) => Ok(Arc::clone(store)),
        Place::Received => received_store(host, database_id, create).await,
    }
}

async fn received_store(
    host: &Host,
    database_id: &str,
    create: bool,
) -> Result<Arc<FoldDB>, RestoreError> {
    let id =
        Id::try_from(database_id.to_owned()).map_err(|_| RestoreError::Scope("database id"))?;
    if !create && !log_ready(&host.home, &id) {
        return Err(RestoreError::Scope("database store"));
    }
    crate::registered_share::receiver_store(host, &id)
        .await
        .map_err(|_| RestoreError::Scope("database store"))
}

fn log_ready(home: &std::path::Path, id: &Id) -> bool {
    let Ok(home) = home.canonicalize() else {
        return false;
    };
    let directory = home
        .join(crate::byte_cloud_restore::MANAGED_SHARE_DIRECTORY)
        .join(id.as_str())
        .join("log");
    fs::symlink_metadata(&directory).is_ok_and(|meta| meta.is_dir())
        && fs::symlink_metadata(directory.join(NODE_MARKER)).is_ok_and(|meta| meta.is_file())
}

async fn finish(
    store: &FoldDB,
    home: &std::path::Path,
    request: &SnapshotInstall,
    chain: &[chain::Link],
    journal: &mut journal::Journal,
) -> Result<u64, RestoreError> {
    if journal.applied_refs.len() != chain.len() {
        return Err(RestoreError::Conflict("snapshot journal"));
    }
    store
        .flush()
        .await
        .map_err(|_| RestoreError::Durable("database flush"))?;
    journal.complete = true;
    journal.tip_ref.clone_from(&request.snapshot_ref);
    journal.through_delivery_seq = chain
        .last()
        .map(|link| link.manifest.through_delivery_seq)
        .ok_or(RestoreError::Chain("snapshot chain"))?;
    journal.next_part = 0;
    journal.next_entry = 0;
    journal.attempt = None;
    journal.current_ref = None;
    journal::save(home, journal)?;
    prefix(chain)
}

fn prefix(chain: &[chain::Link]) -> Result<u64, RestoreError> {
    chain
        .last()
        .map(|link| link.manifest.through_delivery_seq)
        .ok_or(RestoreError::Chain("snapshot chain"))
}
