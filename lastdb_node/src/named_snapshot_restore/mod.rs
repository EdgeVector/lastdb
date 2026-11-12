//! Install one named-database snapshot chain into its registered store.
//!
//! An owned database uses its sibling store. A received database uses
//! `registered_share::receiver_store` after the chain check. A completed
//! journal does not create a missing store. E fetches `delivery/snapshot`
//! and calls [`install`]. This module does not send HTTP and does not
//! acknowledge the cloud log. E acknowledges only after `install` returns
//! and any still-stored suffix is durable.
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
//! is the owned sibling key, or the key from the verified registered-share
//! invitation. This module does not call `accept`, does not write the
//! registry, and does not open the personal store.

mod accepted_record;
mod apply;
mod chain;
mod decode;
mod journal;
mod page;
mod payload;
mod types;
mod wire;

pub use payload::{
    parse_accepted, AcceptedBody, ContributionSection, FileObject, FileSection, InlineSection,
};
pub use types::{
    IncorporatedFrom, InstalledPrefix, NoSnapshotFiles, RestoreError, SnapshotEntry,
    SnapshotFileRequest, SnapshotFileSource, SnapshotInstall, SnapshotManifest, SnapshotObject,
    SnapshotPart,
};
pub use wire::{manifest_from_json, part_from_json};

pub(crate) use page::DeliveredEntry;

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::sync::Arc;

use cloud_backup_protocol::Id;
use fold_db::fold_db_core::FoldDB;
use fold_db::storage::disk_pack::NODE_MARKER;
use zeroize::Zeroizing;

use crate::host::owned::Resolved;
use crate::log_envelope::DatabaseContentKey;
use crate::Host;

use journal::Resume;

/// Apply one verified snapshot chain to the named database `request` selects.
///
/// The returned prefix is durable. It is not an acknowledgement.
pub async fn install(
    host: &Host,
    request: &SnapshotInstall,
    files: &mut (dyn SnapshotFileSource + Send),
) -> Result<InstalledPrefix, RestoreError> {
    let named = open_named(host, &request.database_id)?;
    let chain = chain::order(
        &request.database_id,
        &request.snapshot_ref,
        &request.objects,
    )?;
    decode::check_chain(&named.key, &request.database_id, &chain)?;
    let mut journal = journal::load(&host.home, &request.database_id)?
        .unwrap_or_else(|| journal::Journal::fresh(&request.database_id, &request.snapshot_ref));
    let plan = journal::plan(&journal, request, &chain)?;
    let store = open_store(
        host,
        &request.database_id,
        &named,
        matches!(&plan, Resume::Continue { .. }),
    )
    .await?;
    if let Resume::Continue { retarget } = plan {
        if retarget {
            journal.complete = false;
            journal.tip_ref.clone_from(&request.snapshot_ref);
            journal::save(&host.home, &journal)?;
        }
        apply::replay(
            store.as_ref(),
            &named.key,
            &host.home,
            &request.database_id,
            &chain,
            &mut journal,
            files,
        )
        .await?;
        return finish(store.as_ref(), &host.home, request, &chain, &mut journal).await;
    }
    prefix(request, &chain)
}

/// Apply one received log page into the registered receiver store.
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

struct Named {
    key: DatabaseContentKey,
    place: Place,
}

enum Place {
    Owned(Arc<FoldDB>),
    Received,
}

fn open_named(host: &Host, database_id: &str) -> Result<Named, RestoreError> {
    require_id(database_id)?;
    if owned_claim(host, database_id)? {
        owned_target(host, database_id)
    } else {
        Ok(Named {
            key: received_key(host, database_id)?,
            place: Place::Received,
        })
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

fn owned_claim(host: &Host, database_id: &str) -> Result<bool, RestoreError> {
    let entries = host
        .owned
        .owned_entries()
        .map_err(|_| RestoreError::Scope("database registry"))?;
    Ok(entries.iter().any(|entry| entry.id == database_id))
}

fn owned_target(host: &Host, database_id: &str) -> Result<Named, RestoreError> {
    let store = match host.owned.resolve(database_id) {
        Ok((Resolved::Owned, Some(store))) => store,
        Ok(_) => return Err(RestoreError::Scope("named database")),
        Err(_) => return Err(RestoreError::Scope("database registry")),
    };
    if !store_marker(host, database_id) {
        return Err(RestoreError::Scope("database store"));
    }
    let seed = Zeroizing::new(host.keypair.secret_key_bytes());
    let key = DatabaseContentKey::owned(&seed, database_id)
        .map_err(|_| RestoreError::Scope("database store"))?;
    if key.database_id() != database_id {
        return Err(RestoreError::Scope("database id"));
    }
    Ok(Named {
        key,
        place: Place::Owned(store),
    })
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
    named: &Named,
    create: bool,
) -> Result<Arc<FoldDB>, RestoreError> {
    match &named.place {
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

fn store_marker(host: &Host, database_id: &str) -> bool {
    let marker = host
        .home
        .join("databases")
        .join(database_id)
        .join("data")
        .join(NODE_MARKER);
    fs::symlink_metadata(marker).is_ok_and(|metadata| metadata.is_file())
}

async fn finish(
    store: &FoldDB,
    home: &std::path::Path,
    request: &SnapshotInstall,
    chain: &[chain::Link],
    journal: &mut journal::Journal,
) -> Result<InstalledPrefix, RestoreError> {
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
    prefix(request, chain)
}

fn prefix(
    request: &SnapshotInstall,
    chain: &[chain::Link],
) -> Result<InstalledPrefix, RestoreError> {
    let through = chain
        .last()
        .map(|link| link.manifest.through_delivery_seq)
        .ok_or(RestoreError::Chain("snapshot chain"))?;
    Ok(types::InstalledPrefix {
        database_id: request.database_id.clone(),
        snapshot_ref: request.snapshot_ref.clone(),
        through_delivery_seq: through,
    })
}
