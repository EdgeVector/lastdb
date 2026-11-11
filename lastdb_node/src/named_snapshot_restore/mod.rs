//! Install one named-database snapshot chain into the open sibling store.
//!
//! E fetches `delivery/snapshot` and calls [`install`]. This module does not
//! send HTTP and does not acknowledge the cloud log. E acknowledges only
//! after `install` returns and any still-stored suffix is durable.

mod apply;
mod chain;
mod decode;
mod journal;
mod types;
mod wire;

pub use types::{
    IncorporatedFrom, InstalledPrefix, NoSnapshotFiles, RestoreError, SnapshotEntry,
    SnapshotFileRequest, SnapshotFileSource, SnapshotInstall, SnapshotManifest, SnapshotObject,
    SnapshotPart,
};
pub use wire::{manifest_from_json, part_from_json};

use std::fs;
use std::sync::Arc;

use cloud_backup_protocol::Id;
use fold_db::fold_db_core::FoldDB;
use fold_db::storage::disk_pack::NODE_MARKER;

use crate::host::owned::Resolved;
use crate::Host;

use journal::Resume;
use types::SnapshotInstall;

/// Apply one verified snapshot chain to the named database `request` selects.
///
/// The returned prefix is durable. It is not an acknowledgement.
pub async fn install(
    host: &Host,
    request: &SnapshotInstall,
    files: &mut (dyn SnapshotFileSource + Send),
) -> Result<InstalledPrefix, RestoreError> {
    let store = open_named(host, &request.database_id)?;
    let chain = chain::order(
        &request.database_id,
        &request.snapshot_ref,
        &request.objects,
    )?;
    decode::check_chain(&request.database_id, &chain)?;
    let mut journal = journal::load(&host.home, &request.database_id)?
        .unwrap_or_else(|| journal::Journal::fresh(&request.database_id, &request.snapshot_ref));
    if let Resume::Continue { retarget } = journal::plan(&journal, request, &chain)? {
        if retarget {
            journal.complete = false;
            journal.tip_ref.clone_from(&request.snapshot_ref);
            journal::save(&host.home, &journal)?;
        }
        apply::replay(
            store.as_ref(),
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

fn open_named(host: &Host, database_id: &str) -> Result<Arc<FoldDB>, RestoreError> {
    let id =
        Id::try_from(database_id.to_owned()).map_err(|_| RestoreError::Scope("database id"))?;
    if id.as_str() != database_id {
        return Err(RestoreError::Scope("database id"));
    }
    let entries = host
        .owned
        .owned_entries()
        .map_err(|_| RestoreError::Scope("database registry"))?;
    if !entries.iter().any(|entry| entry.id == database_id) {
        return Err(RestoreError::Scope("named database"));
    }
    let store = match host.owned.resolve(database_id) {
        Ok((Resolved::Owned, Some(store))) => store,
        Ok(_) => return Err(RestoreError::Scope("named database")),
        Err(_) => return Err(RestoreError::Scope("database registry")),
    };
    let marker = host
        .home
        .join("databases")
        .join(database_id)
        .join("data")
        .join(NODE_MARKER);
    match fs::symlink_metadata(&marker) {
        Ok(metadata) if metadata.is_file() => Ok(store),
        _ => Err(RestoreError::Scope("database store")),
    }
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
