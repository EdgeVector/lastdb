//! Install one named-database snapshot chain into its registered store.
//!
//! An owned database uses its sibling store. A received database uses
//! `registered_share::receiver_store` after the stored invitation is
//! verified. E fetches `delivery/snapshot` and calls [`install`]. This module
//! does not send HTTP and does not acknowledge the cloud log. E acknowledges
//! only after `install` returns and any still-stored suffix is durable.
//!
//! Record payloads and file fragments are `log_envelope` ciphertexts. The
//! content key is the owned sibling key, or the key from the verified
//! registered-share invitation. This module does not call `accept`, does not
//! write the registry, and does not open the personal store.

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
    let target = open_named(host, &request.database_id).await?;
    let chain = chain::order(
        &request.database_id,
        &request.snapshot_ref,
        &request.objects,
    )?;
    decode::check_chain(&target.key, &request.database_id, &chain)?;
    let mut journal = journal::load(&host.home, &request.database_id)?
        .unwrap_or_else(|| journal::Journal::fresh(&request.database_id, &request.snapshot_ref));
    if let Resume::Continue { retarget } = journal::plan(&journal, request, &chain)? {
        if retarget {
            journal.complete = false;
            journal.tip_ref.clone_from(&request.snapshot_ref);
            journal::save(&host.home, &journal)?;
        }
        apply::replay(
            target.store.as_ref(),
            &target.key,
            &host.home,
            &request.database_id,
            &chain,
            &mut journal,
            files,
        )
        .await?;
        return finish(
            target.store.as_ref(),
            &host.home,
            request,
            &chain,
            &mut journal,
        )
        .await;
    }
    prefix(request, &chain)
}

struct Target {
    store: Arc<FoldDB>,
    key: DatabaseContentKey,
}

async fn open_named(host: &Host, database_id: &str) -> Result<Target, RestoreError> {
    require_id(database_id)?;
    if owned_claim(host, database_id)? {
        owned_target(host, database_id)
    } else {
        received_target(host, database_id).await
    }
}

/// True only after [`install`] flushed the store and saved a complete prefix.
pub(crate) fn prefix_durable(home: &std::path::Path, database_id: &str) -> bool {
    match journal::load(home, database_id) {
        Ok(Some(journal)) => journal.complete && journal.database_id == database_id,
        _ => false,
    }
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

fn owned_target(host: &Host, database_id: &str) -> Result<Target, RestoreError> {
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
    Ok(Target { store, key })
}

/// Open the managed share store after the stored invitation checks out.
///
/// A publication registration has no signed log, so it does not get this
/// store. The personal store and `databases/<id>/data` are not substitutes.
async fn received_target(host: &Host, database_id: &str) -> Result<Target, RestoreError> {
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
    let store = crate::registered_share::receiver_store(host, &id)
        .await
        .map_err(|_| RestoreError::Scope("database store"))?;
    Ok(Target { store, key })
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
