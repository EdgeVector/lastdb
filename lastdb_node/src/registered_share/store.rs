//! Open a received database after its snapshot is installed.
//!
//! Open-link registrations store the content key locally. The store captures
//! nothing and publishes no snapshot.

use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use cloud_backup_protocol::Id;
use fold_db::crypto::E2eKeys;
use fold_db::fold_db_core::FoldDB;
use fold_db::storage::config::DatabaseConfig;
use fold_db::storage::disk_pack::NODE_MARKER;
use zeroize::{Zeroize, Zeroizing};

use crate::host::Host;
use crate::share_cache::{
    private_directory, Lock, Result, ShareCache, ShareCacheError, MANAGED_SHARE_DIRECTORY,
};

use super::SNAPSHOT_ABSENT;

/// Open the signed-log store for `id`. The same handle is reused.
pub(crate) async fn receiver_store(host: &Host, id: &Id) -> Result<Arc<FoldDB>> {
    let share = super::resolve(&host.home, id.as_str())?.ok_or(ShareCacheError::Unavailable)?;
    if !share.signed_log || share.id.as_str() != id.as_str() {
        return Err(ShareCacheError::Invalid("database id"));
    }
    let mut open = host.received.lock().await;
    if let Some(store) = open.get(id.as_str()) {
        return Ok(Arc::clone(store));
    }
    // A read must not create the node marker before sync installs the snapshot.
    if !store_ready(&host.home, id)? {
        return Err(ShareCacheError::Unavailable);
    }
    let store = open_receiver(host, id).await?;
    open.insert(id.as_str().to_owned(), Arc::clone(&store));
    Ok(store)
}

async fn open_receiver(host: &Host, id: &Id) -> Result<Arc<FoldDB>> {
    let keys = content_keys(&host.home, id)?;
    let data = store_dirs(&host.home, id)?;
    fold_db::fold_db_core::factory::create_fold_db_with_options(
        &DatabaseConfig::local(data),
        &keys,
        &host.keypair,
        fold_db::fold_db_core::factory::FoldDbOptions {
            // A member proposes from its history (`log_sync::proposal`), and
            // nothing sends a spool for a received database, so it captures nothing.
            capture_accepted: false,
            capture_author: None,
        },
    )
    .await
    .map_err(|_| ShareCacheError::Unavailable)
}

/// Content key for a received open-link registration.
pub(crate) fn content_material(home: &Path, id: &Id) -> Result<Zeroizing<[u8; 32]>> {
    let cache = ShareCache::open_managed(home, false)?.ok_or(ShareCacheError::Unavailable)?;
    let entry = cache.entry(id, false)?;
    let _pointer = entry.lock(Lock::PointerRead)?;
    let (registration, _) = entry.load()?;
    let registration = registration.ok_or(ShareCacheError::Unavailable)?;
    cache.require_management(&registration)?;
    registration.link_key()
}

fn content_keys(home: &Path, id: &Id) -> Result<E2eKeys> {
    let mut key = content_material(home, id)?;
    let keys = E2eKeys::from_content_key(*key).map_err(|_| ShareCacheError::Unavailable)?;
    key.zeroize();
    Ok(keys)
}

fn store_ready(home: &Path, id: &Id) -> Result<bool> {
    let home = home
        .canonicalize()
        .map_err(|_| ShareCacheError::Unavailable)?;
    let entry = home.join(MANAGED_SHARE_DIRECTORY).join(id.as_str());
    if regular_file(&entry.join("log").join(NODE_MARKER))? {
        return Ok(true);
    }
    regular_file(&entry.join(SNAPSHOT_ABSENT))
}

fn regular_file(path: &Path) -> Result<bool> {
    match fs::symlink_metadata(path) {
        Ok(meta) if meta.file_type().is_symlink() => {
            Err(ShareCacheError::Invalid("database store"))
        }
        Ok(meta) => Ok(meta.is_file()),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(false),
        Err(_) => Err(ShareCacheError::Unavailable),
    }
}

fn store_dirs(home: &Path, id: &Id) -> Result<PathBuf> {
    let home = home
        .canonicalize()
        .map_err(|_| ShareCacheError::Unavailable)?;
    let entry = home.join(MANAGED_SHARE_DIRECTORY).join(id.as_str());
    private_directory(&entry, false)?;
    let data = entry.join("log");
    private_directory(&data, true)?;
    Ok(data)
}
