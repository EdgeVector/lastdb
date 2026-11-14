//! Receiver FoldDB for one signed-log share.
//!
//! The store is `<home>/received-shares-v1/<id>/log`. Its AES key is the
//! publication key inside the stored invitation. Snapshot install opens this
//! store through [`receiver_store`]. This is not `databases/<id>/data` and not
//! the personal store. There is no raw key file and no second log store.

use crate::host::Host;
use crate::share_cache::{ShareCacheError, MANAGED_SHARE_DIRECTORY};
use cloud_backup_protocol::Id;
use fold_db::crypto::E2eKeys;
use fold_db::fold_db_core::FoldDB;
use fold_db::storage::config::DatabaseConfig;
use std::fs::{self, File};
use std::io::ErrorKind;
use std::os::unix::fs::{DirBuilderExt, MetadataExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use zeroize::{Zeroize, Zeroizing};

use super::verified_invitation;

type Result<T> = std::result::Result<T, ShareCacheError>;

/// Open the signed-log store for `id`. The same handle is reused.
///
/// A publication registration is refused. The caller maps a failure to
/// "database store is unavailable" and does not include the database id.
pub(crate) async fn receiver_store(host: &Host, id: &Id) -> Result<Arc<FoldDB>> {
    let share = super::resolve(&host.home, id.as_str())?.ok_or(ShareCacheError::Unavailable)?;
    if !share.signed_log || share.id.as_str() != id.as_str() {
        return Err(ShareCacheError::Invalid("database id"));
    }
    let mut open = host.received.lock().await;
    if let Some(store) = open.get(id.as_str()) {
        return Ok(Arc::clone(store));
    }
    let store = open_receiver(host, id).await?;
    open.insert(id.as_str().to_owned(), Arc::clone(&store));
    Ok(store)
}

pub(crate) async fn shutdown_stores(host: &Host) -> std::result::Result<(), String> {
    let stores: Vec<Arc<FoldDB>> = {
        let mut open = host.received.lock().await;
        open.drain().map(|(_, store)| store).collect()
    };
    let mut failed = false;
    for store in stores {
        if store.shutdown().await.is_err() {
            failed = true;
        }
    }
    if failed {
        Err("received database shutdown flush failed".into())
    } else {
        Ok(())
    }
}

impl Host {
    /// Flush signed-log receiver stores before the owned sibling stores.
    pub async fn shutdown_received_databases(&self) -> std::result::Result<(), String> {
        shutdown_stores(self).await
    }
}

async fn open_receiver(host: &Host, id: &Id) -> Result<Arc<FoldDB>> {
    let keys = content_keys(&host.home, id)?;
    let (data, inbox) = store_dirs(&host.home, id)?;
    fold_db::db_operations::search_index::queue_policy::prepare(&inbox)
        .map_err(|_| ShareCacheError::Unavailable)?;
    fold_db::fold_db_core::factory::create_fold_db_with_options(
        &DatabaseConfig::local(data),
        &keys,
        Arc::clone(&host.keypair),
        fold_db::fold_db_core::factory::FoldDbOptions {
            offline_maintenance: false,
            search_outbox_inbox: Some(inbox),
            capture_accepted: false,
            capture_initial_snapshot: false,
        },
    )
    .await
    .map_err(|_| ShareCacheError::Unavailable)
}

fn content_keys(home: &Path, id: &Id) -> Result<E2eKeys> {
    let opened = verified_invitation(home, id)?;
    if opened.permission.stream().stream_id.as_str() != id.as_str() {
        return Err(ShareCacheError::Invalid("stored invitation"));
    }
    let mut key = Zeroizing::new(*opened.publication_key);
    let keys = E2eKeys::from_content_key(*key).map_err(|_| ShareCacheError::Unavailable)?;
    key.zeroize();
    Ok(keys)
}

fn store_dirs(home: &Path, id: &Id) -> Result<(PathBuf, PathBuf)> {
    let home = home
        .canonicalize()
        .map_err(|_| ShareCacheError::Unavailable)?;
    let entry = home.join(MANAGED_SHARE_DIRECTORY).join(id.as_str());
    require_private(&entry)?;
    let data = entry.join("log");
    let inbox = entry.join("search-inbox");
    ensure_private(&data)?;
    ensure_private(&inbox)?;
    Ok((data, inbox))
}

fn require_private(path: &Path) -> Result<()> {
    let meta = fs::symlink_metadata(path).map_err(|_| ShareCacheError::Unavailable)?;
    private_dir(&meta)
}

fn ensure_private(path: &Path) -> Result<()> {
    if fs::symlink_metadata(path).is_err_and(|error| error.kind() == ErrorKind::NotFound) {
        match fs::DirBuilder::new().mode(0o700).create(path) {
            Ok(()) => {
                if let Some(parent) = path.parent() {
                    let _ = File::open(parent).and_then(|file| file.sync_all());
                }
            }
            Err(error) if error.kind() == ErrorKind::AlreadyExists => {}
            Err(_) => return Err(ShareCacheError::Unavailable),
        }
    }
    let meta = fs::symlink_metadata(path).map_err(|_| ShareCacheError::Unavailable)?;
    private_dir(&meta)
}

fn private_dir(meta: &fs::Metadata) -> Result<()> {
    if meta.is_dir() && meta.uid() == unsafe { libc::geteuid() } && meta.mode() & 0o7777 == 0o700 {
        Ok(())
    } else {
        Err(ShareCacheError::Invalid("private directory"))
    }
}
