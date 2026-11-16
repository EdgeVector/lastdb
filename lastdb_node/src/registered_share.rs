//! Stable database handles and receiver `FoldDB` stores for accepted shares.

use crate::cloud_backup_client::storage_transfer::ShareEnvironment;
use crate::host::Host;
use crate::share_cache::{ShareCache, ShareCacheError, MANAGED_SHARE_DIRECTORY};
use cloud_backup_protocol::{v2::Stream, Digest, Id};
use fold_db::crypto::E2eKeys;
use fold_db::fold_db_core::FoldDB;
use fold_db::storage::config::DatabaseConfig;
use serde::Serialize;
use std::fs::{self, File};
use std::io::ErrorKind;
use std::os::unix::fs::{DirBuilderExt, MetadataExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use zeroize::{Zeroize, Zeroizing};

type Result<T> = std::result::Result<T, ShareCacheError>;

/// Local database registration. Public JSON contains only identifying and
/// verification status, never an invitation or contributor authority.
#[derive(Serialize)]
pub(crate) struct RegisteredShare {
    pub id: Id,
    pub publisher: Digest,
    pub name: Option<String>,
    pub spec: Option<crate::profile::PublicationSpec>,
    pub expires_ms: u64,
    #[serde(skip)]
    pub(crate) stream: Stream,
    #[serde(skip)]
    pub(crate) environment: ShareEnvironment,
    /// Named-database signed log. Publication shares leave this false.
    #[serde(skip)]
    pub(crate) signed_log: bool,
}

impl RegisteredShare {
    pub(crate) fn whole_database(&self) -> bool {
        self.spec
            .as_ref()
            .is_some_and(crate::profile::PublicationSpec::whole_database)
    }

    pub(crate) fn stream(&self) -> &Stream {
        &self.stream
    }

    pub(crate) fn environment(&self) -> ShareEnvironment {
        self.environment
    }
}

/// Save a verified invitation and stable database handle marked as a signed log.
pub(crate) fn accept_signed_log(
    home: &Path,
    sealed: &[u8],
    publisher: &Digest,
    name: Option<&str>,
    database_id: Option<&str>,
) -> Result<RegisteredShare> {
    let id = database_id
        .map(|id| Id::try_from(id.to_owned()))
        .transpose()
        .map_err(|_| ShareCacheError::Invalid("database selector"))?;
    if let Some(name) = name {
        require_name(name)?;
    }
    ShareCache::open_managed(home, true)?
        .ok_or(ShareCacheError::Unavailable)?
        .register_signed_log(home, sealed, publisher, name, id.as_ref())
}

/// Re-open the invitation stored for this stable stream id.
pub(crate) fn verified_invitation(
    home: &Path,
    id: &Id,
) -> Result<crate::share_invitation::OpenedShareInvitation> {
    let Some(cache) = ShareCache::open_managed(home, false)? else {
        return Err(ShareCacheError::Unavailable);
    };
    cache.open_verified(id)
}

/// Return node-owned shared database registrations, including pending ones.
pub(crate) fn list(home: &Path) -> Result<Vec<RegisteredShare>> {
    let Some(cache) = ShareCache::open_managed(home, false)? else {
        return Ok(Vec::new());
    };
    cache
        .managed_ids()?
        .into_iter()
        .filter_map(|id| cache.managed_record(&id).transpose())
        .collect()
}

/// Resolve a shared database by immutable ID or exact local alias.
pub(crate) fn resolve(home: &Path, selector: &str) -> Result<Option<RegisteredShare>> {
    if let Ok(id) = Id::try_from(selector.to_owned()) {
        let Some(cache) = ShareCache::open_managed(home, false)? else {
            return Ok(None);
        };
        return cache.managed_record(&id);
    }
    require_name(selector)?;
    Ok(list(home)?
        .into_iter()
        .find(|entry| entry.name.as_deref() == Some(selector)))
}

fn require_name(name: &str) -> Result<()> {
    if !crate::profile::valid_database_name(name) {
        return Err(ShareCacheError::Invalid("database name"));
    }
    Ok(())
}

/// Open the signed-log store for `id`. The same handle is reused.
pub(crate) async fn receiver_store(host: &Host, id: &Id) -> Result<Arc<FoldDB>> {
    let share = resolve(&host.home, id.as_str())?.ok_or(ShareCacheError::Unavailable)?;
    if !share.signed_log || share.id.as_str() != id.as_str() {
        return Err(ShareCacheError::Invalid("database id"));
    }
    let mut open = host.received.lock().await;
    if let Some(store) = open.get(id.as_str()) {
        return Ok(Arc::clone(store));
    }
    let store = open_receiver(host, id, share.whole_database()).await?;
    open.insert(id.as_str().to_owned(), Arc::clone(&store));
    Ok(store)
}

impl Host {
    /// Flush signed-log receiver stores before the owned sibling stores.
    pub async fn shutdown_received_databases(&self) -> std::result::Result<(), String> {
        let stores: Vec<Arc<FoldDB>> = {
            let mut open = self.received.lock().await;
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
}

async fn open_receiver(host: &Host, id: &Id, whole_database: bool) -> Result<Arc<FoldDB>> {
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
            track_shared_record_revisions: whole_database,
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
