//! Stable database handles, node-owned Share bindings, and receiver `FoldDB` stores.

use crate::cloud_backup_client::ShareEnvironment;
use crate::host::Host;
use crate::share_cache::{
    private_directory, publish, Lock, Registration, Result, ShareCache, ShareCacheError,
    MANAGED_SHARE_DIRECTORY,
};
use cloud_backup_protocol::{v2::Stream, Digest, Id};
use fold_db::crypto::E2eKeys;
use fold_db::fold_db_core::FoldDB;
use fold_db::storage::config::DatabaseConfig;
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use zeroize::{Zeroize, Zeroizing};

const REGISTRY_LOCK: &str = "registry.lock";
const MAX_REGISTERED: usize = 32;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ManagedRegistration {
    name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    spec: Option<crate::profile::PublicationSpec>,
    expires_ms: u64,
    /// Named-database signed log. Publication shares omit this.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub(crate) signed_log: bool,
}

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

impl ShareCache {
    fn open_managed(home: &Path, create: bool) -> Result<Option<Self>> {
        let root = home.join(MANAGED_SHARE_DIRECTORY);
        if !create
            && matches!(fs::symlink_metadata(root), Err(e) if e.kind() == ErrorKind::NotFound)
        {
            return Ok(None);
        }
        let home = home.canonicalize()?;
        let root = home.join(MANAGED_SHARE_DIRECTORY);
        let recipient = crate::share_invitation::recipient(&home)
            .map_err(|_| ShareCacheError::Invalid("recipient identity"))?;
        private_directory(&root, create)?;
        Ok(Some(Self { root, recipient }))
    }

    fn managed_ids(&self) -> Result<Vec<Id>> {
        let directory = private_directory(&self.root, false)?;
        let mut ids = Vec::new();
        for entry in fs::read_dir(&self.root)? {
            let name = entry?
                .file_name()
                .into_string()
                .map_err(|_| ShareCacheError::Invalid("registry entry"))?;
            if name == REGISTRY_LOCK {
                continue;
            }
            if ids.len() == MAX_REGISTERED {
                return Err(ShareCacheError::Invalid("registry size"));
            }
            ids.push(Id::try_from(name).map_err(|_| ShareCacheError::Invalid("registry entry"))?);
        }
        directory
            .check()
            .map_err(|_| ShareCacheError::Invalid("registry directory"))?;
        ids.sort_by(|a, b| a.as_str().cmp(b.as_str()));
        Ok(ids)
    }

    fn managed_record(&self, id: &Id) -> Result<Option<RegisteredShare>> {
        let entry = match self.entry(id, false) {
            Ok(entry) => entry,
            Err(ShareCacheError::Unavailable) => return Ok(None),
            Err(error) => return Err(error),
        };
        let _pointer = entry.lock(Lock::PointerRead)?;
        let (registration, _) = entry.load()?;
        registration
            .as_ref()
            .map(|registration| describe(id, registration))
            .transpose()
    }

    /// Sealed registration marked as a signed log.
    ///
    /// `expected` is the stable database id. It must equal the invitation stream.
    /// A mismatch writes nothing.
    fn register_signed_log(
        &self,
        home: &Path,
        invitation: &[u8],
        publisher: &Digest,
        name: Option<&str>,
        expected: Option<&Id>,
    ) -> Result<RegisteredShare> {
        let opened = self.recipient.open(invitation, publisher)?;
        let id = &opened.permission.stream().stream_id;
        if expected.is_some_and(|expected| expected.as_str() != id.as_str()) {
            return Err(ShareCacheError::Invalid("database id"));
        }
        let directory = private_directory(&self.root, false)?;
        let lock = directory
            .open_lock(REGISTRY_LOCK)
            .map_err(|_| ShareCacheError::Invalid("registry lock"))?;
        lock.try_lock().map_err(|_| ShareCacheError::Busy)?;
        if crate::host::owned::registry_claims(home, id.as_str(), name)
            .map_err(|()| ShareCacheError::Unavailable)?
        {
            return Err(ShareCacheError::Invalid("database selector already in use"));
        }
        self.admit_name(id, name)?;
        let entry = self.entry(id, true)?;
        let _writer = entry.lock(Lock::Writer)?;
        let (registration, previous) = entry.load()?;
        if let Some(registration) = &registration {
            self.require_management(registration)?;
        }
        let mut registration =
            Registration::with_invitation(registration, &opened, publisher, invitation)?;
        let state = registration.managed.get_or_insert(ManagedRegistration {
            name: None,
            spec: None,
            expires_ms: 0,
            signed_log: false,
        });
        if let Some(name) = name {
            state.name = Some(name.to_owned());
        }
        if let Some(spec) = opened.spec {
            if state.spec.as_ref().is_some_and(|current| current != &spec) {
                return Err(ShareCacheError::Invalid(
                    "invitation spec differs from this share handle",
                ));
            }
            state.spec = Some(spec);
        }
        state.expires_ms = opened.permission.expires_ms();
        state.signed_log = true;
        publish(
            &entry,
            previous.as_deref().map(Vec::as_slice),
            &registration,
        )?;
        describe(id, &registration)
    }

    fn admit_name(&self, id: &Id, name: Option<&str>) -> Result<()> {
        let ids = self.managed_ids()?;
        if ids.len() == MAX_REGISTERED && !ids.contains(id) {
            return Err(ShareCacheError::Invalid("registry size"));
        }
        if let Some(name) = name {
            for other in ids.iter().filter(|other| *other != id) {
                if self
                    .managed_record(other)?
                    .is_some_and(|registered| registered.name.as_deref() == Some(name))
                {
                    return Err(ShareCacheError::Invalid("name already registered"));
                }
            }
        }
        Ok(())
    }
}

fn describe(id: &Id, registration: &Registration) -> Result<RegisteredShare> {
    let state = registration
        .managed
        .as_ref()
        .filter(|_| &registration.stream.stream_id == id)
        .ok_or(ShareCacheError::Invalid("managed registration"))?;
    Ok(RegisteredShare {
        id: id.clone(),
        publisher: registration.publisher.clone(),
        name: state.name.clone(),
        spec: state.spec.clone(),
        expires_ms: state.expires_ms,
        stream: registration.stream.clone(),
        environment: registration.environment,
        signed_log: state.signed_log,
    })
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
    private_directory(&entry, false)?;
    let data = entry.join("log");
    let inbox = entry.join("search-inbox");
    private_directory(&data, true)?;
    private_directory(&inbox, true)?;
    Ok((data, inbox))
}
