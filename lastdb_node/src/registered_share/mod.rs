//! Stable database handles, node-owned Share bindings, and receiver stores that follow the mutation log.
//! Decided state: docs/designs/cloud-api.md (signed history; grants in-chain).

use crate::share_cache::{
    private_directory, Lock, Registration, Result, ShareCache, ShareCacheError,
};
use cloud_backup_protocol::{Digest, Id};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;

mod store;

pub(crate) use store::receiver_store;

pub(crate) const REGISTRY_LOCK: &str = "registry.lock";
/// Sync writes this when the publisher has no snapshot, so replay may open an empty store.
pub(crate) const SNAPSHOT_ABSENT: &str = "snapshot-absent";
const MAX_REGISTERED: usize = 32;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ManagedRegistration {
    pub(crate) name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) spec: Option<crate::profile::PublicationSpec>,
    pub(crate) expires_ms: u64,
    /// Named-database signed log. Publication shares omit this.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub(crate) signed_log: bool,
    /// Open read material. The registration has no sealed invitation.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub(crate) link: bool,
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
}

impl ShareCache {
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

    pub(crate) fn admit_name(&self, id: &Id, name: Option<&str>) -> Result<()> {
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

pub(crate) fn describe(id: &Id, registration: &Registration) -> Result<RegisteredShare> {
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
        signed_log: state.signed_log,
    })
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
