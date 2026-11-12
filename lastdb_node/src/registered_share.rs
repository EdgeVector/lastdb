//! Stable database handles for shares accepted by this running node.
mod store;
mod worker;

pub(crate) use store::receiver_store;

use crate::cloud_backup_client::byte_backup::ShareEnvironment;
use crate::share_cache::{ShareCache, ShareCacheError, ShareReader, ShareSnapshot};
use crate::{offline_home::Directory, share_publication};
use cloud_backup_protocol::{v2::Stream, Digest, Id};
use serde::Serialize;
use std::path::{Path, PathBuf};

pub use worker::spawn;

type Result<T> = std::result::Result<T, ShareCacheError>;

/// Local database registration. Public JSON contains only identifying and
/// verification status, never an invitation or contributor authority.
#[derive(Serialize)]
pub struct RegisteredShare {
    pub id: Id,
    pub publisher: Digest,
    pub name: Option<String>,
    pub snapshot: Option<ShareSnapshot>,
    pub expires_ms: u64,
    #[serde(skip)]
    pub(crate) home: PathBuf,
    #[serde(skip)]
    pub(crate) stream: Stream,
    #[serde(skip)]
    pub(crate) environment: ShareEnvironment,
    #[serde(skip)]
    pub(crate) contributor_grant: Option<Vec<u8>>,
    #[serde(skip)]
    pub(crate) contributor_expires_ms: Option<u64>,
    /// Named-database signed log. Publication shares leave this false.
    #[serde(skip)]
    pub(crate) signed_log: bool,
}

impl RegisteredShare {
    /// Hold one verified read-only view for the duration of a local request.
    pub fn reader(&self) -> Result<ShareReader> {
        ShareCache::open_managed(&self.home, false)?
            .ok_or(ShareCacheError::Unavailable)?
            .reader(&self.id)
    }

    pub fn identity_home(&self) -> &Path {
        &self.home
    }

    pub fn publisher(&self) -> &Digest {
        &self.publisher
    }

    pub fn stream(&self) -> &Stream {
        &self.stream
    }

    pub fn environment(&self) -> ShareEnvironment {
        self.environment
    }

    pub fn contributor_grant(&self) -> Option<&[u8]> {
        self.contributor_grant.as_deref()
    }

    /// Whether this node holds a contributor grant whose signed expiry is in the future.
    pub fn has_unexpired_contributor_grant(&self) -> bool {
        self.contributor_grant.is_some()
            && self
                .contributor_expires_ms
                .is_some_and(|expiry| expiry > fold_db::clock::unix_millis())
    }
}

/// Read the immutable local publication ID. An ordinary personal home has no marker.
pub fn local_publication_id(home: &Path) -> Result<Option<Id>> {
    let home = home.canonicalize()?;
    let invalid = || ShareCacheError::Invalid("local publication marker");
    let directory = Directory::open(&home).map_err(|_| invalid())?;
    share_publication::read_marker(&directory)
        .map(|marker| marker.map(|marker| marker.id))
        .map_err(|_| invalid())
}

/// Save a verified invitation and stable database handle before cloud work.
pub fn accept(
    home: &Path,
    sealed: &[u8],
    publisher: &Digest,
    name: Option<&str>,
) -> Result<RegisteredShare> {
    if let Some(name) = name {
        require_name(name)?;
    }
    ShareCache::open_managed(home, true)?
        .ok_or(ShareCacheError::Unavailable)?
        .register_managed(home, sealed, publisher, name)
}

/// Save the same invitation as [`accept`], marked as a signed log.
///
/// When supplied, `database_id` must match the verified invitation.
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
///
/// `accept` already verified the publisher and kept that stream id. A signed
/// log stays readable after the bearer expires. This does not create a store.
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
pub fn list(home: &Path) -> Result<Vec<RegisteredShare>> {
    let Some(cache) = ShareCache::open_managed(home, false)? else {
        return Ok(Vec::new());
    };
    cache
        .managed_ids()?
        .into_iter()
        .filter_map(|id| cache.managed_record(home, &id).transpose())
        .collect()
}

/// Resolve a shared database by immutable ID or exact local alias.
pub fn resolve(home: &Path, selector: &str) -> Result<Option<RegisteredShare>> {
    if let Ok(id) = Id::try_from(selector.to_owned()) {
        let Some(cache) = ShareCache::open_managed(home, false)? else {
            return Ok(None);
        };
        return cache.managed_record(home, &id);
    }
    require_name(selector)?;
    Ok(list(home)?
        .into_iter()
        .find(|entry| entry.name.as_deref() == Some(selector)))
}

/// Attach an independently sealed contributor grant to an existing share.
pub fn attach_contributor(home: &Path, selector: &str, sealed: &[u8]) -> Result<RegisteredShare> {
    let registration = resolve(home, selector)?.ok_or(ShareCacheError::Unavailable)?;
    let identity_home = registration.identity_home();
    ShareCache::open_managed(identity_home, false)?
        .ok_or(ShareCacheError::Unavailable)?
        .attach_managed_grant(identity_home, &registration.id, sealed)
}

fn require_name(name: &str) -> Result<()> {
    if name.is_empty()
        || name.len() > 64
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
        || name.eq_ignore_ascii_case("personal")
        || uuid::Uuid::parse_str(name).is_ok()
    {
        return Err(ShareCacheError::Invalid("database name"));
    }
    Ok(())
}
