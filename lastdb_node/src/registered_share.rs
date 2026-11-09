//! Stable database handles for shares accepted by this running node.
mod worker;

use crate::share_cache::{ShareCache, ShareCacheError, ShareReader, ShareSnapshot};
use cloud_backup_client::byte_backup::ShareEnvironment;
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
