//! Stable database handles for shares accepted by this running node.
mod store;

pub(crate) use store::receiver_store;

use crate::cloud_backup_client::byte_backup::ShareEnvironment;
use crate::share_cache::{ShareCache, ShareCacheError, ShareSnapshot};
use cloud_backup_protocol::{v2::Stream, Digest, Id};
use serde::Serialize;
use std::path::Path;

type Result<T> = std::result::Result<T, ShareCacheError>;

/// Local database registration. Public JSON contains only identifying and
/// verification status, never an invitation or contributor authority.
#[derive(Serialize)]
pub struct RegisteredShare {
    pub id: Id,
    pub publisher: Digest,
    pub name: Option<String>,
    pub spec: Option<crate::share_spec::PublicationSpec>,
    pub snapshot: Option<ShareSnapshot>,
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
    pub fn publisher(&self) -> &Digest {
        &self.publisher
    }

    pub fn stream(&self) -> &Stream {
        &self.stream
    }

    pub fn environment(&self) -> ShareEnvironment {
        self.environment
    }
}

/// Save a verified invitation and stable database handle marked as a signed log.
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
/// `accept_signed_log` already verified the publisher and kept that stream id.
/// A signed log stays readable after the bearer expires. This does not create a store.
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
        .filter_map(|id| cache.managed_record(&id).transpose())
        .collect()
}

/// Resolve a shared database by immutable ID or exact local alias.
pub fn resolve(home: &Path, selector: &str) -> Result<Option<RegisteredShare>> {
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
