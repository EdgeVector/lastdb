//! Open read link.
//!
//! The token is opaque, not a URL. The content key must not land in a query
//! string, a browser history, or a proxy log. The node does not read the
//! share settings when it makes the token. The service decides who may use it.

use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use cloud_backup_protocol::v2::share_access::ShareEnvironment;
use cloud_backup_protocol::v2::Stream;
use cloud_backup_protocol::{canonical_bytes, CloudScope, Digest, Id};
use serde::{Deserialize, Serialize};
use zeroize::Zeroize;

use crate::cloud::load_connected_home_context;
use crate::host::owned::{self, Entry};
use crate::host::Host;
use crate::profile::PublicationSpec;
use crate::registered_share::{self, describe, ManagedRegistration, RegisteredShare};
use crate::share_cache::{
    private_directory, publish, Lock, Registration, ShareCache, ShareCacheError,
};

const PREFIX: &str = "lastdb-read-v1.";
const MAX_TOKEN: usize = 2048;

pub(crate) struct OpenedLink {
    environment: ShareEnvironment,
    scope: CloudScope,
    database_id: Id,
    publisher: Digest,
    content_key: [u8; 32],
}

impl Drop for OpenedLink {
    fn drop(&mut self) {
        self.content_key.zeroize();
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Payload {
    version: u8,
    environment: ShareEnvironment,
    scope: CloudScope,
    database: Id,
    publisher: Digest,
    content_key: Digest,
}

pub(crate) struct IssueError {
    status: u16,
    message: &'static str,
}

impl IssueError {
    const fn invalid(message: &'static str) -> Self {
        Self {
            status: 400,
            message,
        }
    }

    const fn unavailable(message: &'static str) -> Self {
        Self {
            status: 503,
            message,
        }
    }

    pub(crate) fn status(&self) -> (u16, &'static str) {
        (self.status, self.message)
    }
}

/// Mint the token for one shareable database. This does not read share settings.
pub(crate) async fn issue(host: &Host, entry: &Entry) -> Result<String, IssueError> {
    let ctx = match load_connected_home_context(&host.home, Some(Arc::clone(&host.keypair))).await {
        Ok(ctx) => ctx,
        Err(error) => {
            tracing::warn!(target: "lastdbd::log_sync", %error, "open read link is unavailable");
            return Err(IssueError::unavailable("cloud connection is unavailable"));
        }
    };
    if ctx.account_id.as_str() != host.user_hash {
        return Err(IssueError::unavailable(
            "node account id does not match the node key",
        ));
    }
    let environment = ShareEnvironment::parse(ctx.env.as_str()).map_err(|_| {
        IssueError::unavailable("saved cloud environment is not a share environment")
    })?;
    let publisher = Digest::try_from(fold_db::hex::hex_lower(host.keypair.public_key_bytes()))
        .map_err(|_| IssueError::unavailable("node key is not a publisher key"))?;
    let seed = zeroize::Zeroizing::new(host.keypair.secret_key_bytes());
    let content = owned::content_encryption_key(&seed, &entry.id)
        .map_err(|_| IssueError::unavailable("database content key is unavailable"))?;
    let database_id =
        Id::try_from(entry.id.clone()).map_err(|_| IssueError::invalid("database id"))?;
    encode(&OpenedLink {
        environment,
        scope: ctx.scope,
        database_id,
        publisher,
        content_key: *content,
    })
    .map_err(IssueError::invalid)
}

pub(crate) fn encode(opened: &OpenedLink) -> Result<String, &'static str> {
    let bytes = canonical_bytes(&payload(opened)?).map_err(|_| "invalid link")?;
    let token = format!("{PREFIX}{}", URL_SAFE_NO_PAD.encode(bytes));
    if token.len() > MAX_TOKEN {
        return Err("invalid link");
    }
    Ok(token)
}

pub(crate) fn open(token: &str) -> Result<OpenedLink, &'static str> {
    let raw = decode_token(token)?;
    let payload: Payload = serde_json::from_slice(&raw).map_err(|_| "invalid link")?;
    if payload.version != 1 {
        return Err("invalid link");
    }
    let again = canonical_bytes(&payload).map_err(|_| "invalid link")?;
    if again != raw {
        return Err("invalid link");
    }
    let content_key =
        fold_db::hex::hex_decode_array::<32>(payload.content_key.as_str()).ok_or("invalid link")?;
    if content_key == [0; 32] {
        return Err("invalid link");
    }
    Ok(OpenedLink {
        environment: payload.environment,
        scope: payload.scope,
        database_id: payload.database,
        publisher: payload.publisher,
        content_key,
    })
}

/// Store a link registration. It does not open the receiver store.
pub(crate) fn register(
    home: &Path,
    opened: &OpenedLink,
    name: Option<&str>,
) -> Result<RegisteredShare, ShareCacheError> {
    if let Some(name) = name {
        if !crate::profile::valid_database_name(name) {
            return Err(ShareCacheError::Invalid("database name"));
        }
    }
    let cache = ShareCache::open_managed(home, true)?.ok_or(ShareCacheError::Unavailable)?;
    let _registry = registry_lock(&cache)?;
    let id = &opened.database_id;
    if owned::registry_claims(home, id.as_str(), name).map_err(|()| ShareCacheError::Unavailable)? {
        return Err(ShareCacheError::Invalid("database selector already in use"));
    }
    cache.admit_name(id, name)?;
    let entry = cache.entry(id, true)?;
    let _writer = entry.lock(Lock::Writer)?;
    let (existing, previous) = entry.load()?;
    let kept = retained_name(&cache, existing.as_ref(), opened)?;
    let registration = Registration::with_link(
        opened.publisher.clone(),
        stream(opened),
        opened.environment,
        opened.content_key,
        managed(name.map(str::to_owned).or(kept)),
    )?;
    publish(
        &entry,
        previous.as_deref().map(Vec::as_slice),
        &registration,
    )?;
    describe(id, &registration)
}

fn payload(opened: &OpenedLink) -> Result<Payload, &'static str> {
    if opened.content_key == [0; 32] {
        return Err("invalid link");
    }
    Ok(Payload {
        version: 1,
        environment: opened.environment,
        scope: opened.scope.clone(),
        database: opened.database_id.clone(),
        publisher: opened.publisher.clone(),
        content_key: Digest::try_from(fold_db::hex::hex_lower(opened.content_key))
            .map_err(|_| "invalid link")?,
    })
}

fn decode_token(token: &str) -> Result<Vec<u8>, &'static str> {
    let Some(body) = token.strip_prefix(PREFIX) else {
        return Err("invalid link");
    };
    if token.len() > MAX_TOKEN
        || body.is_empty()
        || !body
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
    {
        return Err("invalid link");
    }
    URL_SAFE_NO_PAD.decode(body).map_err(|_| "invalid link")
}

fn registry_lock(cache: &ShareCache) -> Result<File, ShareCacheError> {
    let directory = private_directory(&cache.root, false)?;
    let lock = directory
        .open_lock(registered_share::REGISTRY_LOCK)
        .map_err(|_| ShareCacheError::Invalid("registry lock"))?;
    lock.try_lock().map_err(|_| ShareCacheError::Busy)?;
    Ok(lock)
}

fn retained_name(
    cache: &ShareCache,
    existing: Option<&Registration>,
    opened: &OpenedLink,
) -> Result<Option<String>, ShareCacheError> {
    let Some(existing) = existing else {
        return Ok(None);
    };
    cache.require_management(existing)?;
    if !existing.accepts_link_refresh(
        &opened.publisher,
        &stream(opened),
        opened.environment,
        &opened.content_key,
    ) {
        return Err(ShareCacheError::Invalid("database selector already in use"));
    }
    Ok(existing
        .managed
        .as_ref()
        .and_then(|state| state.name.clone()))
}

fn managed(name: Option<String>) -> ManagedRegistration {
    let binding = database_policy::SpecBinding::whole_database();
    ManagedRegistration {
        name,
        spec: Some(PublicationSpec {
            name: binding.name().to_owned(),
            version: binding.version(),
        }),
        expires_ms: 0,
        signed_log: true,
        link: true,
    }
}

fn stream(opened: &OpenedLink) -> Stream {
    Stream {
        scope: opened.scope.clone(),
        stream_id: opened.database_id.clone(),
    }
}
