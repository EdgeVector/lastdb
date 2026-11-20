//! Open read link.
//!
//! The token is opaque, not a URL. The content key must not land in a query
//! string, a browser history, or a proxy log. The node does not read the
//! share settings when it makes the token. Minting is local: saved credentials
//! supply environment and account id. There is no ExeMem blob/bytes round-trip.
//! People wraps this token inside a hand-off consent code; ExeMem and the relay
//! only see signed grant entries on the chain.

use std::fs::File;
use std::path::Path;

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use cloud_backup_protocol::v2::share_access::ShareEnvironment;
use cloud_backup_protocol::v2::Stream;
use cloud_backup_protocol::{canonical_bytes, AccountId, CloudScope, DecimalU64, Digest, Id};
use serde::{Deserialize, Serialize};
use zeroize::Zeroize;

use crate::cloud::load_verified_cloud_credentials;
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
    /// Publisher device stream the receiver must `log.read` from seq 1.
    device_id: Digest,
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
    /// Publisher `device_id` (Hex32) for the birth/grant stream.
    device_id: Digest,
    content_key: Digest,
}

pub(crate) struct IssueError {
    status: u16,
    message: &'static str,
    /// Saved credentials could not be opened. The caller classifies enrollment.
    cloud_connection: bool,
}

impl IssueError {
    const fn invalid(message: &'static str) -> Self {
        Self {
            status: 400,
            message,
            cloud_connection: false,
        }
    }

    const fn unavailable(message: &'static str) -> Self {
        Self {
            status: 503,
            message,
            cloud_connection: false,
        }
    }

    const fn cloud_connection() -> Self {
        Self {
            status: 503,
            message: "cloud connection is unavailable",
            cloud_connection: true,
        }
    }

    pub(crate) fn is_cloud_connection(&self) -> bool {
        self.cloud_connection
    }

    pub(crate) fn status(&self) -> (u16, &'static str) {
        (self.status, self.message)
    }
}

/// Mint the token for one shareable database from local credentials only.
pub(crate) fn issue(host: &Host, entry: &Entry) -> Result<String, IssueError> {
    let creds = match load_verified_cloud_credentials(&host.home) {
        Ok(Some(creds)) => creds,
        Ok(None) => return Err(IssueError::cloud_connection()),
        Err(error) => {
            tracing::warn!(target: "lastdbd::log_sync", %error, "open read link is unavailable");
            return Err(IssueError::cloud_connection());
        }
    };
    let account_id = AccountId::try_from(host.user_hash.clone())
        .map_err(|_| IssueError::unavailable("node account id is unavailable"))?;
    if let Some(conn) = &creds.connection {
        if conn.account_id != account_id {
            return Err(IssueError::unavailable(
                "saved cloud connection account does not match this home's identity",
            ));
        }
    }
    let environment = ShareEnvironment::parse(creds.env.as_str()).map_err(|_| {
        IssueError::unavailable("saved cloud environment is not a share environment")
    })?;
    let publisher = Digest::try_from(fold_db::hex::hex_lower(host.keypair.public_key_bytes()))
        .map_err(|_| IssueError::unavailable("node key is not a publisher key"))?;
    let device_id = local_device_id(&host.home)?;
    let seed = zeroize::Zeroizing::new(host.keypair.secret_key_bytes());
    let content = owned::content_encryption_key(&seed, &entry.id)
        .map_err(|_| IssueError::unavailable("database content key is unavailable"))?;
    let database_id =
        Id::try_from(entry.id.clone()).map_err(|_| IssueError::invalid("database id"))?;
    // Phase 2 open tokens carry account identity only. Generation is not fetched
    // from ExeMem; zero matches the local signed-history publisher scope.
    let scope = CloudScope {
        account_id,
        account_generation: DecimalU64::new(0),
    };
    encode(&OpenedLink {
        environment,
        scope,
        database_id,
        publisher,
        device_id,
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
        device_id: payload.device_id,
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
        managed(
            name.map(str::to_owned).or(kept),
            opened.device_id.as_str().to_owned(),
        ),
    )?;
    publish(
        &entry,
        previous.as_deref().map(Vec::as_slice),
        &registration,
    )?;
    mark_signed_log_ready(home, id)?;
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
        device_id: opened.device_id.clone(),
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

fn managed(name: Option<String>, publisher_device_id: String) -> ManagedRegistration {
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
        publisher_device_id: Some(publisher_device_id),
    }
}

/// Empty signed-log receiver store: no publisher snapshot; sync pulls the chain.
fn mark_signed_log_ready(home: &Path, id: &Id) -> Result<(), ShareCacheError> {
    let entry = home
        .canonicalize()
        .map_err(|_| ShareCacheError::Unavailable)?
        .join(registered_share::MANAGED_SHARE_DIRECTORY)
        .join(id.as_str());
    private_directory(&entry, true)?;
    let marker = entry.join(registered_share::SNAPSHOT_ABSENT);
    if marker.exists() {
        return Ok(());
    }
    std::fs::write(&marker, b"").map_err(|_| ShareCacheError::Io)?;
    Ok(())
}

/// Same file as `log_sync::device::device_id`. Kept here so open-link mint stays
/// below the sync layer.
fn local_device_id(home: &Path) -> Result<Digest, IssueError> {
    use crate::profile::private_fs as disk;
    let path = home.join("log-sync").join("device-id");
    if let Ok(Some(bytes)) = disk::read_limited(&path, 4096) {
        let text = std::str::from_utf8(&bytes)
            .map_err(|_| IssueError::unavailable("device id is not text"))?
            .trim();
        return Digest::try_from(text.to_owned())
            .map_err(|_| IssueError::unavailable("device id is not hex32"));
    }
    let mut bytes = [0u8; 32];
    rand::RngCore::fill_bytes(&mut rand::thread_rng(), &mut bytes);
    let hex = fold_db::hex::hex_lower(&bytes);
    disk::write_private(&path, format!("{hex}\n").as_bytes())
        .map_err(|_| IssueError::unavailable("cannot write device id"))?;
    Digest::try_from(hex).map_err(|_| IssueError::unavailable("device id is not hex32"))
}

fn stream(opened: &OpenedLink) -> Stream {
    Stream {
        scope: opened.scope.clone(),
        stream_id: opened.database_id.clone(),
    }
}
