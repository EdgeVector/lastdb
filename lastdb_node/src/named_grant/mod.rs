//! Publisher grant, revoke, and invitation sealing for one named database.

mod config;
mod exchange;
mod pending;
mod successor;

use crate::cloud::load_connected_home_context;
use crate::cloud_backup_client::{
    authorization_post, build_shared_http_client, GuestReadPermission, PathPostTarget,
};
use crate::host::owned;
use crate::profile::endpoints::Environment;
use crate::share_invitation::{self, ShareContact};
use cloud_backup_protocol::v2::share_access::{
    MemberListReadRequest, ShareEnvironment, ShareMemberState, SHARE_MEMBER_LIST_READ_PATH,
};
use cloud_backup_protocol::v2::{Stream, Version};
use cloud_backup_protocol::{CloudScope, Digest, Id};
use fold_db::security::Ed25519KeyPair;
use reqwest::Url;
use std::path::Path;
use std::sync::Arc;
use zeroize::Zeroizing;

pub(crate) use config::{lock_applied, read_config, set_config, ConfigError, ConfigUpdate};

/// What the publisher wants the member row to become.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MemberIntent {
    /// Active reader. `regrant` restores a revoked key.
    Read { regrant: bool },
    /// Active contributor. `regrant` restores a revoked key.
    Submit { regrant: bool },
    /// Keep the row and mark it revoked.
    Revoke,
}

/// Service acknowledgement of one member row.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MemberAck {
    pub(crate) database_id: String,
    pub(crate) revision: u64,
    pub(crate) unchanged: bool,
}

struct PublisherSession {
    http: reqwest::Client,
    origin: Url,
    environment: ShareEnvironment,
    signer: Arc<Ed25519KeyPair>,
    publisher: Digest,
    scope: CloudScope,
    seed: Zeroizing<[u8; 32]>,
}

impl PublisherSession {
    async fn open(home: &Path) -> Result<Self, String> {
        let ctx = load_connected_home_context(home, None).await?;
        let environment = match ctx.env {
            Environment::Dev => ShareEnvironment::Dev,
            Environment::Prod => ShareEnvironment::Prod,
        };
        let origin = Url::parse(ctx.api_url)
            .map_err(|_| "saved credentials do not match the share publisher and environment")?;
        let seed = Zeroizing::new(ctx.keypair.secret_key_bytes());
        let publisher = Digest::try_from(fold_db::hex::hex_lower(ctx.keypair.public_key_bytes()))
            .map_err(|_| "invalid share publisher public key")?;
        Ok(Self {
            http: build_shared_http_client()
                .map_err(|_| "cannot construct member service client")?,
            origin,
            environment,
            signer: ctx.keypair,
            publisher,
            scope: ctx.scope,
            seed,
        })
    }
}

impl std::fmt::Debug for PublisherSession {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PublisherSession")
            .field("publisher", &self.publisher)
            .field("environment", &self.environment)
            .finish()
    }
}

/// Grant or revoke one node key. `Ok` means the service has the desired row.
pub(crate) async fn update_member(
    home: &Path,
    selector: &str,
    node: &Digest,
    intent: MemberIntent,
) -> Result<MemberAck, String> {
    let entry = owned::find_shareable(home, selector)?;
    if entry.whole_database() && matches!(intent, MemberIntent::Submit { .. }) {
        return Err("whole-database writers require a configured schema scope".into());
    }
    let _lock = pending::lock(home, &entry.id)?;
    let session = PublisherSession::open(home).await?;
    if &session.publisher == node {
        return Err("share.members.publisher".into());
    }
    if let Some(saved) = pending::load(home, &entry.id)? {
        return pending::retry(home, &session, &entry.id, node, intent, saved).await;
    }
    let document = exchange::read_document(&session, &entry.id).await?;
    successor::require_stream(&document, &session, &entry.id)?;
    if let Some(ack) = successor::unchanged(&document, &entry.id, node, intent)? {
        return Ok(ack);
    }
    let signed = successor::sign(&session, &document, node, intent)?;
    pending::store(home, &entry.id, node, intent, &signed.body)?;
    exchange::commit(&session, home, &entry.id, &signed).await
}

/// Seal this database's AES key to `contact`. Opens a fresh publisher session.
pub(crate) async fn seal_named_invitation(
    home: &Path,
    database_id: &str,
    contact: &ShareContact,
    lifetime_hours: u32,
) -> Result<Vec<u8>, String> {
    let entry = owned::find_shareable(home, database_id)?;
    let session = PublisherSession::open(home).await?;
    seal_invitation(&session, &entry, contact, lifetime_hours)
}

pub(crate) async fn issue_invitation(
    home: &Path,
    database_id: &str,
    contact_code: &str,
    expected_key: &Digest,
    lifetime_hours: u32,
) -> Result<Vec<u8>, ConfigError> {
    let contact = ShareContact::parse(contact_code)
        .map_err(|error| ConfigError::Invalid(error.to_string()))?;
    if contact.identity_public_key() != expected_key || !(1..=168).contains(&lifetime_hours) {
        return Err(ConfigError::Invalid(
            "invalid invitation recipient or lifetime".into(),
        ));
    }
    let entry = owned::find_shareable(home, database_id).map_err(ConfigError::Invalid)?;
    let _lock = pending::lock(home, database_id).map_err(|_| ConfigError::Unconfirmed)?;
    if pending::load(home, database_id)
        .map_err(|_| ConfigError::Unconfirmed)?
        .is_some()
    {
        return Err(ConfigError::Pending);
    }
    let session = PublisherSession::open(home)
        .await
        .map_err(|e| config::unconfirmed_or_payment(&e))?;
    let document = config::read(&session, database_id).await?;
    if !document.update.as_ref().is_some_and(|update| {
        update.members.iter().any(|member| {
            &member.node_public_key == expected_key && member.state == ShareMemberState::Active
        })
    }) {
        return Err(ConfigError::Invalid("recipient has no active grant".into()));
    }
    seal_invitation(&session, &entry, &contact, lifetime_hours).map_err(ConfigError::Invalid)
}

fn seal_invitation(
    session: &PublisherSession,
    entry: &owned::Entry,
    contact: &ShareContact,
    lifetime_hours: u32,
) -> Result<Vec<u8>, String> {
    if !(1..=168).contains(&lifetime_hours) {
        return Err("share invitation lifetime must be between 1 and 168 hours".into());
    }
    let id =
        Id::try_from(entry.id.clone()).map_err(|_| "database is not registered on this node")?;
    let key = owned::content_encryption_key(&session.seed, id.as_str())?;
    crate::log_envelope::refuse_owner_material(&session.seed, &key)
        .map_err(|_| "named database content key is not invitable")?;
    let expires = chrono::Utc::now() + chrono::Duration::hours(i64::from(lifetime_hours));
    let permission = GuestReadPermission::sign(
        Stream {
            scope: session.scope.clone(),
            stream_id: id,
        },
        session.environment,
        &expires.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        session.signer.public_key_bytes(),
        |bytes| session.signer.sign(bytes).to_bytes(),
    )
    .map_err(|error| error.to_string())?;
    let sealed = share_invitation::seal(
        contact,
        &permission,
        &key,
        &session.signer,
        entry.spec.as_ref(),
    )
    .map_err(|error| error.to_string())?;
    if sealed.len() > share_invitation::MAX_SEALED_INVITATION_BYTES {
        return Err("sealed share invitation exceeds its size limit".into());
    }
    Ok(sealed)
}

fn read_body(session: &PublisherSession, database_id: &str) -> Result<Vec<u8>, String> {
    let request = MemberListReadRequest {
        version: Version::V2,
        stream: Stream {
            scope: session.scope.clone(),
            stream_id: Id::try_from(database_id.to_owned())
                .map_err(|_| "database registry is unavailable")?,
        },
    };
    request.validate().map_err(|error| error.field)?;
    serde_json::to_vec(&request).map_err(|_| "cannot encode member list read".into())
}

fn authorization(
    session: &PublisherSession,
    body: &[u8],
    database_id: &str,
    revision: u64,
) -> Result<String, String> {
    let database_id = Id::try_from(database_id.to_owned())
        .map_err(|_| "database registry is unavailable".to_string())?;
    authorization_post(
        &session.signer,
        PathPostTarget {
            scope: &session.scope,
            database_id: &database_id,
            environment: session.environment,
            path: SHARE_MEMBER_LIST_READ_PATH,
        },
        body,
        revision,
        fold_db::clock::unix_millis(),
    )
}
