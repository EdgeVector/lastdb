//! Private submission grants and publisher-side intake for ordinary Share.
mod apply;
mod crypto;
mod outbox;
mod state;

use state::State;

pub use apply::{WriteRequest, WriteRule};
pub(crate) use crypto::inbox_key;
pub(crate) use outbox::{submit_registered, validate_grant, SubmitError};

use crate::cloud_backup_client::byte_backup::{ByteBackupClient, GuestAppendPermission};
use crate::share_invitation::ShareContact;
use crate::share_publication::{self, PublicationIdentity, WriteMode};
use cloud_backup_protocol::v2::{
    context::ContextReadRequest, contributions::ReadAudience, Stream, Version,
};
use cloud_backup_protocol::{AccountId, Digest, Id};
use fold_db::canonical::CanonicalWriter;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use std::path::Path;
use x25519_dalek::PublicKey;

const MAX_WRITERS: usize = 12;
const MAX_RULES: usize = 8;

#[derive(Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Policy {
    writers: Vec<Grant>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Grant {
    writer: Digest,
    stream: Stream,
    version: u64,
    operation: Option<Id>,
    expires_at: String,
    rules: Vec<WriteRule>,
    through: u64,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Invitation {
    publication: Id,
    writer: Digest,
    version: u64,
    authorization: String,
    inbox_key: [u8; 32],
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Submission {
    publication: Id,
    stream: Stream,
    version: u64,
    sequence: u64,
    request: WriteRequest,
}

/// App request to grant one verified writer a complete set of rules.
pub struct GrantRequest<'a> {
    pub home: &'a Path,
    pub owner_home: &'a Path,
    pub contact: &'a ShareContact,
    pub rules: Vec<WriteRule>,
    pub operation_id: Id,
    pub expected_version: u64,
}

/// Save or replace a grant, returning the sealed private append invitation.
pub async fn grant(request: GrantRequest<'_>) -> Result<Vec<u8>, String> {
    let identity = share_publication::load(request.home, request.owner_home)?;
    require_validated(&identity)?;
    if request.rules.is_empty()
        || request.rules.len() > MAX_RULES
        || request.rules.iter().any(|rule| !rule.valid())
    {
        return Err("invalid contributor write rules".into());
    }
    let state = State::open(request.owner_home, "contributor-policy", &identity.id)?;
    let (mut policy, saved) = state.load::<Policy>()?;
    let writer = request.contact.identity_public_key();
    let client = identity.client(request.owner_home)?;
    let publication = public_stream(&client, &identity.account, &identity.id).await?;
    let existing = policy
        .writers
        .iter()
        .position(|entry| &entry.writer == writer);
    if let Some(existing) = existing {
        let current = &policy.writers[existing];
        if current.operation.as_ref() == Some(&request.operation_id) {
            if current.rules != request.rules
                || current.version.checked_sub(1) != Some(request.expected_version)
            {
                return Err("contributor grant operation ID conflict".into());
            }
            return seal_grant(request.owner_home, &identity, request.contact, current);
        }
        if current.version != request.expected_version || current.stream.scope != publication.scope
        {
            return Err("contributor grant version or scope conflict".into());
        }
    } else if request.expected_version != 0 || policy.writers.len() == MAX_WRITERS {
        return Err("contributor grant version or writer limit".into());
    }
    if policy
        .writers
        .iter()
        .any(|entry| entry.operation.as_ref() == Some(&request.operation_id))
    {
        return Err("contributor grant operation ID conflict".into());
    }
    let version = request
        .expected_version
        .checked_add(1)
        .ok_or("contributor version limit")?;
    let previous = existing.map(|position| &policy.writers[position]);
    let stream = match previous {
        Some(entry) => entry.stream.clone(),
        None => Stream {
            scope: publication.scope.clone(),
            stream_id: inbox_id(&publication.stream_id, writer)?,
        },
    };
    let through = previous.map_or(0, |entry| entry.through);
    let grant = Grant {
        writer: writer.clone(),
        stream,
        version,
        operation: Some(request.operation_id),
        expires_at: (chrono::Utc::now() + chrono::Duration::hours(24))
            .to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        rules: request.rules,
        through,
    };
    let invitation = seal_grant(request.owner_home, &identity, request.contact, &grant)?;
    client
        .set_read_audience(&publication, ReadAudience::PublicationReaders)
        .await
        .map_err(|e| e.to_string())?;
    client
        .set_read_audience(&grant.stream, ReadAudience::ScopeOwner)
        .await
        .map_err(|e| e.to_string())?;
    if let Some(index) = existing {
        policy.writers[index] = grant;
    } else {
        policy.writers.push(grant);
    }
    state.save(&policy, saved.as_deref().map(Vec::as_slice))?;
    Ok(invitation)
}

fn seal_grant(
    owner_home: &Path,
    identity: &PublicationIdentity,
    contact: &ShareContact,
    grant: &Grant,
) -> Result<Vec<u8>, String> {
    let owner = share_publication::owner_seed(owner_home)?;
    let inbox = inbox_key(&owner, &identity.id)?;
    let permission = GuestAppendPermission::sign(
        grant.stream.clone(),
        identity.environment,
        &grant.expires_at,
        identity.signer.public_key_bytes(),
        |bytes| identity.signer.sign(bytes).to_bytes(),
    )
    .map_err(|e| e.to_string())?;
    crypto::seal(
        &Invitation {
            publication: identity.id.clone(),
            writer: grant.writer.clone(),
            version: grant.version,
            authorization: permission.authorization().to_owned(),
            inbox_key: PublicKey::from(&inbox).to_bytes(),
        },
        &contact.encryption_key(),
        &identity.signer,
        crypto::GRANT,
    )
}

/// Revoke native write authority. The publisher drains already-uploaded entries.
pub fn revoke(
    home: &Path,
    owner_home: &Path,
    writer: &Digest,
    expected_version: u64,
) -> Result<(), String> {
    let identity = share_publication::load(home, owner_home)?;
    require_validated(&identity)?;
    let state = State::open(owner_home, "contributor-policy", &identity.id)?;
    let (mut policy, saved) = state.load::<Policy>()?;
    let grant = policy
        .writers
        .iter_mut()
        .find(|entry| &entry.writer == writer)
        .filter(|entry| entry.version == expected_version && entry.operation.is_some())
        .ok_or("contributor grant version conflict")?;
    grant.version = grant
        .version
        .checked_add(1)
        .ok_or("contributor version limit")?;
    grant.operation = None;
    grant.rules.clear();
    state.save(&policy, saved.as_deref().map(Vec::as_slice))?;
    Ok(())
}

fn require_validated(identity: &PublicationIdentity) -> Result<(), String> {
    if identity.write_mode != WriteMode::PublisherValidated {
        return Err("share does not allow contributor submissions".into());
    }
    Ok(())
}

fn inbox_id(publication: &Id, writer: &Digest) -> Result<Id, String> {
    let encoded = CanonicalWriter::new()
        .field(b"lastdb:share-inbox-stream:v1")
        .field(publication.as_str().as_bytes())
        .field(writer.as_str().as_bytes())
        .finish();
    let hash = Sha256::digest(encoded);
    let mut id = [0; 16];
    id.copy_from_slice(&hash[..16]);
    Id::try_from(uuid::Builder::from_random_bytes(id).into_uuid().to_string())
        .map_err(|_| "invalid inbox ID".into())
}

async fn public_stream(
    client: &ByteBackupClient,
    account: &AccountId,
    id: &Id,
) -> Result<Stream, String> {
    let response = client
        .context(&ContextReadRequest {
            version: Version::V2,
            account_id: account.clone(),
            required_destinations: Vec::new(),
        })
        .await
        .map_err(|e| e.to_string())?;
    Ok(Stream {
        scope: response.scope,
        stream_id: id.clone(),
    })
}
