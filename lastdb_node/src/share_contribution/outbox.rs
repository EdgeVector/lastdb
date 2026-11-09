//! Durable local submission and exact encrypted retries.
use super::{crypto, inbox_id, Invitation, State, Submission, WriteRequest};
use crate::share_publication;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use cloud_backup_client::byte_backup::{
    BackupTransportError, GuestAppendPermission, GuestContributionWriter, ShareEnvironment,
};
use cloud_backup_protocol::{
    v2::{contributions::MAX_CONTRIBUTION_SEQUENCE, Stream},
    Digest, ErrorCode, Id,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use std::collections::VecDeque;
use std::fmt;
use std::path::Path;
use x25519_dalek::PublicKey;

const MAX_REQUEST_BYTES: usize = 8 * 1024;
const MAX_GRANT_BYTES: usize = 4096;
const MAX_RECEIPTS: usize = 64;

#[derive(Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Outbox {
    through: u64,
    last: Option<Pending>,
    pending: Option<Pending>,
    #[serde(default, skip_serializing_if = "VecDeque::is_empty")]
    recent: VecDeque<Sent>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Sent {
    id: Id,
    fingerprint: String,
    sequence: u64,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Pending {
    id: Id,
    fingerprint: String,
    // The sent marker has no ciphertext. The pending entry keeps the exact bytes.
    ciphertext: Option<String>,
}

pub(crate) enum SubmitError {
    Invalid(String),
    TooLarge,
    Conflict(&'static str),
    Forbidden,
    Unavailable(String),
}

impl fmt::Display for SubmitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid(message) | Self::Unavailable(message) => f.write_str(message),
            Self::TooLarge => f.write_str("contribution exceeds its size limit"),
            Self::Conflict(message) => f.write_str(message),
            Self::Forbidden => f.write_str("contributor append permission was refused or expired"),
        }
    }
}

struct OpenGrant {
    invitation: Invitation,
    permission: GuestAppendPermission,
}

impl OpenGrant {
    fn open(identity_home: &Path, sealed: &[u8], publisher: &Digest) -> Result<Self, SubmitError> {
        if sealed.len() > MAX_GRANT_BYTES {
            return Err(SubmitError::TooLarge);
        }
        let recipient =
            share_publication::recipient(identity_home).map_err(SubmitError::Unavailable)?;
        let invitation: Invitation = crypto::open(
            sealed,
            recipient.encryption_secret(),
            publisher,
            crypto::GRANT,
        )
        .map_err(SubmitError::Invalid)?;
        let permission = GuestAppendPermission::parse(&invitation.authorization)
            .map_err(|error| SubmitError::Invalid(error.to_string()))?;
        let writer = recipient.contact().identity_public_key().clone();
        if invitation.writer != writer
            || permission.publisher_public_key() != publisher
            || permission.stream().stream_id
                != inbox_id(&invitation.publication, &writer).map_err(SubmitError::Invalid)?
            || invitation.version == 0
        {
            return Err(SubmitError::Invalid(
                "contributor invitation binding differs".into(),
            ));
        }
        Ok(Self {
            invitation,
            permission,
        })
    }

    fn require_publication(
        &self,
        publication: &Stream,
        environment: ShareEnvironment,
    ) -> Result<(), SubmitError> {
        if self.invitation.publication != publication.stream_id
            || self.permission.stream().scope != publication.scope
            || self.permission.environment() != environment
        {
            return Err(SubmitError::Forbidden);
        }
        Ok(())
    }

    fn require_live(&self) -> Result<(), SubmitError> {
        let now = u64::try_from(chrono::Utc::now().timestamp_millis()).unwrap_or_default();
        if self.permission.expires_ms() <= now {
            return Err(SubmitError::Forbidden);
        }
        Ok(())
    }
}

/// Verify a private append invitation against the registered read invitation.
pub fn validate_grant(
    identity_home: &Path,
    sealed: &[u8],
    publisher: &Digest,
    publication: &Stream,
    environment: ShareEnvironment,
) -> Result<(), String> {
    (|| {
        let grant = OpenGrant::open(identity_home, sealed, publisher)?;
        grant.require_publication(publication, environment)?;
        grant.require_live()
    })()
    .map_err(|error| error.to_string())
}

/// Save the exact signed envelope locally before uploading, so retries are safe.
pub(crate) async fn submit_registered(
    identity_home: &Path,
    sealed_grant: &[u8],
    publisher: &Digest,
    publication: &Stream,
    environment: ShareEnvironment,
    request: WriteRequest,
) -> Result<u64, SubmitError> {
    let grant = OpenGrant::open(identity_home, sealed_grant, publisher)?;
    grant.require_publication(publication, environment)?;
    append(identity_home, grant, request).await
}

async fn append(
    identity_home: &Path,
    grant: OpenGrant,
    request: WriteRequest,
) -> Result<u64, SubmitError> {
    let encoded = serde_json::to_vec(&request)
        .map_err(|_| SubmitError::Invalid("invalid contribution request".into()))?;
    if encoded.len() > MAX_REQUEST_BYTES {
        return Err(SubmitError::TooLarge);
    }
    let stream = grant.permission.stream().clone();
    let state = State::open(identity_home, "contributor-outbox", &stream.stream_id)
        .map_err(SubmitError::Unavailable)?;
    let (mut outbox, mut saved) = state.load::<Outbox>().map_err(SubmitError::Unavailable)?;
    let fingerprint = hex::encode(Sha256::digest(&encoded));
    let id = request.mutation_id.clone();
    let previous = outbox
        .last
        .as_ref()
        .filter(|last| last.id == id)
        .map(|last| (&last.fingerprint, outbox.through))
        .or_else(|| {
            outbox
                .recent
                .iter()
                .find(|entry| entry.id == id)
                .map(|entry| (&entry.fingerprint, entry.sequence))
        });
    if let Some((previous_fingerprint, sequence)) = previous {
        if previous_fingerprint != &fingerprint {
            return Err(SubmitError::Conflict("contribution mutation ID conflict"));
        }
        return Ok(sequence);
    }
    grant.require_live()?;
    let client = GuestContributionWriter::new(&grant.permission).map_err(transport_error)?;
    let sequence = outbox
        .through
        .checked_add(1)
        .filter(|sequence| *sequence <= MAX_CONTRIBUTION_SEQUENCE)
        .ok_or(SubmitError::Conflict("contribution sequence limit"))?;
    if let Some(pending) = &outbox.pending {
        if pending.id != id || pending.fingerprint != fingerprint {
            return Err(SubmitError::Conflict(
                "retry the pending contribution before submitting another",
            ));
        }
    } else {
        let signed = Submission {
            publication: grant.invitation.publication,
            stream,
            version: grant.invitation.version,
            sequence,
            request,
        };
        let seed =
            share_publication::owner_seed(identity_home).map_err(SubmitError::Unavailable)?;
        let signer = share_publication::signer(&seed).map_err(SubmitError::Unavailable)?;
        let ciphertext = URL_SAFE_NO_PAD.encode(
            crypto::seal(
                &signed,
                &PublicKey::from(grant.invitation.inbox_key),
                &signer,
                crypto::MUTATION,
            )
            .map_err(SubmitError::Unavailable)?,
        );
        outbox.pending = Some(Pending {
            id,
            fingerprint,
            ciphertext: Some(ciphertext),
        });
        saved = Some(
            state
                .save(&outbox, saved.as_deref().map(Vec::as_slice))
                .map_err(SubmitError::Unavailable)?,
        );
    }
    let mut pending = outbox
        .pending
        .take()
        .ok_or_else(|| SubmitError::Unavailable("missing pending contribution".into()))?;
    let ciphertext = pending
        .ciphertext
        .take()
        .ok_or_else(|| SubmitError::Unavailable("missing pending ciphertext".into()))?;
    client
        .append(sequence, &ciphertext)
        .await
        .map_err(transport_error)?;
    if let Some(previous) = outbox.last.replace(pending) {
        outbox.recent.push_back(Sent {
            id: previous.id,
            fingerprint: previous.fingerprint,
            sequence: outbox.through,
        });
        if outbox.recent.len() >= MAX_RECEIPTS {
            outbox.recent.pop_front();
        }
    }
    outbox.through = sequence;
    state
        .save(&outbox, saved.as_deref().map(Vec::as_slice))
        .map_err(SubmitError::Unavailable)?;
    Ok(sequence)
}

fn transport_error(error: BackupTransportError) -> SubmitError {
    match &error {
        BackupTransportError::Authentication | BackupTransportError::Http(401 | 403) => {
            SubmitError::Forbidden
        }
        BackupTransportError::Protocol(error) if error.code == ErrorCode::Unauthorized => {
            SubmitError::Forbidden
        }
        BackupTransportError::Protocol(error)
            if matches!(
                error.code,
                ErrorCode::IdentityConflict | ErrorCode::PredecessorConflict
            ) =>
        {
            SubmitError::Conflict("the private submission sequence conflicts with the cloud queue")
        }
        _ => SubmitError::Unavailable(error.to_string()),
    }
}
