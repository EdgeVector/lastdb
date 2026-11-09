//! Durable local submission and exact encrypted retries.
use super::{crypto, Invitation, State, Submission, WriteRequest};
use crate::share_publication;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use cloud_backup_client::byte_backup::{GuestAppendPermission, GuestContributionWriter};
use cloud_backup_protocol::{Digest, Id};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use std::path::Path;
use x25519_dalek::PublicKey;

const MAX_REQUEST_BYTES: usize = 8 * 1024;
const MAX_GRANT_BYTES: usize = 4096;

#[derive(Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Outbox {
    through: u64,
    last: Option<Pending>,
    pending: Option<Pending>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Pending {
    id: Id,
    fingerprint: String,
    // The sent marker has no ciphertext. The pending entry keeps the exact bytes.
    ciphertext: Option<String>,
}

/// Save the exact signed envelope locally before uploading, so retries are safe.
pub async fn submit(
    identity_home: &Path,
    sealed_grant: &[u8],
    publisher: &Digest,
    request: WriteRequest,
) -> Result<u64, String> {
    let encoded = serde_json::to_vec(&request).map_err(|_| "invalid contribution request")?;
    if encoded.len() > MAX_REQUEST_BYTES || sealed_grant.len() > MAX_GRANT_BYTES {
        return Err("contribution exceeds its size limit".into());
    }
    let recipient = share_publication::recipient(identity_home)?;
    let invitation: Invitation = crypto::open(
        sealed_grant,
        recipient.encryption_secret(),
        publisher,
        crypto::GRANT,
    )?;
    let permission =
        GuestAppendPermission::parse(&invitation.authorization).map_err(|e| e.to_string())?;
    if invitation.writer != *recipient.contact().identity_public_key()
        || permission.publisher_public_key() != publisher
        || invitation.version == 0
    {
        return Err("contributor invitation binding differs".into());
    }
    let stream = permission.stream().clone();
    let client = GuestContributionWriter::new(&permission).map_err(|e| e.to_string())?;
    let state = State::open(identity_home, "contributor-outbox", &stream.stream_id)?;
    let (mut outbox, mut saved) = state.load::<Outbox>()?;
    let fingerprint = hex::encode(Sha256::digest(&encoded));
    let id = request.mutation_id.clone();
    if let Some(last) = outbox.last.as_ref().filter(|last| last.id == id) {
        if last.fingerprint != fingerprint {
            return Err("contribution mutation ID conflict".into());
        }
        return Ok(outbox.through);
    }
    let sequence = outbox
        .through
        .checked_add(1)
        .ok_or("contribution sequence limit")?;
    if let Some(pending) = &outbox.pending {
        if pending.id != id || pending.fingerprint != fingerprint {
            return Err("retry the pending contribution before submitting another".into());
        }
    } else {
        let signed = Submission {
            publication: invitation.publication,
            stream,
            version: invitation.version,
            sequence,
            request,
        };
        let seed = share_publication::owner_seed(identity_home)?;
        let signer = share_publication::signer(&seed)?;
        let ciphertext = URL_SAFE_NO_PAD.encode(crypto::seal(
            &signed,
            &PublicKey::from(invitation.inbox_key),
            &signer,
            crypto::MUTATION,
        )?);
        outbox.pending = Some(Pending {
            id,
            fingerprint,
            ciphertext: Some(ciphertext),
        });
        saved = Some(state.save(&outbox, saved.as_deref().map(Vec::as_slice))?);
    }
    let mut pending = outbox
        .pending
        .take()
        .ok_or("missing pending contribution")?;
    let ciphertext = pending
        .ciphertext
        .take()
        .ok_or("missing pending ciphertext")?;
    client
        .append(sequence, &ciphertext)
        .await
        .map_err(|e| e.to_string())?;
    outbox.through = sequence;
    outbox.last = Some(pending);
    state.save(&outbox, saved.as_deref().map(Vec::as_slice))?;
    Ok(sequence)
}
