//! Build and check one member's signed entry. Regrant is explicit.

use super::{MemberIntent, PublisherSession};
use cloud_backup_protocol::v2::share_access::{
    Ed25519Signature, MemberEntry, ShareMemberRole, ShareMemberState, ShareWriteScope,
};
use cloud_backup_protocol::v2::{Stream, Version};
use cloud_backup_protocol::{DecimalU64, Digest, Id};

/// What an entry gives its member, without identity, sequence, or signature.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Grant {
    pub role: ShareMemberRole,
    pub state: ShareMemberState,
    pub write_schemas: Option<ShareWriteScope>,
}

impl Grant {
    fn of(entry: &MemberEntry) -> Self {
        Self {
            role: entry.role,
            state: entry.state,
            write_schemas: entry.write_schemas.clone(),
        }
    }
}

pub(super) fn stream(session: &PublisherSession, database_id: &str) -> Result<Stream, String> {
    Ok(Stream {
        scope: session.scope.clone(),
        stream_id: Id::try_from(database_id.to_owned())
            .map_err(|_| "database registry is unavailable")?,
    })
}

/// Accept only an entry this publisher signed for this database.
///
/// The service stores entries. This check is what makes one trustworthy.
pub(super) fn verify(
    session: &PublisherSession,
    database_id: &str,
    entry: &MemberEntry,
) -> Result<(), String> {
    if entry.environment != session.environment
        || entry.stream.stream_id.as_str() != database_id
        || entry.stream.scope != session.scope
        || entry.publisher_public_key != session.publisher
    {
        return Err("share.grant.identity".into());
    }
    let key = app_identity_crypto::VerifyingKey::from_bytes(&session.signer.public_key_bytes())
        .map_err(|_| "share.grant.identity")?;
    let signature =
        fold_db::hex::hex_decode_array(entry.signature.as_str()).ok_or("share.signature")?;
    let digest = entry.digest().map_err(|error| error.field)?;
    app_identity_crypto::verify(&key, &signature, digest.as_str().as_bytes())
        .map_err(|_| "share.signature".into())
}

/// The grant one member command asks for. `None` when the entry has it already.
pub(super) fn member_step(
    current: Option<&MemberEntry>,
    intent: MemberIntent,
) -> Result<Option<Grant>, String> {
    let active = |role: ShareMemberRole| Grant {
        role,
        state: ShareMemberState::Active,
        write_schemas: None,
    };
    let (grant, regrant) = match intent {
        MemberIntent::Read { regrant } => (active(ShareMemberRole::Read), regrant),
        MemberIntent::Submit { regrant } => (active(ShareMemberRole::Submit), regrant),
        MemberIntent::Revoke => {
            let current = current.ok_or("share.grant.revoke_unknown")?;
            if current.state == ShareMemberState::Revoked {
                return Ok(None);
            }
            return Ok(Some(Grant {
                role: current.role,
                state: ShareMemberState::Revoked,
                write_schemas: None,
            }));
        }
    };
    match (current.map(|entry| entry.state), regrant) {
        (Some(ShareMemberState::Revoked), false) => Err("share.grant.regrant_required".into()),
        // An active entry with the asked role is the asked result, with or
        // without `regrant`: a repeat of a restore whose answer was lost
        // finds the restore stored.
        (Some(ShareMemberState::Active), _)
            if current.is_some_and(|entry| entry.role == grant.role) =>
        {
            Ok(None)
        }
        (Some(ShareMemberState::Active) | None, true) => Err("share.grant.regrant".into()),
        _ => Ok(Some(grant)),
    }
}

/// The grant a whole-configuration write asks for one member. `None` when the
/// entry has it already. A revoked key returns only with `regrant`.
pub(super) fn config_step(
    current: Option<&MemberEntry>,
    desired: Grant,
    regrant: bool,
) -> Result<Option<Grant>, String> {
    let Some(current) = current else {
        return match (desired.state, regrant) {
            (ShareMemberState::Active, false) => Ok(Some(desired)),
            (ShareMemberState::Revoked, _) => Err("share.grant.revoke_unknown".into()),
            (ShareMemberState::Active, true) => Err("share.grant.regrant".into()),
        };
    };
    // A stored entry equal to the asked one is the asked result, whatever
    // `regrant` says: a repeat of a restore whose answer was lost finds it.
    if Grant::of(current) == desired {
        return Ok(None);
    }
    match (current.state, desired.state) {
        (ShareMemberState::Active, ShareMemberState::Active) if !regrant => {}
        (_, ShareMemberState::Revoked) if !regrant && desired.role == current.role => {}
        (ShareMemberState::Revoked, ShareMemberState::Active) if regrant => {}
        (ShareMemberState::Revoked, ShareMemberState::Active) => {
            return Err("share.grant.regrant_required".into())
        }
        (ShareMemberState::Active, ShareMemberState::Revoked) => {
            return Err("share.grant.revoke".into())
        }
        (ShareMemberState::Revoked, ShareMemberState::Revoked) => {
            return Err("share.grant.revoked_state".into())
        }
        (ShareMemberState::Active, ShareMemberState::Active) => {
            return Err("share.grant.regrant".into())
        }
    }
    Ok(Some(desired))
}

/// Sign the entry after `current` for `node`.
pub(super) fn sign(
    session: &PublisherSession,
    database_id: &str,
    node: &Digest,
    current: Option<&MemberEntry>,
    grant: Grant,
) -> Result<MemberEntry, String> {
    let sequence = current
        .map_or(Some(1), |entry| entry.sequence.get().checked_add(1))
        .ok_or("share.member.sequence")?;
    let now = fold_db::clock::unix_millis();
    if now == 0 {
        return Err("invalid clock for member update".into());
    }
    let mut entry = MemberEntry {
        version: Version::V2,
        environment: session.environment,
        stream: stream(session, database_id)?,
        node_public_key: node.clone(),
        role: grant.role,
        state: grant.state,
        write_schemas: grant.write_schemas,
        sequence: DecimalU64::new(sequence),
        publisher_public_key: session.publisher.clone(),
        signed_at_ms: DecimalU64::new(now),
        signature: Ed25519Signature::try_from("0".repeat(128)).map_err(|_| "share.signature")?,
    };
    entry
        .validate_successor_of(current)
        .map_err(|error| error.field)?;
    let digest = entry.digest().map_err(|error| error.field)?;
    let signature = session.signer.sign(digest.as_str().as_bytes()).to_bytes();
    entry.signature = Ed25519Signature::try_from(fold_db::hex::hex_lower(signature))
        .map_err(|_| "share.signature")?;
    Ok(entry)
}
