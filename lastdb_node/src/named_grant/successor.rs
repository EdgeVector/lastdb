//! Build the next whole member set. Revoked rows stay. Regrant is explicit.

use super::session::PublisherSession;
use super::MemberAck;
use super::MemberIntent;
use cloud_backup_protocol::v2::share_access::{
    Ed25519Signature, MemberListReadResponse, MemberListUpdate, ShareMember, ShareMemberRole,
    ShareMemberState,
};
use cloud_backup_protocol::v2::Version;
use cloud_backup_protocol::{DecimalU64, Digest};

/// Service transaction limit: 100 items, minus the head check and the document.
const MEMBER_LIMIT: usize = 98;

pub(super) struct SignedUpdate {
    pub body: Vec<u8>,
    pub revision: u64,
}

pub(super) fn require_stream(
    document: &MemberListReadResponse,
    session: &PublisherSession,
    database_id: &str,
) -> Result<(), String> {
    document
        .validate_for(
            &cloud_backup_protocol::v2::share_access::MemberListReadRequest {
                version: Version::V2,
                stream: document.stream.clone(),
            },
        )
        .map_err(|error| error.field)?;
    if document.stream.stream_id.as_str() != database_id || document.stream.scope != session.scope {
        return Err("share.grant.identity".into());
    }
    if let Some(update) = &document.update {
        if update.publisher_public_key != session.publisher {
            return Err("share.grant.identity".into());
        }
        let key = app_identity_crypto::VerifyingKey::from_bytes(&session.signer.public_key_bytes())
            .map_err(|_| "share.grant.identity")?;
        let signature = hex::decode(update.signature.as_str())
            .ok()
            .and_then(|bytes| bytes.try_into().ok())
            .ok_or("share.signature")?;
        let digest = update.digest().map_err(|error| error.field)?;
        app_identity_crypto::verify(&key, &signature, digest.as_str().as_bytes())
            .map_err(|_| "share.signature")?;
    }
    Ok(())
}

pub(super) fn unchanged(
    document: &MemberListReadResponse,
    database_id: &str,
    node: &Digest,
    intent: MemberIntent,
) -> Result<Option<MemberAck>, String> {
    let current = member(document, node);
    if !already(&current, intent)? {
        return Ok(None);
    }
    Ok(Some(MemberAck {
        database_id: database_id.to_owned(),
        revision: document.revision.get(),
        unchanged: true,
    }))
}

pub(super) fn sign(
    session: &PublisherSession,
    document: &MemberListReadResponse,
    node: &Digest,
    intent: MemberIntent,
) -> Result<SignedUpdate, String> {
    let mut members = current_members(document);
    let next = next_member(
        members
            .iter()
            .find(|member| &member.node_public_key == node),
        node,
        intent,
    )?;
    upsert(&mut members, next);
    sign_members(session, document, members)
}

pub(super) fn current_members(document: &MemberListReadResponse) -> Vec<ShareMember> {
    let mut members = document
        .update
        .as_ref()
        .map(|update| update.members.clone())
        .unwrap_or_default();
    for member in &mut members {
        member.regrant = false;
    }
    members
}

pub(super) fn sign_members(
    session: &PublisherSession,
    document: &MemberListReadResponse,
    mut members: Vec<ShareMember>,
) -> Result<SignedUpdate, String> {
    if members.len() > MEMBER_LIMIT {
        return Err("mutation_log.transaction".into());
    }
    members.sort_by(|left, right| left.node_public_key.cmp(&right.node_public_key));
    let revision = document
        .revision
        .get()
        .checked_add(1)
        .ok_or("share.grant.revision")?;
    let mut update = MemberListUpdate {
        version: Version::V2,
        stream: document.stream.clone(),
        revision: DecimalU64::new(revision),
        members,
        publisher_public_key: session.publisher.clone(),
        signature: Ed25519Signature::try_from("0".repeat(128)).map_err(|_| "share.signature")?,
        updated_at_ms: DecimalU64::new(nonzero_now()?),
    };
    update
        .validate_successor_of(document.update.as_ref())
        .map_err(|error| error.field)?;
    let digest = update.digest().map_err(|error| error.field)?;
    let signature = session.signer.sign(digest.as_str().as_bytes()).to_bytes();
    update.signature =
        Ed25519Signature::try_from(hex::encode(signature)).map_err(|_| "share.signature")?;
    let body = serde_json::to_vec(&update).map_err(|_| "cannot encode member update")?;
    Ok(SignedUpdate { body, revision })
}

fn already(current: &Option<&ShareMember>, intent: MemberIntent) -> Result<bool, String> {
    match (intent, *current) {
        (MemberIntent::Read { regrant: false }, Some(member)) => {
            active_match(member, ShareMemberRole::Read)
        }
        (MemberIntent::Submit { regrant: false }, Some(member)) => {
            active_match(member, ShareMemberRole::Submit)
        }
        (MemberIntent::Read { regrant: false } | MemberIntent::Submit { regrant: false }, None) => {
            Ok(false)
        }
        (MemberIntent::Read { regrant: true } | MemberIntent::Submit { regrant: true }, member) => {
            refuse_regrant(member)
        }
        (MemberIntent::Revoke, None) => Err("share.grant.revoke_unknown".into()),
        (MemberIntent::Revoke, Some(member)) => Ok(member.state == ShareMemberState::Revoked),
    }
}

fn active_match(member: &ShareMember, role: ShareMemberRole) -> Result<bool, String> {
    match member.state {
        ShareMemberState::Active => Ok(member.role == role),
        ShareMemberState::Revoked => Err("share.grant.regrant_required".into()),
    }
}

fn refuse_regrant(member: Option<&ShareMember>) -> Result<bool, String> {
    match member.map(|member| member.state) {
        Some(ShareMemberState::Revoked) => Ok(false),
        Some(ShareMemberState::Active) | None => Err("share.grant.regrant".into()),
    }
}

fn next_member(
    current: Option<&ShareMember>,
    node: &Digest,
    intent: MemberIntent,
) -> Result<ShareMember, String> {
    let (role, state, regrant) = match intent {
        MemberIntent::Read { regrant } => {
            (ShareMemberRole::Read, ShareMemberState::Active, regrant)
        }
        MemberIntent::Submit { regrant } => {
            (ShareMemberRole::Submit, ShareMemberState::Active, regrant)
        }
        MemberIntent::Revoke => {
            let role = current.ok_or("share.grant.revoke_unknown")?.role;
            (role, ShareMemberState::Revoked, false)
        }
    };
    Ok(ShareMember {
        node_public_key: node.clone(),
        role,
        state,
        regrant,
    })
}

fn upsert(members: &mut Vec<ShareMember>, member: ShareMember) {
    if let Some(existing) = members
        .iter_mut()
        .find(|existing| existing.node_public_key == member.node_public_key)
    {
        *existing = member;
    } else {
        members.push(member);
    }
}

fn member<'a>(document: &'a MemberListReadResponse, node: &Digest) -> Option<&'a ShareMember> {
    document.update.as_ref().and_then(|update| {
        update
            .members
            .iter()
            .find(|member| &member.node_public_key == node)
    })
}

fn nonzero_now() -> Result<u64, String> {
    let now = fold_db::clock::unix_millis();
    if now == 0 {
        return Err("invalid clock for member update".into());
    }
    Ok(now)
}
