//! Successor rules for a publisher-signed member list.
//!
//! The signed document is the whole member set. A revoked key stays on that
//! set until a later revision marks it active and sets `regrant`.

use super::receipt::SignedRequestTarget;
use super::{
    account_key_matches, is_mutation_log_path, MemberListReadResponse, MemberListRevision,
    MemberListUpdate, NodeReadToken, ShareHttpMethod, ShareMember, ShareMemberRole,
    ShareMemberState, SHARE_MEMBER_LIST_READ_PATH,
};
use crate::{Digest, ProtocolError};
use std::cmp::Ordering;

/// What a node key may do with the current member list.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ShareAccess {
    /// Account key for this database. It is not a member row.
    Publisher,
    /// Active `read` member.
    Reader,
    /// Active `submit` member. Reader rights plus pending submissions.
    Contributor,
}

/// Body of a member-list read after the current grant allows the caller.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MemberListReadView {
    /// Publisher named the stored revision. Return the stored member document.
    Document,
    /// Authorized caller. Return [`MemberListRevision`] and no members.
    RevisionOnly,
}

/// Accept `next` only as the revision immediately after `previous`.
pub fn validate_successor(
    previous: Option<&MemberListUpdate>,
    next: &MemberListUpdate,
) -> Result<(), ProtocolError> {
    next.validate()?;
    let Some(previous) = previous else {
        if next.revision.get() != 1 {
            return Err(ProtocolError::invalid("share.grant.revision"));
        }
        return require_initial(next);
    };
    previous.validate()?;
    if next.stream != previous.stream || next.publisher_public_key != previous.publisher_public_key
    {
        return Err(ProtocolError::invalid("share.grant.identity"));
    }
    let Some(expected) = previous.revision.get().checked_add(1) else {
        return Err(ProtocolError::invalid("share.grant.revision"));
    };
    if next.revision.get() != expected {
        return Err(ProtocolError::invalid("share.grant.revision"));
    }
    walk(&previous.members, &next.members)
}

/// Resolve the current grant for `node`.
///
/// The account key is the publisher even when no list is stored. A revoked
/// row and a missing row are unauthorized. One node key covers every device
/// that signs with it. Revoking the key refuses all of those devices.
/// `DeviceCursor` is not a second grant.
pub fn authorize(
    current: &MemberListReadResponse,
    node: &Digest,
) -> Result<ShareAccess, ProtocolError> {
    current.stream.validate()?;
    if let Some(update) = &current.update {
        update.validate()?;
        if update.stream != current.stream || update.revision != current.revision {
            return Err(ProtocolError::invalid("share.member_list.read.identity"));
        }
    } else if current.revision.get() != 0 {
        return Err(ProtocolError::invalid("share.member_list.read.update"));
    }
    if account_key_matches(node, &current.stream.scope.account_id).is_ok() {
        return Ok(ShareAccess::Publisher);
    }
    let Some(update) = &current.update else {
        return Err(ProtocolError::unauthorized("share.grant.absent"));
    };
    match member_role(update, node)? {
        ShareMemberRole::Read => Ok(ShareAccess::Reader),
        ShareMemberRole::Submit => Ok(ShareAccess::Contributor),
    }
}

/// Reject a token whose signed stream is not the stored stream.
///
/// The caller verifies the signature with `token.node_public_key`.
/// That key is not the publisher key. This function does not verify it.
pub fn bind_request_scope(
    token: &NodeReadToken,
    current: &MemberListReadResponse,
) -> Result<(), ProtocolError> {
    if token.database_id != token.stream.stream_id || token.stream != current.stream {
        return Err(ProtocolError::unauthorized("share.node_read.scope"));
    }
    Ok(())
}

/// Authorize a mutation-log request against the current grant.
///
/// The signature check and the nonce spend happen before this call. They are
/// not the grant check. The database must already be native or committed.
/// The signed revision must be the stored revision. The member-list read does
/// not use this function.
pub fn authorize_request(
    current: &MemberListReadResponse,
    token: &NodeReadToken,
    target: SignedRequestTarget<'_>,
) -> Result<ShareAccess, ProtocolError> {
    require_signed(token, &target)?;
    if !is_mutation_log_path(target.path) {
        return Err(ProtocolError::invalid("share.node_read.route"));
    }
    token.require_target(target.environment, target.method, target.path)?;
    bind_request_scope(token, current)?;
    if token.member_list_revision != current.revision {
        return Err(ProtocolError::unauthorized("share.node_read.revision"));
    }
    authorize(current, &token.node_public_key)
}

/// Choose the member-list read body.
///
/// The database must already be native or committed. A stale revision does not
/// reject this route. Only a publisher who named the stored revision receives
/// the member document. Every other authorized node receives the revision
/// alone. An unauthorized node receives neither.
pub fn member_list_read_view(
    current: &MemberListReadResponse,
    token: &NodeReadToken,
    target: SignedRequestTarget<'_>,
) -> Result<MemberListReadView, ProtocolError> {
    require_signed(token, &target)?;
    if target.method != ShareHttpMethod::Post || target.path != SHARE_MEMBER_LIST_READ_PATH {
        return Err(ProtocolError::invalid("share.node_read.route"));
    }
    token.require_target(target.environment, target.method, target.path)?;
    bind_request_scope(token, current)?;
    let access = authorize(current, &token.node_public_key)?;
    if access == ShareAccess::Publisher && token.member_list_revision == current.revision {
        return Ok(MemberListReadView::Document);
    }
    Ok(MemberListReadView::RevisionOnly)
}

fn require_signed(
    token: &NodeReadToken,
    target: &SignedRequestTarget<'_>,
) -> Result<(), ProtocolError> {
    let Some(state) = target.access else {
        return Err(ProtocolError::unauthorized("share.cutover.required"));
    };
    state.bind(token, target.environment)
}

/// Revision object for [`MemberListReadView::RevisionOnly`]. It names no members.
#[must_use]
pub fn member_list_revision(current: &MemberListReadResponse) -> MemberListRevision {
    MemberListRevision {
        version: current.version,
        database_id: current.stream.stream_id.clone(),
        revision: current.revision,
    }
}

fn require_initial(next: &MemberListUpdate) -> Result<(), ProtocolError> {
    if next.members.iter().any(|member| !fresh_grant(member)) {
        return Err(ProtocolError::invalid("share.grant.initial"));
    }
    Ok(())
}

fn fresh_grant(member: &ShareMember) -> bool {
    member.state == ShareMemberState::Active && !member.regrant
}

fn walk(previous: &[ShareMember], next: &[ShareMember]) -> Result<(), ProtocolError> {
    let mut left = 0;
    let mut right = 0;
    while left < previous.len() || right < next.len() {
        if left < previous.len() && right < next.len() {
            match previous[left]
                .node_public_key
                .cmp(&next[right].node_public_key)
            {
                Ordering::Equal => {
                    transition(&previous[left], &next[right])?;
                    left += 1;
                    right += 1;
                }
                Ordering::Less => {
                    omitted(&previous[left])?;
                    left += 1;
                }
                Ordering::Greater => {
                    introduced(&next[right])?;
                    right += 1;
                }
            }
        } else if left < previous.len() {
            omitted(&previous[left])?;
            left += 1;
        } else {
            introduced(&next[right])?;
            right += 1;
        }
    }
    Ok(())
}

fn omitted(member: &ShareMember) -> Result<(), ProtocolError> {
    let field = match member.state {
        ShareMemberState::Active => "share.grant.omitted",
        ShareMemberState::Revoked => "share.grant.revoked_omitted",
    };
    Err(ProtocolError::invalid(field))
}

fn introduced(member: &ShareMember) -> Result<(), ProtocolError> {
    if fresh_grant(member) {
        return Ok(());
    }
    let field = if member.state == ShareMemberState::Revoked {
        "share.grant.revoke_unknown"
    } else {
        "share.grant.regrant"
    };
    Err(ProtocolError::invalid(field))
}

fn transition(previous: &ShareMember, next: &ShareMember) -> Result<(), ProtocolError> {
    match (previous.state, next.state) {
        (ShareMemberState::Active, ShareMemberState::Active) if !next.regrant => Ok(()),
        (ShareMemberState::Active, ShareMemberState::Revoked)
            if !next.regrant && next.role == previous.role =>
        {
            Ok(())
        }
        (ShareMemberState::Revoked, ShareMemberState::Revoked)
            if !next.regrant && next.role == previous.role =>
        {
            Ok(())
        }
        (ShareMemberState::Revoked, ShareMemberState::Active) if next.regrant => Ok(()),
        (ShareMemberState::Revoked, ShareMemberState::Active) => {
            Err(ProtocolError::invalid("share.grant.regrant_required"))
        }
        (ShareMemberState::Active, ShareMemberState::Revoked) => {
            Err(ProtocolError::invalid("share.grant.revoke"))
        }
        (ShareMemberState::Revoked, ShareMemberState::Revoked) => {
            Err(ProtocolError::invalid("share.grant.revoked_state"))
        }
        (ShareMemberState::Active, ShareMemberState::Active) => {
            Err(ProtocolError::invalid("share.grant.regrant"))
        }
    }
}

fn member_role(update: &MemberListUpdate, node: &Digest) -> Result<ShareMemberRole, ProtocolError> {
    let index = update
        .members
        .binary_search_by(|member| member.node_public_key.cmp(node))
        .map_err(|_| ProtocolError::unauthorized("share.grant.absent"))?;
    let member = &update.members[index];
    match member.state {
        ShareMemberState::Active => Ok(member.role),
        ShareMemberState::Revoked => Err(ProtocolError::unauthorized("share.grant.revoked")),
    }
}
