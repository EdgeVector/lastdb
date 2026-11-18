//! Member-list successor walk. A revoked key stays until a later revision regrants it.

use super::{MemberListUpdate, ShareMember, ShareMemberRole, ShareMemberState};
use crate::{Digest, ProtocolError};
use std::cmp::Ordering;

pub(super) fn require_initial(next: &MemberListUpdate) -> Result<(), ProtocolError> {
    if next.members.iter().any(|member| !fresh_grant(member)) {
        return Err(ProtocolError::invalid("share.grant.initial"));
    }
    Ok(())
}

fn fresh_grant(member: &ShareMember) -> bool {
    member.state == ShareMemberState::Active && !member.regrant
}

pub(super) fn walk(previous: &[ShareMember], next: &[ShareMember]) -> Result<(), ProtocolError> {
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
        (_, ShareMemberState::Revoked) if !next.regrant && next.role == previous.role => Ok(()),
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

pub(super) fn member_role(
    update: &MemberListUpdate,
    node: &Digest,
) -> Result<ShareMemberRole, ProtocolError> {
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
