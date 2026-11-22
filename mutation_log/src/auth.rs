//! Publisher authorization and grant-window checks (AcceptEntry step 6).

use crate::accept::RejectReason;
use crate::entry::{BundleBody, DataBody, EntryHeader, SignedEntry};
use crate::grants::{past_cutoff, AcceptedMeta, GrantState};
use crate::ids::{DeviceId, EntryHash, NodeKey};
use crate::kinds::{Role, StreamKind};

/// Authorize a bundle entry (direct write / grant window / cutoffs like data).
pub fn authorize_bundle<'s>(
    entry: &SignedEntry,
    body: &BundleBody,
    owner: Option<NodeKey>,
    grants: &GrantState,
    lookup: &dyn Fn(&EntryHash) -> Option<&'s AcceptedMeta>,
) -> Option<RejectReason> {
    let h = entry.header();
    if owner == Some(h.node_key) {
        if body.grant_at_device.is_some() || body.grant_at_seq.is_some() {
            return Some(RejectReason::Malformed(
                "publisher bundle must omit grant_at",
            ));
        }
        return None;
    }
    let Some(grant_device) = body.grant_at_device else {
        return Some(RejectReason::Unauthorized(
            "member bundle requires grant_at",
        ));
    };
    let Some(grant_seq) = body.grant_at_seq else {
        return Some(RejectReason::Unauthorized(
            "member bundle requires grant_at",
        ));
    };
    if !seen_names_grant(&h.seen_heads, grant_device, grant_seq, lookup) {
        return Some(RejectReason::Unauthorized(
            "grant_at must name a publisher grant in seen_heads",
        ));
    }
    let window = GrantState::fold_window_from_seen(&h.seen_heads, |hash| lookup(hash));
    match window.role_of(&h.node_key) {
        Some(Role::Write) => {}
        Some(_) => {
            return Some(RejectReason::Unauthorized(
                "member bundle requires write role at grant window",
            ))
        }
        None => {
            return Some(RejectReason::Unauthorized(
                "non-grantee bundle write rejected",
            ))
        }
    }
    check_cutoff(h, grants)
}

/// Authorize a data entry using the entry's `seen_heads` and S cutoffs.
pub fn authorize_data<'s>(
    entry: &SignedEntry,
    body: &DataBody,
    owner: Option<NodeKey>,
    grants: &GrantState,
    lookup: &dyn Fn(&EntryHash) -> Option<&'s AcceptedMeta>,
) -> Option<RejectReason> {
    let h = entry.header();
    if owner == Some(h.node_key) {
        return None;
    }
    match body.stream {
        StreamKind::Direct => authorize_direct(h, body, grants, lookup),
        StreamKind::Proposal => authorize_proposal(h, body, grants, lookup),
    }
}

fn authorize_direct<'s>(
    h: &EntryHeader,
    body: &DataBody,
    grants: &GrantState,
    lookup: &dyn Fn(&EntryHash) -> Option<&'s AcceptedMeta>,
) -> Option<RejectReason> {
    if body.incorporated_from.is_some() {
        return Some(RejectReason::Malformed(
            "non-publisher direct must not carry incorporated_from",
        ));
    }
    let Some(grant_device) = body.grant_at_device else {
        return Some(RejectReason::Unauthorized(
            "member direct requires grant_at",
        ));
    };
    let Some(grant_seq) = body.grant_at_seq else {
        return Some(RejectReason::Unauthorized(
            "member direct requires grant_at",
        ));
    };
    if !seen_names_grant(&h.seen_heads, grant_device, grant_seq, lookup) {
        return Some(RejectReason::Unauthorized(
            "grant_at must name a publisher grant in seen_heads",
        ));
    }
    let window = GrantState::fold_window_from_seen(&h.seen_heads, |h| lookup(h));
    match window.role_of(&h.node_key) {
        Some(Role::Write) => {}
        Some(_) => {
            return Some(RejectReason::Unauthorized(
                "member direct requires write role at grant window",
            ))
        }
        None => {
            return Some(RejectReason::Unauthorized(
                "non-grantee direct write rejected",
            ))
        }
    }
    check_cutoff(h, grants)
}

fn authorize_proposal<'s>(
    h: &EntryHeader,
    body: &DataBody,
    grants: &GrantState,
    lookup: &dyn Fn(&EntryHash) -> Option<&'s AcceptedMeta>,
) -> Option<RejectReason> {
    if body.grant_at_device.is_some() || body.grant_at_seq.is_some() {
        return Some(RejectReason::Malformed("proposal must not carry grant_at"));
    }
    if body.incorporated_from.is_some() {
        return Some(RejectReason::Malformed(
            "non-publisher proposal must not carry incorporated_from",
        ));
    }
    let window = GrantState::fold_window_from_seen(&h.seen_heads, |h| lookup(h));
    match window.role_of(&h.node_key) {
        Some(Role::Submit | Role::Write) => {}
        _ => {
            return Some(RejectReason::Unauthorized(
                "proposal requires submit or write grant",
            ))
        }
    }
    check_cutoff(h, grants)
}

/// True while a non-owner entry still falls inside its writer's window: not
/// past the cutoff of a later role reduction. A later reduction can void an
/// entry that was accepted earlier.
#[must_use]
pub(crate) fn within_cutoff(entry: &SignedEntry, grants: &GrantState) -> bool {
    check_cutoff(entry.header(), grants).is_none()
}

fn check_cutoff(h: &EntryHeader, grants: &GrantState) -> Option<RejectReason> {
    // After a reduction, missing device tips are cutoff 0 (unseen → reject seq≥1).
    let tip = grants.cutoff(&h.node_key, &h.device_id)?;
    if past_cutoff(h.writer_seq, tip) {
        Some(RejectReason::Unauthorized(
            "write past role-reduction cutoff",
        ))
    } else {
        None
    }
}

fn seen_names_grant<'s>(
    seen: &[EntryHash],
    device: DeviceId,
    seq: u64,
    lookup: &dyn Fn(&EntryHash) -> Option<&'s AcceptedMeta>,
) -> bool {
    seen.iter().any(|h| {
        let Some(meta) = lookup(h) else {
            return false;
        };
        if !meta.is_publisher || meta.grant_body.is_none() {
            return false;
        }
        let hdr = meta.entry.header();
        hdr.device_id == device && hdr.writer_seq == seq
    })
}
