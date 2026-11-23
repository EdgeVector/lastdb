//! Who may sign what (AcceptEntry step 6).
//!
//! Only the publisher signs entries that change a database: direct data,
//! bundles, and the publisher kinds. A member signs proposals. A proposal has
//! no effect until the publisher incorporates it, so a member's grant decides
//! only whether it may propose.

use crate::accept::RejectReason;
use crate::entry::{BundleBody, DataBody, EntryHeader, LeaseBody, SignedEntry};
use crate::grants::{AcceptedMeta, GrantState};
use crate::ids::{EntryHash, NodeKey};
use crate::kinds::{EntryKind, Role, StreamKind};

/// Authorize a bundle entry: the publisher's alone.
pub fn authorize_bundle(
    entry: &SignedEntry,
    body: &BundleBody,
    owner: Option<NodeKey>,
) -> Option<RejectReason> {
    if owner != Some(entry.header().node_key) {
        return Some(RejectReason::Unauthorized(
            "only the publisher signs bundle entries",
        ));
    }
    if body.grant_at_device.is_some() || body.grant_at_seq.is_some() {
        return Some(RejectReason::Malformed(
            "publisher bundle must omit grant_at",
        ));
    }
    None
}

/// Authorize a data entry: a direct entry is the publisher's, or a member's
/// that names a lease covering it; a proposal needs a submit or write grant.
pub fn authorize_data<'s>(
    entry: &SignedEntry,
    body: &DataBody,
    owner: Option<NodeKey>,
    lookup: &dyn Fn(&EntryHash) -> Option<&'s AcceptedMeta>,
    leases_held: bool,
) -> Option<RejectReason> {
    let h = entry.header();
    if owner == Some(h.node_key) {
        return None;
    }
    match body.stream {
        StreamKind::Direct => authorize_member_direct(h, body, lookup, leases_held),
        StreamKind::Proposal => authorize_proposal(h, body, lookup),
    }
}

/// A member's direct entry names its lease by citing it in `seen_heads`: the
/// entry cites exactly one lease entry, and the member held the `write` role
/// as of that lease. Whether the entry counts is the history's counting rule.
fn authorize_member_direct<'s>(
    h: &EntryHeader,
    body: &DataBody,
    lookup: &dyn Fn(&EntryHash) -> Option<&'s AcceptedMeta>,
    leases_held: bool,
) -> Option<RejectReason> {
    if body.grant_at_device.is_some() || body.grant_at_seq.is_some() {
        return Some(RejectReason::Malformed(
            "member direct entry must not carry grant_at",
        ));
    }
    if body.incorporated_from.is_some() {
        return Some(RejectReason::Malformed(
            "member direct entry must not carry incorporated_from",
        ));
    }
    let mut leases = h
        .seen_heads
        .iter()
        .filter(|hash| lookup(hash).is_some_and(|meta| meta.entry.kind() == EntryKind::Lease));
    let (Some(_), None) = (leases.next(), leases.next()) else {
        return Some(RejectReason::Malformed(
            "member direct entry must cite exactly one lease",
        ));
    };
    if leases_held {
        None
    } else {
        Some(RejectReason::Unauthorized(
            "member direct entry needs a history that holds leases",
        ))
    }
}

fn authorize_proposal<'s>(
    h: &EntryHeader,
    body: &DataBody,
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
    // Acceptance into the member's stream depends only on the grants cited in
    // `seen_heads`, keeping `AcceptEntry` causal and order-independent. Whether
    // a proposal changes a record is decided by the publisher against current
    // folded grants.
    let window = GrantState::fold_window_from_seen(&h.seen_heads, |h| lookup(h));
    match window.role_of(&h.node_key) {
        Some(Role::Submit | Role::Write) => None,
        _ => Some(RejectReason::Unauthorized(
            "proposal requires submit or write grant",
        )),
    }
}

/// Public (`any`) grants may carry `read` or `revoked` only.
pub fn check_grant(body: &crate::entry::GrantBody) -> Option<RejectReason> {
    if body.grantee == crate::kinds::Grantee::Any
        && !matches!(body.role, Role::Read | Role::Revoked)
    {
        return Some(RejectReason::Malformed(
            "any grant role must be read or revoked",
        ));
    }
    None
}

/// A lease must close later than it expires.
pub fn check_lease(body: &LeaseBody) -> Option<RejectReason> {
    if body.closes_at_ms <= body.expires_at_ms {
        return Some(RejectReason::Malformed(
            "lease closes_at_ms must be later than expires_at_ms",
        ));
    }
    None
}
