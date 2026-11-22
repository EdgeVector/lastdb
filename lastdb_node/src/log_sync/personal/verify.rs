//! Check one entry of another device's stream before it is applied.
//!
//! The check is `AcceptEntry` of `mutation_log`, with a context made of this
//! node's own rows: the stream head, the owner (this node key), and the birth,
//! which is the only entry that a personal entry cites. The owner is the only
//! writer, so there are no grants and no proposals.

use mutation_log::{
    evaluate, AcceptContext, AcceptOutcome, EntryHash, GrantState, SignedEntry, StreamHead,
    StreamKind,
};

use super::Ctx;

/// What the check says about an entry.
pub(super) enum Verdict {
    /// The entry follows the head and is valid. `hash` is its entry hash.
    Next { hash: EntryHash },
    /// The entry is not valid, and no entry of the stream after it can be
    /// trusted. The stream is frozen.
    Refused(String),
}

/// Check `entry`, which must be the entry after `head` on the stream of
/// `device` (hex).
pub(super) fn check(
    ctx: &Ctx<'_>,
    device: &str,
    head: Option<StreamHead>,
    entry: &SignedEntry,
) -> Result<Verdict, String> {
    let header = entry.header();
    if header.node_key != ctx.identity.node_key || header.device_id.to_hex() != device {
        return Ok(refused(
            "an entry names another stream than it was read from",
        ));
    }
    if !matches!(entry, SignedEntry::Data { body, .. } if body.stream == StreamKind::Direct) {
        return Ok(refused(
            "an entry of a personal stream is not a direct data entry",
        ));
    }
    let birth = &ctx.birth;
    let birth_hash = ctx.birth_hash;
    let lookup = |hash: &EntryHash| (*hash == birth_hash).then_some(birth);
    let grants = GrantState::empty();
    let accept = AcceptContext {
        database_id: ctx.database,
        owner: Some(ctx.identity.node_key),
        stream_head: head,
        stream_frozen: false,
        hash_at_seq: None,
        lookup: &lookup,
        grants: &grants,
    };
    match evaluate(entry, &accept) {
        AcceptOutcome::Accept {
            entry_hash,
            duplicate: false,
        } => Ok(Verdict::Next { hash: entry_hash }),
        AcceptOutcome::Accept { .. } => Err("an entry that follows the head is a duplicate".into()),
        // A relay page is contiguous from the head, so a missing predecessor
        // means the relay served the wrong entry, which is not the stream's fault.
        AcceptOutcome::Pending { reason } => {
            Err(format!("a stream entry is out of place: {reason:?}"))
        }
        AcceptOutcome::Reject { reason } => Ok(refused(&format!("{reason:?}"))),
    }
}

fn refused(reason: &str) -> Verdict {
    Verdict::Refused(reason.to_owned())
}
