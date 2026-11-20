//! `AcceptEntry(E, S) → accept | pending | reject`.

use crate::auth::authorize_data;
use crate::entry::SignedEntry;
use crate::grants::{AcceptedMeta, GrantState};
use crate::ids::{DatabaseId, EntryHash, NodeKey, StreamId};
use crate::kinds::EntryKind;
use crate::payload::RecordPosition;
use crate::wire::{entry_hash, verify_entry_signature, EncodeError, VerifyError};

/// Outcome of [`super::history::VerifiedHistory::accept_entry`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AcceptOutcome {
    /// Entry applied (or identical duplicate already held).
    Accept {
        /// Computed entry hash.
        entry_hash: EntryHash,
        /// True when this was an idempotent duplicate.
        duplicate: bool,
    },
    /// Missing predecessor or unseen `seen_heads` hash — not applied.
    Pending {
        /// Why the entry cannot be applied yet.
        reason: PendingReason,
    },
    /// Rejected; evidence retained when applicable.
    Reject {
        /// Rejection cause.
        reason: RejectReason,
    },
}

/// Pending causes (do not apply).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PendingReason {
    /// Stream predecessor not yet accepted.
    MissingPredecessor {
        /// Expected prev hash.
        prev_hash: EntryHash,
    },
    /// A `seen_heads` hash is not held+accepted.
    MissingSeenHead {
        /// Missing hash.
        entry_hash: EntryHash,
    },
}

/// Reject causes (do not apply; keep evidence).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RejectReason {
    /// Signature / encode failure.
    Signature(VerifyError),
    /// `database_id` does not match S.
    DatabaseMismatch,
    /// Equivocation at this stream seq — stream frozen.
    Equivocation {
        /// Competing entry hash already held.
        held_hash: EntryHash,
        /// Incoming entry hash.
        new_hash: EntryHash,
    },
    /// Stream already frozen.
    StreamFrozen,
    /// Lamport ≠ 1 + max(L_prev, L_seen).
    Lamport {
        /// Expected lamport.
        expected: u64,
        /// Claimed lamport.
        got: u64,
    },
    /// Writer not authorized (publisher kind / grant window / cutoff).
    Unauthorized(&'static str),
    /// Birth rules violated.
    Birth(&'static str),
    /// Malformed entry fields.
    Malformed(&'static str),
    /// Encode failure while hashing.
    Encode(EncodeError),
}

/// Stream head retained in verified history.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StreamHead {
    /// Tip entry hash.
    pub entry_hash: EntryHash,
    /// Tip writer_seq.
    pub writer_seq: u64,
    /// Tip lamport.
    pub lamport: u64,
}

/// Inputs derived from S for the pure AcceptEntry checks (no mutation).
pub struct AcceptContext<'a> {
    /// Database id of S.
    pub database_id: DatabaseId,
    /// Owner from birth, if birth accepted.
    pub owner: Option<NodeKey>,
    /// Stream head for the entry's stream, if any.
    pub stream_head: Option<StreamHead>,
    /// True when this stream is frozen.
    pub stream_frozen: bool,
    /// Hash already stored at this exact seq (for duplicate / equivocation).
    pub hash_at_seq: Option<EntryHash>,
    /// Lookup accepted entry meta by hash.
    pub lookup: &'a dyn Fn(&EntryHash) -> Option<&'a AcceptedMeta>,
    /// Global grant state (cutoffs already in S).
    pub grants: &'a GrantState,
}

/// Run AcceptEntry checks 1–6. Does not mutate S.
pub fn evaluate(entry: &SignedEntry, ctx: &AcceptContext<'_>) -> AcceptOutcome {
    if let Err(err) = verify_entry_signature(entry) {
        return reject(RejectReason::Signature(err));
    }
    let hash = match entry_hash(entry) {
        Ok(h) => h,
        Err(e) => return reject(RejectReason::Encode(e)),
    };
    let h = entry.header();
    if h.database_id != ctx.database_id {
        return reject(RejectReason::DatabaseMismatch);
    }
    if ctx.stream_frozen {
        return reject(RejectReason::StreamFrozen);
    }
    if let Some(outcome) = check_chain(h.writer_seq, h.prev_hash, hash, ctx) {
        return outcome;
    }
    if let Some(missing) = missing_seen_head(&h.seen_heads, ctx) {
        return AcceptOutcome::Pending {
            reason: PendingReason::MissingSeenHead {
                entry_hash: missing,
            },
        };
    }
    let expected = expected_lamport(h.writer_seq, ctx.stream_head, &h.seen_heads, ctx);
    if h.lamport != expected {
        return reject(RejectReason::Lamport {
            expected,
            got: h.lamport,
        });
    }
    if let Some(reason) = check_auth(entry, ctx) {
        return reject(reason);
    }
    AcceptOutcome::Accept {
        entry_hash: hash,
        duplicate: false,
    }
}

fn reject(reason: RejectReason) -> AcceptOutcome {
    AcceptOutcome::Reject { reason }
}

fn check_chain(
    writer_seq: u64,
    prev_hash: EntryHash,
    new_hash: EntryHash,
    ctx: &AcceptContext<'_>,
) -> Option<AcceptOutcome> {
    if writer_seq < 1 {
        return Some(reject(RejectReason::Malformed("writer_seq must be ≥ 1")));
    }
    if let Some(held) = ctx.hash_at_seq {
        if held == new_hash {
            return Some(AcceptOutcome::Accept {
                entry_hash: new_hash,
                duplicate: true,
            });
        }
        return Some(reject(RejectReason::Equivocation {
            held_hash: held,
            new_hash,
        }));
    }
    match ctx.stream_head {
        None => {
            if writer_seq != 1 || prev_hash != EntryHash::ZERO {
                return Some(AcceptOutcome::Pending {
                    reason: PendingReason::MissingPredecessor { prev_hash },
                });
            }
        }
        Some(head) => {
            if writer_seq != head.writer_seq + 1 {
                return Some(AcceptOutcome::Pending {
                    reason: PendingReason::MissingPredecessor { prev_hash },
                });
            }
            if prev_hash != head.entry_hash {
                return Some(reject(RejectReason::Malformed(
                    "prev_hash does not match stream head",
                )));
            }
        }
    }
    None
}

fn missing_seen_head(seen: &[EntryHash], ctx: &AcceptContext<'_>) -> Option<EntryHash> {
    seen.iter().find(|h| (ctx.lookup)(h).is_none()).copied()
}

fn expected_lamport(
    writer_seq: u64,
    head: Option<StreamHead>,
    seen: &[EntryHash],
    ctx: &AcceptContext<'_>,
) -> u64 {
    let l_prev = if writer_seq == 1 {
        0
    } else {
        head.map(|h| h.lamport).unwrap_or(0)
    };
    let mut l_seen = 0u64;
    for h in seen {
        if let Some(meta) = (ctx.lookup)(h) {
            l_seen = l_seen.max(meta.position.lamport);
        }
    }
    1 + l_prev.max(l_seen)
}

fn check_auth(entry: &SignedEntry, ctx: &AcceptContext<'_>) -> Option<RejectReason> {
    let h = entry.header();
    match entry.kind() {
        EntryKind::Birth => check_birth(entry, ctx),
        k if k.is_publisher_kind() => match ctx.owner {
            Some(owner) if h.node_key == owner => None,
            _ => Some(RejectReason::Unauthorized(
                "publisher kind requires birth owner",
            )),
        },
        EntryKind::Data => match entry {
            SignedEntry::Data { body, .. } => {
                authorize_data(entry, body, ctx.owner, ctx.grants, ctx.lookup)
            }
            _ => Some(RejectReason::Malformed("data kind mismatch")),
        },
        _ => None,
    }
}

fn check_birth(entry: &SignedEntry, ctx: &AcceptContext<'_>) -> Option<RejectReason> {
    let h = entry.header();
    if h.writer_seq != 1 {
        return Some(RejectReason::Birth("birth requires writer_seq=1"));
    }
    if ctx.owner.is_some() {
        return Some(RejectReason::Birth("birth already accepted"));
    }
    match entry {
        SignedEntry::Birth { body, .. } if body.owner == h.node_key => None,
        SignedEntry::Birth { .. } => Some(RejectReason::Birth("owner must equal node_key")),
        _ => Some(RejectReason::Malformed("birth kind mismatch")),
    }
}

/// Build stream id from an entry header.
#[must_use]
pub fn stream_id_of(entry: &SignedEntry) -> StreamId {
    let h = entry.header();
    StreamId {
        node_key: h.node_key,
        device_id: h.device_id,
    }
}

/// Position of an entry for LWW / grants.
#[must_use]
pub fn position_of(entry: &SignedEntry) -> RecordPosition {
    let h = entry.header();
    RecordPosition {
        lamport: h.lamport,
        node_key: h.node_key,
        device_id: h.device_id,
    }
}
