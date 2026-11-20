//! Folded grant state and role-reduction cutoffs.

use std::collections::BTreeMap;

use crate::entry::{GrantBody, SignedEntry};
use crate::ids::{DeviceId, EntryHash, NodeKey};
use crate::kinds::{Grantee, Role};
use crate::payload::RecordPosition;

/// Effective role for one grantee after folding publisher grant entries.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct GrantState {
    /// Per-member role (absent = no grant).
    members: BTreeMap<NodeKey, Role>,
    /// Public-read (`any`) role; only `Read` is valid when present.
    any: Option<Role>,
    /// Accumulated cutoffs keyed by (grantee node_key, device_id).
    /// For `any`, cutoffs are ignored (`any` is read-only).
    cutoffs: BTreeMap<(NodeKey, DeviceId), CutoffTip>,
}

/// One device cutoff tip.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CutoffTip {
    /// Last accepted writer_seq (0 = no writes accepted).
    pub writer_seq: u64,
    /// Entry hash at that seq (zeros when seq is 0).
    pub entry_hash: EntryHash,
}

impl GrantState {
    /// Empty grant table (owner still authorized separately).
    #[must_use]
    pub fn empty() -> Self {
        Self::default()
    }

    /// Effective role for `node_key` (None if never granted / revoked).
    #[must_use]
    pub fn role_of(&self, node_key: &NodeKey) -> Option<Role> {
        self.members
            .get(node_key)
            .copied()
            .filter(|r| *r != Role::Revoked)
    }

    /// Public-read role when `any` is granted.
    #[must_use]
    pub fn any_role(&self) -> Option<Role> {
        self.any
    }

    /// Cutoff tip for `(grantee, device)`, if any.
    #[must_use]
    pub fn cutoff(&self, grantee: &NodeKey, device: &DeviceId) -> Option<CutoffTip> {
        self.cutoffs.get(&(*grantee, *device)).copied()
    }

    /// Apply one grant entry in merge order. Equal lamport: revoked wins.
    pub fn apply_grant(&mut self, position: RecordPosition, body: &GrantBody) {
        let _ = position; // ordering is the caller's responsibility
        match body.grantee {
            Grantee::Any => {
                if body.role == Role::Read {
                    self.any = Some(Role::Read);
                }
            }
            Grantee::Member(key) => {
                let prev = self.members.get(&key).copied();
                let next = pick_role(prev, body.role);
                if next == Role::Revoked {
                    self.members.remove(&key);
                } else {
                    self.members.insert(key, next);
                }
                if is_reduction(prev, body.role) {
                    for c in &body.cutoffs {
                        self.cutoffs.insert(
                            (key, c.device_id),
                            CutoffTip {
                                writer_seq: c.writer_seq,
                                entry_hash: c.entry_hash,
                            },
                        );
                    }
                }
            }
        }
    }

    /// Fold a grant window from publisher entries named by `seen_heads` only.
    ///
    /// Starts empty (does not inherit `self`). Cutoffs on the returned state are
    /// those carried by reductions inside the window; callers still apply
    /// cutoffs from the full verified `S` separately.
    pub fn fold_window_from_seen(
        seen: &[EntryHash],
        lookup: impl Fn(&EntryHash) -> Option<&AcceptedMeta>,
    ) -> GrantState {
        let mut grants: Vec<(RecordPosition, &GrantBody)> = Vec::new();
        for h in seen {
            let Some(meta) = lookup(h) else {
                continue;
            };
            if !meta.is_publisher {
                continue;
            }
            if let Some(body) = meta.grant_body.as_ref() {
                grants.push((meta.position, body));
            }
        }
        grants.sort_by(|a, b| {
            let primary = (a.0.lamport, a.0.node_key, a.0.device_id).cmp(&(
                b.0.lamport,
                b.0.node_key,
                b.0.device_id,
            ));
            primary.then_with(|| grant_tie_rank(a.1.role).cmp(&grant_tie_rank(b.1.role)))
        });
        let mut folded = GrantState::empty();
        for (pos, body) in grants {
            folded.apply_grant(pos, body);
        }
        folded
    }
}

fn grant_tie_rank(role: Role) -> u8 {
    // Lower sorts first; apply revoked last so it wins.
    match role {
        Role::Write => 0,
        Role::Submit => 1,
        Role::Read => 2,
        Role::Revoked => 3,
    }
}

fn pick_role(prev: Option<Role>, next: Role) -> Role {
    match (prev, next) {
        (_, Role::Revoked) => Role::Revoked,
        (Some(Role::Revoked), r) => r,
        (_, n) => n,
    }
}

fn is_reduction(prev: Option<Role>, next: Role) -> bool {
    match prev {
        None => false, // first grant — cutoffs not used
        Some(p) => next.is_lower_than(p) || next == Role::Revoked,
    }
}

/// Metadata retained for each accepted entry (enough for AcceptEntry deps).
#[derive(Clone, Debug)]
pub struct AcceptedMeta {
    /// LWW / grant position.
    pub position: RecordPosition,
    /// True when the writer is the birth owner (publisher stream).
    pub is_publisher: bool,
    /// Kind was grant; body retained for folding.
    pub grant_body: Option<GrantBody>,
    /// Full entry kept for evidence / pending promotion.
    pub entry: SignedEntry,
}

/// True when `writer_seq` is past a cutoff tip (direct write rejected).
#[must_use]
pub fn past_cutoff(seq: u64, tip: CutoffTip) -> bool {
    seq > tip.writer_seq
}
