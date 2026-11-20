//! Folded grant state and role-reduction cutoffs.

use std::collections::{BTreeMap, BTreeSet};

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
    /// Members that have received at least one role reduction/revoke.
    /// Unlisted devices for these members default to cutoff tip 0.
    reduced: BTreeSet<NodeKey>,
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

    /// Active members and their roles, ascending by node key. Revoked keys are absent.
    pub fn members(&self) -> impl Iterator<Item = (NodeKey, Role)> + '_ {
        self.members.iter().map(|(key, role)| (*key, *role))
    }

    /// Public-read role when `any` is granted.
    #[must_use]
    pub fn any_role(&self) -> Option<Role> {
        self.any
    }

    /// True when this member has ever been role-reduced or revoked.
    #[must_use]
    pub fn was_reduced(&self, grantee: &NodeKey) -> bool {
        self.reduced.contains(grantee)
    }

    /// Cutoff tip for `(grantee, device)`.
    ///
    /// After a reduction, devices absent from `cutoffs[]` are tip 0 (unseen →
    /// cutoff 0). Before any reduction, returns `None` (unrestricted).
    #[must_use]
    pub fn cutoff(&self, grantee: &NodeKey, device: &DeviceId) -> Option<CutoffTip> {
        if let Some(tip) = self.cutoffs.get(&(*grantee, *device)).copied() {
            return Some(tip);
        }
        if self.reduced.contains(grantee) {
            return Some(CutoffTip {
                writer_seq: 0,
                entry_hash: EntryHash::ZERO,
            });
        }
        None
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
                    self.reduced.insert(key);
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

    /// Rebuild grant state from every accepted publisher grant body.
    ///
    /// Sorts by `(lamport, node_key, device_id)` then revoke-wins tie rank,
    /// then applies in that order. Call after each accepted publisher grant.
    #[must_use]
    pub fn rebuild_from<'a>(
        grants: impl IntoIterator<Item = (RecordPosition, &'a GrantBody)>,
    ) -> Self {
        let mut list: Vec<(RecordPosition, &GrantBody)> = grants.into_iter().collect();
        sort_grants(&mut list);
        let mut folded = Self::empty();
        for (pos, body) in list {
            folded.apply_grant(pos, body);
        }
        folded
    }

    /// Fold a grant window from publisher entries named by `seen_heads` only.
    ///
    /// Starts empty (does not inherit `self`). Cutoffs on the returned state are
    /// those carried by reductions inside the window; callers still apply
    /// cutoffs from the full verified `S` separately.
    pub fn fold_window_from_seen<'s>(
        seen: &[EntryHash],
        lookup: impl Fn(&EntryHash) -> Option<&'s AcceptedMeta>,
    ) -> Self {
        let grants = seen.iter().filter_map(|h| {
            let meta = lookup(h)?;
            if !meta.is_publisher {
                return None;
            }
            meta.grant_body.as_ref().map(|body| (meta.position, body))
        });
        Self::rebuild_from(grants)
    }
}

fn sort_grants(grants: &mut [(RecordPosition, &GrantBody)]) {
    grants.sort_by(|a, b| {
        let primary = (a.0.lamport, a.0.node_key, a.0.device_id).cmp(&(
            b.0.lamport,
            b.0.node_key,
            b.0.device_id,
        ));
        primary.then_with(|| grant_tie_rank(a.1.role).cmp(&grant_tie_rank(b.1.role)))
    });
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
