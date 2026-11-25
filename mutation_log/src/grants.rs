//! Folded grant state.

use std::collections::BTreeMap;

use crate::entry::{GrantBody, SignedEntry};
use crate::ids::{EntryHash, NodeKey};
use crate::kinds::{Grantee, Role};
use crate::payload::RecordPosition;

/// Effective role for one grantee after folding publisher grant entries.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct GrantState {
    /// Per-member role (absent = no grant).
    members: BTreeMap<NodeKey, Role>,
    /// Public-read (`any`) role; only `Read` is valid when present.
    any: Option<Role>,
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

    /// Members and their folded roles, ascending by node key.
    /// Includes `Role::Revoked` so owners can see who was removed; auth still
    /// uses [`Self::role_of`], which treats revoked as no grant.
    pub fn members(&self) -> impl Iterator<Item = (NodeKey, Role)> + '_ {
        self.members.iter().map(|(key, role)| (*key, *role))
    }

    /// Public-read role when `any` is granted.
    #[must_use]
    pub fn any_role(&self) -> Option<Role> {
        self.any
    }

    /// Apply one grant entry in merge order. Equal lamport: revoked wins.
    pub fn apply_grant(&mut self, position: RecordPosition, body: &GrantBody) {
        let _ = position; // ordering is the caller's responsibility
        match body.grantee {
            Grantee::Any => match body.role {
                Role::Read => self.any = Some(Role::Read),
                Role::Revoked => self.any = None,
                Role::Submit | Role::Write => {}
            },
            Grantee::Member(key) => {
                let prev = self.members.get(&key).copied();
                let next = pick_role(prev, body.role);
                // Keep revoked rows so the owner grants view can list them.
                // role_of filters Revoked.
                self.members.insert(key, next);
            }
        }
    }

    /// Rebuild grant state from every accepted publisher grant body.
    ///
    /// Sorts by `lamport`, revoke-wins tie rank, then `(node_key, device_id)`,
    /// and applies in that order. Call after each accepted publisher grant.
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
    /// Starts empty (does not inherit `self`).
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
        a.0.lamport
            .cmp(&b.0.lamport)
            .then_with(|| grant_tie_rank(a.1.role).cmp(&grant_tie_rank(b.1.role)))
            .then_with(|| (a.0.node_key, a.0.device_id).cmp(&(b.0.node_key, b.0.device_id)))
    });
}

pub(crate) fn grant_tie_rank(role: Role) -> u8 {
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
