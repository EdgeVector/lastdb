//! Per-database publisher and folded grant state, derived from signed birth/grant entries.

use std::collections::{BTreeMap, HashMap};

use mutation_log::{
    position_of, DatabaseId, DeviceId, GrantBody, GrantState, Grantee, Hex32, NodeKey,
    RecordPosition, Role, SignedEntry,
};

/// Publisher identity proven by a verified `birth` entry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Birth {
    pub(crate) owner: NodeKey,
    pub(crate) db_salt: Hex32,
}

struct DbGrants {
    birth: Birth,
    grants: BTreeMap<(DeviceId, u64), (RecordPosition, GrantBody)>,
    state: GrantState,
}

/// Grant index for every database this relay has seen a verified birth for.
#[derive(Default)]
pub(crate) struct GrantIndex {
    dbs: HashMap<DatabaseId, DbGrants>,
}

impl GrantIndex {
    pub(crate) fn birth(&self, db: &DatabaseId) -> Option<Birth> {
        self.dbs.get(db).map(|d| d.birth)
    }

    /// Current non-revoked member role for `key`.
    pub(crate) fn role_of(&self, db: &DatabaseId, key: &NodeKey) -> Option<Role> {
        self.dbs.get(db)?.state.role_of(key)
    }

    /// True when folded grants list `key` as `Role::Revoked`.
    pub(crate) fn is_revoked(&self, db: &DatabaseId, key: &NodeKey) -> bool {
        self.dbs.get(db).is_some_and(|d| {
            d.state
                .members()
                .any(|(member, role)| member == *key && role == Role::Revoked)
        })
    }

    /// True when any recorded publisher grant gave `key` the `write` role, now or before.
    pub(crate) fn ever_held_write(&self, db: &DatabaseId, key: &NodeKey) -> bool {
        self.dbs.get(db).is_some_and(|d| {
            d.grants
                .values()
                .any(|(_, body)| body.role == Role::Write && body.grantee == Grantee::Member(*key))
        })
    }

    /// True when an active `grant#any` / `read` is in force.
    pub(crate) fn any_read(&self, db: &DatabaseId) -> bool {
        self.dbs
            .get(db)
            .is_some_and(|d| d.state.any_role() == Some(Role::Read))
    }

    /// True when any `grant` entry (`grant#any` or member grant) has been recorded for `db`.
    pub(crate) fn has_grants(&self, db: &DatabaseId) -> bool {
        self.dbs.get(db).is_some_and(|d| {
            !d.grants.is_empty()
                || d.state.any_role().is_some()
                || d.state.members().next().is_some()
        })
    }

    /// Record a verified birth. The first verified birth for a database wins.
    pub(crate) fn install_birth(&mut self, db: DatabaseId, birth: Birth) {
        self.dbs.entry(db).or_insert_with(|| DbGrants {
            birth,
            grants: BTreeMap::new(),
            state: GrantState::empty(),
        });
    }

    /// Fold one verified publisher grant entry. Replays of the same entry are idempotent.
    pub(crate) fn apply_grant(&mut self, db: &DatabaseId, entry: &SignedEntry) {
        let (Some(dbg), SignedEntry::Grant { header, body, .. }) = (self.dbs.get_mut(db), entry)
        else {
            return;
        };
        dbg.grants.insert(
            (header.device_id, header.writer_seq),
            (position_of(entry), body.clone()),
        );
        dbg.state = GrantState::rebuild_from(dbg.grants.values().map(|(p, b)| (*p, b)));
    }
}
