//! Per-database publisher and folded grant state, derived from signed birth/grant entries.

use std::collections::{BTreeMap, HashMap};

use mutation_log::{
    position_of, DatabaseId, DeviceId, GrantBody, GrantState, Hex32, NodeKey, RecordPosition, Role,
    SignedEntry,
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

    /// A member's explicit grant takes precedence over the public read grant.
    pub(crate) fn may_read(&self, db: &DatabaseId, key: &NodeKey) -> bool {
        self.dbs.get(db).is_some_and(|db| db.state.may_read(key))
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
