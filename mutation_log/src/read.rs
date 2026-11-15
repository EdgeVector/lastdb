use crate::{
    Capability, DatabaseKind, Entry, Grant, LogError, LogState, NamespaceKind, NamespacePolicy,
    PrincipalId, SnapshotRef, Stamp, StoredEntry, LIST_PAGE_LIMIT,
};

/// Head a reader is allowed to see.
///
/// `cursor` is the log head's delivery sequence. `stamp` is the last entry
/// this reader can see, and it is absent when every stored entry is hidden.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScopedHead {
    /// Log head delivery sequence. Hidden entries do not move it backward.
    pub cursor: u64,
    /// Stamp of the last visible entry. Absent when this reader can see none.
    pub stamp: Option<Stamp>,
}

/// Snapshot the caller downloads before applying the still-stored suffix.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Reset {
    /// Shell pointer to the snapshot bytes.
    pub snapshot_ref: SnapshotRef,
    /// Last delivery sequence the snapshot includes.
    pub through: u64,
    /// When the shell cut the snapshot.
    pub at_ms: u64,
}

/// Still-stored entries a device needs after its position falls below the floor.
///
/// The caller downloads `reset` when it is set, applies `entries`, persists
/// that, then acks `next`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Recovery {
    /// Snapshot to install when the caller's position is below the trim floor.
    pub reset: Option<Reset>,
    /// Visible entries still stored after the caller's position.
    pub entries: Vec<StoredEntry>,
    /// Scan cursor to ack after the reset and `entries` are applied.
    pub next: u64,
}

/// One page of entries the reader may see, in delivery order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListPage {
    /// Visible entries, oldest first.
    pub entries: Vec<StoredEntry>,
    /// Last delivery sequence scanned, including entries this reader cannot see.
    ///
    /// The next call passes this value as `after`, so hidden entries are covered.
    /// Unchanged when nothing after `after` was scanned.
    pub next: u64,
}

/// Whether `principal` may read this entry.
///
/// On a personal database, only an owner key reads. On a shareable database
/// the publisher key reads every entry except an app-private namespace. A
/// receiver reads entries whose writer key is the publisher key, including
/// re-appends, and does not read an app-private namespace. Pending member
/// entries are visible only to the publisher. This matches the list filter.
#[must_use]
pub fn can_read(
    principal: &PrincipalId,
    database: &LogState,
    entry: &Entry,
    policy: &impl NamespacePolicy,
) -> bool {
    if entry.database_id != database.database_id {
        return false;
    }
    if shareable_app_private(database, entry, policy) {
        return false;
    }
    match &database.kind {
        DatabaseKind::Personal => holds_owner(database, principal),
        DatabaseKind::Shareable { publisher } => share_read(database, principal, publisher, entry),
    }
}

fn shareable_app_private(
    database: &LogState,
    entry: &Entry,
    policy: &impl NamespacePolicy,
) -> bool {
    matches!(database.kind, DatabaseKind::Shareable { .. })
        && policy.classify(&entry.namespace) == NamespaceKind::AppPrivate
}

fn share_read(
    database: &LogState,
    principal: &PrincipalId,
    publisher: &PrincipalId,
    entry: &Entry,
) -> bool {
    if principal == publisher {
        return true;
    }
    &entry.writer.key == publisher && holds_receive(database, principal)
}

/// Whether one stored entry is visible to `reader` in `database`.
#[must_use]
pub fn entry_visible(
    database: &LogState,
    stored: &StoredEntry,
    reader: &PrincipalId,
    policy: &impl NamespacePolicy,
) -> bool {
    if stored.entry.database_id != database.database_id {
        return false;
    }
    if !can_read(reader, database, &stored.entry, policy) {
        return false;
    }
    shareable_namespace_allowed(database, stored, policy)
}

fn shareable_namespace_allowed(
    database: &LogState,
    stored: &StoredEntry,
    policy: &impl NamespacePolicy,
) -> bool {
    if !matches!(database.kind, DatabaseKind::Shareable { .. }) {
        return true;
    }
    policy.classify(&stored.entry.namespace) == NamespaceKind::Official
}

impl LogState {
    /// Whether this key may call list or head.
    #[must_use]
    pub fn reader_allowed(&self, principal: &PrincipalId) -> bool {
        match &self.kind {
            DatabaseKind::Personal => holds_owner(self, principal),
            DatabaseKind::Shareable { publisher } => {
                principal == publisher
                    || holds_receive(self, principal)
                    || holds_submit(self, principal)
            }
        }
    }

    /// Grant `capability` to `principal`. The caller must already be allowed.
    ///
    /// The first grant on a personal database is an owner key granting itself.
    /// A personal database rejects [`Capability::Receive`] and [`Capability::Submit`].
    /// A shareable database rejects [`Capability::Owner`].
    pub fn grant(
        &self,
        caller: &PrincipalId,
        principal: PrincipalId,
        capability: Capability,
    ) -> Result<Self, LogError> {
        self.reject_wrong_kind(&capability)?;
        authorize_grant(self, caller, &principal, &capability)?;
        if self
            .grants
            .iter()
            .any(|grant| grant.principal == principal && grant.capability == capability)
        {
            return Ok(self.clone());
        }
        let mut next = self.clone();
        next.grants.push(Grant {
            principal,
            capability,
        });
        Ok(next)
    }

    fn reject_wrong_kind(&self, capability: &Capability) -> Result<(), LogError> {
        match (&self.kind, capability) {
            (DatabaseKind::Personal, Capability::Receive | Capability::Submit) => {
                Err(LogError::PersonalNotShareable)
            }
            (DatabaseKind::Shareable { .. }, Capability::Owner) => {
                Err(LogError::DeviceGrantNotShareable)
            }
            _ => Ok(()),
        }
    }

    /// Last visible entry, with the cursor set to the last delivery scanned.
    pub fn head_for(
        &self,
        reader: &PrincipalId,
        policy: &impl NamespacePolicy,
    ) -> Result<Option<ScopedHead>, LogError> {
        if !self.reader_allowed(reader) {
            return Err(LogError::Unauthorized);
        }
        Ok(scoped_head(self, reader, policy))
    }

    /// List after the caller's position using the last persisted trim boundary.
    pub fn list_retained(
        &self,
        reader: &PrincipalId,
        after: u64,
        limit: usize,
        policy: &impl NamespacePolicy,
    ) -> Result<ListPage, LogError> {
        if limit == 0 || limit > LIST_PAGE_LIMIT {
            return Err(LogError::InvalidPage);
        }
        if !self.reader_allowed(reader) {
            return Err(LogError::Unauthorized);
        }
        if after < self.trimmed_through {
            return Err(LogError::CursorExpired {
                snapshot_through: self
                    .snapshot
                    .as_ref()
                    .map_or(0, |cut| cut.through_delivery_seq),
            });
        }
        Ok(page(self, reader, after, limit, policy))
    }
}

fn authorize_grant(
    state: &LogState,
    caller: &PrincipalId,
    principal: &PrincipalId,
    capability: &Capability,
) -> Result<(), LogError> {
    match &state.kind {
        DatabaseKind::Shareable { publisher } => caller_is(caller, publisher),
        DatabaseKind::Personal => authorize_personal_grant(state, caller, principal, capability),
    }
}

fn authorize_personal_grant(
    state: &LogState,
    caller: &PrincipalId,
    principal: &PrincipalId,
    capability: &Capability,
) -> Result<(), LogError> {
    let bootstrapping = !state
        .grants
        .iter()
        .any(|grant| matches!(grant.capability, Capability::Owner));
    if bootstrapping {
        return if caller == principal && matches!(capability, Capability::Owner) {
            Ok(())
        } else {
            Err(LogError::Unauthorized)
        };
    }
    if holds_owner(state, caller) {
        Ok(())
    } else {
        Err(LogError::Unauthorized)
    }
}

fn caller_is(caller: &PrincipalId, expected: &PrincipalId) -> Result<(), LogError> {
    if caller == expected {
        Ok(())
    } else {
        Err(LogError::Unauthorized)
    }
}

pub(crate) fn holds_owner(database: &LogState, principal: &PrincipalId) -> bool {
    database
        .grants
        .iter()
        .any(|grant| grant.principal == *principal && matches!(grant.capability, Capability::Owner))
}

fn holds_receive(database: &LogState, principal: &PrincipalId) -> bool {
    database
        .grants
        .iter()
        .any(|grant| grant.principal == *principal && grant.capability == Capability::Receive)
}

pub(crate) fn holds_submit(database: &LogState, principal: &PrincipalId) -> bool {
    database
        .grants
        .iter()
        .any(|grant| grant.principal == *principal && grant.capability == Capability::Submit)
}

fn scoped_head(
    state: &LogState,
    reader: &PrincipalId,
    policy: &impl NamespacePolicy,
) -> Option<ScopedHead> {
    let cursor = state.head.as_ref().map(|head| head.delivery_seq)?;
    let mut stamp = None;
    for stored in &state.entries {
        if entry_visible(state, stored, reader, policy) {
            stamp = Some(stored.merge_stamp());
        }
    }
    Some(ScopedHead { cursor, stamp })
}

fn page(
    state: &LogState,
    reader: &PrincipalId,
    after: u64,
    limit: usize,
    policy: &impl NamespacePolicy,
) -> ListPage {
    let mut entries = Vec::new();
    let mut next = after;
    for stored in &state.entries {
        if stored.delivery_seq <= after {
            continue;
        }
        next = stored.delivery_seq;
        if !entry_visible(state, stored, reader, policy) {
            continue;
        }
        entries.push(stored.clone());
        if entries.len() == limit {
            break;
        }
    }
    ListPage { entries, next }
}
