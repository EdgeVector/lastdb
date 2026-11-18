use crate::{
    BoundPolicy, Capability, DatabaseKind, Entry, Grant, LogError, LogState, PrincipalId, Stamp,
    StoredEntry, LIST_PAGE_LIMIT,
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

fn entry_visible(
    database: &LogState,
    stored: &StoredEntry,
    reader: &PrincipalId,
    policy: &BoundPolicy,
) -> bool {
    let entry = &stored.entry;
    if entry.database_id != database.database_id {
        return false;
    }
    match &database.kind {
        DatabaseKind::Personal => holds_owner(database, reader),
        DatabaseKind::Shareable { publisher } => {
            policy.allows_in_share(&entry.namespace)
                && share_visible(database, entry, reader, publisher, policy)
        }
    }
}

/// Mutation-log visibility. The default matches the previous predicate.
///
/// The publisher sees every official entry. Anyone else needs Receive, or
/// `read: anyone`, or Submit when `write` is `direct`. That reader then sees
/// publisher-written entries. `write: direct` also shows an entry whose writer
/// key holds Submit. `proposals` does not.
fn share_visible(
    database: &LogState,
    entry: &Entry,
    reader: &PrincipalId,
    publisher: &PrincipalId,
    policy: &BoundPolicy,
) -> bool {
    if reader == publisher {
        return true;
    }
    let admission = policy.share();
    let can_read = holds_receive(database, reader)
        || admission.reads_without_grant()
        || (admission.shows_submit_writers() && holds_submit(database, reader));
    if !can_read {
        return false;
    }
    if &entry.writer.key == publisher {
        return true;
    }
    admission.shows_submit_writers() && holds_submit(database, &entry.writer.key)
}

impl LogState {
    /// Whether this key may call list or head.
    ///
    /// `policy` is the caller's share settings. The default does not change
    /// this check. `read: anyone` also allows a caller that holds no grant.
    /// That caller still cannot append.
    #[must_use]
    pub fn reader_allowed(&self, principal: &PrincipalId, policy: &BoundPolicy) -> bool {
        match &self.kind {
            DatabaseKind::Personal => holds_owner(self, principal),
            DatabaseKind::Shareable { publisher } => {
                principal == publisher
                    || holds_receive(self, principal)
                    || holds_submit(self, principal)
                    || policy.share().reads_without_grant()
            }
        }
    }

    /// Allow a principal to read or append.
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

    /// Read the last entry visible to a principal.
    pub fn head_for(
        &self,
        reader: &PrincipalId,
        policy: &BoundPolicy,
    ) -> Result<Option<ScopedHead>, LogError> {
        if !self.reader_allowed(reader, policy) {
            return Err(LogError::Unauthorized);
        }
        Ok(scoped_head(self, reader, policy))
    }

    /// List visible entries after the caller's position.
    pub fn list_retained(
        &self,
        reader: &PrincipalId,
        after: u64,
        limit: usize,
        policy: &BoundPolicy,
    ) -> Result<ListPage, LogError> {
        if limit == 0 || limit > LIST_PAGE_LIMIT {
            return Err(LogError::InvalidPage);
        }
        if !self.reader_allowed(reader, policy) {
            return Err(LogError::Unauthorized);
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
        DatabaseKind::Shareable { publisher } => {
            if caller == publisher {
                Ok(())
            } else {
                Err(LogError::Unauthorized)
            }
        }
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

fn scoped_head(state: &LogState, reader: &PrincipalId, policy: &BoundPolicy) -> Option<ScopedHead> {
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
    policy: &BoundPolicy,
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
