use crate::{
    BoundPolicy, Capability, DatabaseKind, Grant, LogError, LogRole, LogState, PrincipalId, Stamp,
    StoredEntry, LIST_PAGE_LIMIT,
};

/// Head a reader is allowed to see.
///
/// `cursor` is the log head's delivery sequence. `stamp` is the last entry of
/// this database. It is absent when the log has no such entry.
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
    /// Entries of this database, oldest first.
    pub entries: Vec<StoredEntry>,
    /// Last delivery sequence scanned.
    ///
    /// The next call passes this value as `after`. An entry for another
    /// database is scanned and not returned. Unchanged when nothing after
    /// `after` was scanned.
    pub next: u64,
}

/// An entry of this log.
///
/// A reader who passed [`LogState::reader_allowed`] is served every entry
/// whose database id is this log. The writer is not a filter. Namespace
/// admission is an append rule, not a read filter.
fn entry_in_log(database: &LogState, stored: &StoredEntry) -> bool {
    stored.entry.database_id == database.database_id
}

impl LogState {
    /// Whether this key may call list or head.
    ///
    /// `policy` is the caller's share settings. The default does not change
    /// the mutation log. `read: anyone` also allows a caller that holds no
    /// grant on that log. That caller still cannot append.
    ///
    /// The proposal log is listed by the publisher only.
    #[must_use]
    pub fn reader_allowed(&self, principal: &PrincipalId, policy: &BoundPolicy) -> bool {
        match (&self.kind, self.role) {
            (DatabaseKind::Personal, LogRole::Mutation) => holds_owner(self, principal),
            (DatabaseKind::Personal, LogRole::Proposal) => false,
            (DatabaseKind::Shareable { publisher }, LogRole::Proposal) => principal == publisher,
            (DatabaseKind::Shareable { publisher }, LogRole::Mutation) => {
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
        Ok(scoped_head(self))
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
        Ok(page(self, after, limit))
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

fn scoped_head(state: &LogState) -> Option<ScopedHead> {
    let cursor = state.head.as_ref().map(|head| head.delivery_seq)?;
    let mut stamp = None;
    for stored in &state.entries {
        if entry_in_log(state, stored) {
            stamp = Some(stored.merge_stamp());
        }
    }
    Some(ScopedHead { cursor, stamp })
}

fn page(state: &LogState, after: u64, limit: usize) -> ListPage {
    let mut entries = Vec::new();
    let mut next = after;
    for stored in &state.entries {
        if stored.delivery_seq <= after {
            continue;
        }
        next = stored.delivery_seq;
        if !entry_in_log(state, stored) {
            continue;
        }
        entries.push(stored.clone());
        if entries.len() == limit {
            break;
        }
    }
    ListPage { entries, next }
}
