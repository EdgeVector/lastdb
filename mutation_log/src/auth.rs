use crate::{Capability, DatabaseKind, Entry, Grant, LogError, LogState, PrincipalId};

/// Whether `principal` may read this entry.
///
/// On a personal database, only an owner key reads. On a shareable database
/// the publisher key reads every entry. A receiver reads entries whose writer
/// key is the publisher key, including re-appends. A contributor also reads
/// their own pending entries.
#[must_use]
pub fn can_read(principal: &PrincipalId, database: &LogState, entry: &Entry) -> bool {
    if entry.database_id != database.database_id {
        return false;
    }
    match &database.kind {
        DatabaseKind::Personal => holds_owner(database, principal),
        DatabaseKind::Shareable { publisher } => share_read(database, principal, publisher, entry),
    }
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
    if &entry.writer.key == publisher {
        return holds_receive(database, principal);
    }
    entry.incorporated_from.is_none()
        && &entry.writer.key == principal
        && holds_submit(database, principal)
}

impl LogState {
    /// Whether this key may call list, head, or ack.
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

    /// Remove one grant. A missing grant succeeds and leaves the log unchanged.
    ///
    /// Revoking a key's owner or submit grant clears every open append for
    /// that key, on every device. A key that can no longer read loses its ack.
    pub fn revoke(
        &self,
        caller: &PrincipalId,
        principal: &PrincipalId,
        capability: &Capability,
    ) -> Result<Self, LogError> {
        self.authorize_maintainer_or_publisher(caller)?;
        let Some(index) = self
            .grants
            .iter()
            .position(|grant| &grant.principal == principal && &grant.capability == capability)
        else {
            return Ok(self.clone());
        };
        let mut next = self.clone();
        next.grants.remove(index);
        if !next.reader_allowed(principal) {
            next.acks.remove(principal);
        }
        if matches!(capability, Capability::Owner | Capability::Submit) {
            next.inflight.retain(|writer| &writer.key != principal);
        }
        Ok(next)
    }

    /// Publisher, or an owner key, may snapshot and trim.
    pub(crate) fn authorize_maintainer(&self, caller: &PrincipalId) -> Result<(), LogError> {
        self.authorize_maintainer_or_publisher(caller)
    }

    fn authorize_maintainer_or_publisher(&self, caller: &PrincipalId) -> Result<(), LogError> {
        match &self.kind {
            DatabaseKind::Shareable { publisher } => caller_is(caller, publisher),
            DatabaseKind::Personal => {
                if holds_owner(self, caller) {
                    Ok(())
                } else {
                    Err(LogError::Unauthorized)
                }
            }
        }
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

pub(crate) fn holds_receive(database: &LogState, principal: &PrincipalId) -> bool {
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
