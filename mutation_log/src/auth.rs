use crate::{
    cursor::cursor_space, state::StreamProgress, AckCursor, Capability, CursorSpace, DatabaseKind,
    Grant, LogError, LogState, PrincipalId, StreamId, StreamRole,
};

/// A receiver may read only the publisher stream of this shareable database.
///
/// The publisher reads every stream of this database, including submissions.
/// A contributor reads only the submission stream named by that contributor's
/// grant. A personal database is readable by its device grantees and by no
/// receiver, because a receiver grant cannot be stored there.
#[must_use]
pub fn can_read(principal: &PrincipalId, database: &LogState, stream: &StreamId) -> bool {
    match &database.kind {
        DatabaseKind::Personal => holds_device(database, principal),
        DatabaseKind::Shareable {
            publisher,
            publisher_stream,
        } => {
            if principal == publisher {
                return true;
            }
            if stream == publisher_stream {
                return holds_receive(database, principal);
            }
            holds_submit(database, principal, stream)
        }
    }
}

impl LogState {
    /// Whether `principal` may list or ack this database at all.
    #[must_use]
    pub fn reader_allowed(&self, principal: &PrincipalId) -> bool {
        match &self.kind {
            DatabaseKind::Personal => holds_device(self, principal),
            DatabaseKind::Shareable { publisher, .. } => {
                principal == publisher
                    || holds_receive(self, principal)
                    || self.grants.iter().any(|grant| {
                        grant.principal == *principal
                            && matches!(grant.capability, Capability::Submit { .. })
                    })
            }
        }
    }

    /// Grant read or append access. Sharing grants this and nothing about storage.
    ///
    /// `caller` must be the publisher, or a device grantee of a personal
    /// database. The first personal device grant is made by that device.
    /// A second identical grant is the same state. A personal database rejects
    /// [`Capability::Receive`] and [`Capability::Submit`]. A shareable database
    /// rejects [`Capability::Device`].
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
        let previous = cursor_space(self, &principal);
        let name = principal.clone();
        let mut next = self.clone();
        register_stream(&mut next, &principal, &capability)?;
        next.grants.push(Grant {
            principal,
            capability,
        });
        realign_ack(&mut next, &name, &previous);
        Ok(next)
    }

    /// Remove one grant. An open append on that stream is cleared.
    ///
    /// The ack is removed when the principal can no longer read. When the
    /// principal can still read but the cursor space changed, the ack resets
    /// to the start of the new space.
    ///
    /// `can_read` is false for that capability on the returned state. Revoking
    /// a grant that is not present leaves the state unchanged. There is no
    /// key rotation; this is the whole removal path.
    pub fn revoke(
        &self,
        caller: &PrincipalId,
        principal: &PrincipalId,
        capability: &Capability,
    ) -> Result<Self, LogError> {
        self.authorize_maintainer(caller)?;
        let previous = cursor_space(self, principal);
        let Some(index) = self
            .grants
            .iter()
            .position(|grant| &grant.principal == principal && &grant.capability == capability)
        else {
            return Ok(self.clone());
        };
        let mut next = self.clone();
        next.grants.remove(index);
        if next.reader_allowed(principal) {
            realign_ack(&mut next, principal, &previous);
        } else {
            next.acks.remove(principal);
        }
        clear_inflight(&mut next, principal, capability);
        Ok(next)
    }

    /// Publisher, or a device grantee, may snapshot and trim.
    pub(crate) fn authorize_maintainer(&self, caller: &PrincipalId) -> Result<(), LogError> {
        match &self.kind {
            DatabaseKind::Shareable { publisher, .. } => caller_is(caller, publisher),
            DatabaseKind::Personal => {
                if holds_device(self, caller) {
                    Ok(())
                } else {
                    Err(LogError::Unauthorized)
                }
            }
        }
    }

    fn reject_wrong_kind(&self, capability: &Capability) -> Result<(), LogError> {
        match (&self.kind, capability) {
            (DatabaseKind::Personal, Capability::Receive | Capability::Submit { .. }) => {
                Err(LogError::PersonalNotShareable)
            }
            (DatabaseKind::Shareable { .. }, Capability::Device { .. }) => {
                Err(LogError::DeviceGrantNotShareable)
            }
            _ => Ok(()),
        }
    }
}

fn register_stream(
    state: &mut LogState,
    principal: &PrincipalId,
    capability: &Capability,
) -> Result<(), LogError> {
    match capability {
        Capability::Receive => Ok(()),
        Capability::Device { stream } => insert_role(
            state,
            stream,
            StreamRole::Device {
                owner: principal.clone(),
            },
        ),
        Capability::Submit { stream } => {
            if let DatabaseKind::Shareable {
                publisher_stream, ..
            } = &state.kind
            {
                if stream == publisher_stream {
                    return Err(LogError::Unauthorized);
                }
            }
            insert_role(
                state,
                stream,
                StreamRole::Submission {
                    author: principal.clone(),
                },
            )
        }
    }
}

fn insert_role(state: &mut LogState, stream: &StreamId, role: StreamRole) -> Result<(), LogError> {
    match state.streams.get(stream) {
        None => {
            state.streams.insert(
                stream.clone(),
                StreamProgress {
                    last_writer_seq: 0,
                    trimmed_writer_seq: 0,
                    role,
                },
            );
            Ok(())
        }
        Some(progress) if progress.role == role => Ok(()),
        Some(_) => Err(LogError::Unauthorized),
    }
}

fn authorize_grant(
    state: &LogState,
    caller: &PrincipalId,
    principal: &PrincipalId,
    capability: &Capability,
) -> Result<(), LogError> {
    match &state.kind {
        DatabaseKind::Shareable { publisher, .. } => caller_is(caller, publisher),
        DatabaseKind::Personal => {
            let bootstrapping = !state
                .grants
                .iter()
                .any(|grant| matches!(grant.capability, Capability::Device { .. }));
            if bootstrapping {
                return if caller == principal && matches!(capability, Capability::Device { .. }) {
                    Ok(())
                } else {
                    Err(LogError::Unauthorized)
                };
            }
            if holds_device(state, caller) {
                Ok(())
            } else {
                Err(LogError::Unauthorized)
            }
        }
    }
}

fn caller_is(caller: &PrincipalId, expected: &PrincipalId) -> Result<(), LogError> {
    if caller == expected {
        Ok(())
    } else {
        Err(LogError::Unauthorized)
    }
}

fn realign_ack(state: &mut LogState, principal: &PrincipalId, previous: &CursorSpace) {
    let current = cursor_space(state, principal);
    if current == *previous {
        return;
    }
    state.acks.insert(
        principal.clone(),
        AckCursor {
            space: current,
            through: 0,
        },
    );
}

fn clear_inflight(state: &mut LogState, principal: &PrincipalId, capability: &Capability) {
    let stream = match capability {
        Capability::Device { stream } | Capability::Submit { stream } => stream,
        Capability::Receive => return,
    };
    if state.inflight.get(stream) == Some(principal) {
        state.inflight.remove(stream);
    }
}

pub(crate) fn is_publisher(state: &LogState, principal: &PrincipalId) -> bool {
    matches!(&state.kind, DatabaseKind::Shareable { publisher, .. } if publisher == principal)
}

pub(crate) fn submit_streams(state: &LogState, principal: &PrincipalId) -> Vec<StreamId> {
    state
        .grants
        .iter()
        .filter_map(|grant| match &grant.capability {
            Capability::Submit { stream } if grant.principal == *principal => Some(stream.clone()),
            _ => None,
        })
        .collect()
}

fn holds_device(database: &LogState, principal: &PrincipalId) -> bool {
    database.grants.iter().any(|grant| {
        grant.principal == *principal && matches!(grant.capability, Capability::Device { .. })
    })
}

pub(crate) fn holds_receive(database: &LogState, principal: &PrincipalId) -> bool {
    database
        .grants
        .iter()
        .any(|grant| grant.principal == *principal && grant.capability == Capability::Receive)
}

fn holds_submit(database: &LogState, principal: &PrincipalId, stream: &StreamId) -> bool {
    database.grants.iter().any(|grant| {
        grant.principal == *principal
            && matches!(&grant.capability, Capability::Submit { stream: granted } if granted == stream)
    })
}
