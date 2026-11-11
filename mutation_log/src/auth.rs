use crate::{
    state::StreamProgress, Capability, DatabaseKind, Grant, LogError, LogState, PrincipalId,
    StreamId, StreamRole,
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
    /// A second identical grant is the same state. A personal database rejects
    /// [`Capability::Receive`] and [`Capability::Submit`]. A shareable database
    /// rejects [`Capability::Device`].
    pub fn grant(&self, principal: PrincipalId, capability: Capability) -> Result<Self, LogError> {
        self.reject_wrong_kind(&capability)?;
        if self
            .grants
            .iter()
            .any(|grant| grant.principal == principal && grant.capability == capability)
        {
            return Ok(self.clone());
        }
        let mut next = self.clone();
        register_stream(&mut next, &principal, &capability)?;
        next.grants.push(Grant {
            principal,
            capability,
        });
        Ok(next)
    }

    /// Remove one grant. The principal's ack is removed with it.
    ///
    /// Revoking a grant that is not present leaves the state unchanged.
    pub fn revoke(
        &self,
        principal: &PrincipalId,
        capability: &Capability,
    ) -> Result<Self, LogError> {
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
        Ok(next)
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
                    role,
                },
            );
            Ok(())
        }
        Some(progress) if progress.role == role => Ok(()),
        Some(_) => Err(LogError::Unauthorized),
    }
}

fn holds_device(database: &LogState, principal: &PrincipalId) -> bool {
    database.grants.iter().any(|grant| {
        grant.principal == *principal && matches!(grant.capability, Capability::Device { .. })
    })
}

fn holds_receive(database: &LogState, principal: &PrincipalId) -> bool {
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
