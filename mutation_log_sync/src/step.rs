use crate::error::SyncError;
use crate::model::{writer_of, SyncEvent, SyncRole, SyncState};
use mutation_log::{
    AppendBatch, DatabaseKind, DeviceId, Entry, Incorporation, LogError, Namespace, NamespaceKind,
    NamespacePolicy, PrincipalId,
};

/// An effect [`crate::drive`] applies through the port. The log is not here.
pub(crate) enum Effect {
    Append(AppendBatch),
    Pull {
        reader: PrincipalId,
        after: u64,
        now_ms: u64,
    },
}

pub(crate) fn propose(
    state: &SyncState,
    event: SyncEvent,
    policy: &impl NamespacePolicy,
) -> Result<Effect, SyncError> {
    match event {
        SyncEvent::Append {
            namespace,
            payload,
            authored_at_ms,
            accepted_at_ms,
        } => append_effect(
            state,
            namespace,
            payload,
            authored_at_ms,
            accepted_at_ms,
            policy,
        ),
        SyncEvent::Pull { now_ms } => Ok(Effect::Pull {
            reader: state.reader().clone(),
            after: state.cursor,
            now_ms,
        }),
        SyncEvent::Reappend {
            entry,
            accepted_at_ms,
        } => reappend_effect(state, entry, accepted_at_ms, policy),
    }
}

fn append_effect(
    state: &SyncState,
    namespace: Namespace,
    payload: Vec<u8>,
    authored_at_ms: u64,
    accepted_at_ms: u64,
    policy: &impl NamespacePolicy,
) -> Result<Effect, SyncError> {
    let writer = role_writer(state).ok_or(SyncError::ReadOnly)?;
    if !state.resumed {
        return Err(SyncError::NeedsResume);
    }
    reject_app_private(&state.kind, &namespace, policy)?;
    let entry = Entry {
        database_id: state.database_id.clone(),
        writer: writer.clone(),
        writer_seq: state.next_seq,
        authored_at_ms,
        namespace,
        incorporated_from: None,
        payload,
    };
    Ok(Effect::Append(AppendBatch {
        writer,
        entries: vec![entry],
        accepted_at_ms,
    }))
}

fn reappend_effect(
    state: &SyncState,
    entry: Entry,
    accepted_at_ms: u64,
    policy: &impl NamespacePolicy,
) -> Result<Effect, SyncError> {
    let (principal, device) = publisher_device(state)?;
    if !state.resumed {
        return Err(SyncError::NeedsResume);
    }
    if entry.database_id != state.database_id {
        return Err(LogError::WrongDatabase.into());
    }
    if entry.writer.key == *principal {
        return Err(SyncError::NotSubmission);
    }
    reject_app_private(&state.kind, &entry.namespace, policy)?;
    let writer = writer_of(principal.clone(), device.clone());
    let copy = Entry {
        database_id: entry.database_id,
        writer,
        writer_seq: state.next_seq,
        authored_at_ms: entry.authored_at_ms,
        namespace: entry.namespace,
        incorporated_from: Some(Incorporation {
            writer: entry.writer,
            writer_seq: entry.writer_seq,
        }),
        payload: entry.payload,
    };
    Ok(Effect::Append(AppendBatch {
        writer: copy.writer.clone(),
        entries: vec![copy],
        accepted_at_ms,
    }))
}

fn reject_app_private(
    kind: &DatabaseKind,
    namespace: &Namespace,
    policy: &impl NamespacePolicy,
) -> Result<(), SyncError> {
    if matches!(kind, DatabaseKind::Shareable { .. })
        && policy.classify(namespace) == NamespaceKind::AppPrivate
    {
        return Err(LogError::AppPrivateNamespace.into());
    }
    Ok(())
}

/// Writer this participant appends as, when the role can append.
pub(crate) fn role_writer(state: &SyncState) -> Option<mutation_log::Writer> {
    match &state.role {
        SyncRole::Owner { principal, device }
        | SyncRole::Publisher { principal, device }
        | SyncRole::Contributor { principal, device } => {
            Some(writer_of(principal.clone(), device.clone()))
        }
        SyncRole::Receiver { .. } => None,
    }
}

fn publisher_device(state: &SyncState) -> Result<(&PrincipalId, &DeviceId), SyncError> {
    match &state.role {
        SyncRole::Publisher { principal, device } => Ok((principal, device)),
        SyncRole::Owner { .. } | SyncRole::Receiver { .. } | SyncRole::Contributor { .. } => {
            Err(SyncError::NotPublisher)
        }
    }
}
