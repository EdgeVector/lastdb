use crate::error::SyncError;
use crate::model::{SyncEvent, SyncRole, SyncState};
use mutation_log::{
    AppendBatch, DatabaseKind, Entry, Incorporation, LogError, Namespace, NamespaceKind,
    NamespacePolicy, StreamId,
};

/// An effect [`crate::drive`] applies through the port. The log is not here.
pub(crate) enum Effect {
    Append(AppendBatch),
    Pull {
        reader: mutation_log::PrincipalId,
        cursor: crate::ReadCursor,
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
            cursor: state.cursor.clone(),
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
    let author = writer(state)?.clone();
    let stream = write_stream(state)?.clone();
    if !state.resumed {
        return Err(SyncError::NeedsResume);
    }
    reject_app_private(&state.kind, &namespace, policy)?;
    let entry = Entry {
        database_id: state.database_id.clone(),
        stream: stream.clone(),
        writer_seq: state.next_seq,
        authored_at_ms,
        author: author.clone(),
        namespace,
        incorporated_from: None,
        payload,
    };
    Ok(Effect::Append(AppendBatch {
        author,
        stream,
        entries: vec![entry],
        accepted_at_ms,
    }))
}

fn reappend_effect(
    state: &SyncState,
    mut entry: Entry,
    accepted_at_ms: u64,
    policy: &impl NamespacePolicy,
) -> Result<Effect, SyncError> {
    let SyncRole::Publisher { principal } = &state.role else {
        return Err(SyncError::NotPublisher);
    };
    if !state.resumed {
        return Err(SyncError::NeedsResume);
    }
    let publisher_stream = publisher_stream(state)?;
    if entry.database_id != state.database_id {
        return Err(LogError::WrongDatabase.into());
    }
    if entry.stream == *publisher_stream {
        return Err(SyncError::NotSubmission);
    }
    reject_app_private(&state.kind, &entry.namespace, policy)?;
    let author = principal.clone();
    let stream = publisher_stream.clone();
    entry.incorporated_from = Some(Incorporation {
        submission_stream: entry.stream.clone(),
        writer_seq: entry.writer_seq,
        contributor: entry.author.clone(),
    });
    entry.stream = stream.clone();
    entry.writer_seq = state.next_seq;
    entry.author = author.clone();
    Ok(Effect::Append(AppendBatch {
        author,
        stream,
        entries: vec![entry],
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

fn writer(state: &SyncState) -> Result<&mutation_log::PrincipalId, SyncError> {
    match &state.role {
        SyncRole::Device { principal, .. }
        | SyncRole::Publisher { principal }
        | SyncRole::Contributor { principal, .. } => Ok(principal),
        SyncRole::Receiver { .. } => Err(SyncError::ReadOnly),
    }
}

pub(crate) fn write_stream(state: &SyncState) -> Result<&StreamId, SyncError> {
    match &state.role {
        SyncRole::Device { stream, .. } | SyncRole::Contributor { stream, .. } => Ok(stream),
        SyncRole::Publisher { .. } => publisher_stream(state),
        SyncRole::Receiver { .. } => Err(SyncError::ReadOnly),
    }
}

fn publisher_stream(state: &SyncState) -> Result<&StreamId, SyncError> {
    match &state.kind {
        DatabaseKind::Shareable {
            publisher_stream, ..
        } => Ok(publisher_stream),
        DatabaseKind::Personal => Err(LogError::PersonalNotShareable.into()),
    }
}
