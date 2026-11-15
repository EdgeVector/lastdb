//! Shared-sync step for the unified mutation log.
//!
//! One participant proposes an effect. [`drive`] applies that effect through
//! an injected [`MutationLog`] and returns the next participant state.
//! Personal writers and share participants use this same step. [`drive`] is
//! the only step. There is no flag and no second path. This crate does not
//! read the process environment, open a socket, or call Exemem.
//! It depends on the [`MutationLog`] port, not on an adapter.

#![deny(clippy::disallowed_methods, clippy::disallowed_types)]

mod model;

use std::fmt;

use model::writer_of;
pub use model::{Outcome, SyncEvent, SyncRole, SyncState};
pub use mutation_log::MutationLog;
use mutation_log::{
    AppendBatch, AppendOutcome, DatabaseKind, Entry, LogError, Namespace, NamespaceKind,
    NamespacePolicy, Writer, LIST_PAGE_LIMIT,
};

/// A refused sync step. The caller's previous participant state is unchanged.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SyncError {
    /// The log refused the effect.
    Log(LogError),
    /// A receiver has nothing to append.
    ReadOnly,
    /// Append ran before [`SyncState::resume`] loaded the writer tip.
    NeedsResume,
}

impl From<LogError> for SyncError {
    fn from(error: LogError) -> Self {
        Self::Log(error)
    }
}

impl fmt::Display for SyncError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Log(error) => write!(formatter, "{error}"),
            Self::ReadOnly => write!(formatter, "a receiver cannot append"),
            Self::NeedsResume => write!(formatter, "resume the writer sequence before appending"),
        }
    }
}

impl std::error::Error for SyncError {}

enum Effect {
    Append(AppendBatch),
    Pull {
        reader: Writer,
        after: u64,
        now_ms: u64,
    },
}

/// Apply one participant event through `log`.
///
/// On `Err`, `state` is unchanged. The port's own step also keeps its previous
/// value when an append fails. A pull returns a page; a cursor expiry runs
/// recovery without moving the failed cursor beforehand.
pub fn drive<L: MutationLog>(
    state: &SyncState,
    log: &mut L,
    event: SyncEvent,
    policy: &impl NamespacePolicy,
) -> Result<(SyncState, Outcome), SyncError> {
    let effect = propose(state, event, policy)?;
    let outcome = apply(log, effect)?;
    Ok((commit(state, &outcome), outcome))
}

fn propose(
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
            reader: state.read_writer(),
            after: state.cursor,
            now_ms,
        }),
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

pub(crate) fn role_writer(state: &SyncState) -> Option<Writer> {
    match &state.role {
        SyncRole::Owner { principal, device }
        | SyncRole::Publisher { principal, device }
        | SyncRole::Contributor { principal, device } => {
            Some(writer_of(principal.clone(), device.clone()))
        }
        SyncRole::Receiver { .. } => None,
    }
}

fn apply<L: MutationLog>(log: &mut L, effect: Effect) -> Result<Outcome, SyncError> {
    match effect {
        Effect::Append(batch) => append(log, batch),
        Effect::Pull {
            reader,
            after,
            now_ms,
        } => pull(log, &reader, after, now_ms),
    }
}

fn append<L: MutationLog>(log: &mut L, batch: AppendBatch) -> Result<Outcome, SyncError> {
    let last = batch.entries.last().ok_or(LogError::EmptyBatch)?;
    let next_seq = last
        .writer_seq
        .checked_add(1)
        .ok_or(LogError::SequenceExhausted)?;
    match log.append(batch)? {
        AppendOutcome::Accepted { delivery_seqs } => Ok(Outcome::Appended {
            delivery_seqs,
            next_seq,
        }),
        AppendOutcome::Duplicate { next_seq } => Ok(Outcome::Duplicate { next_seq }),
    }
}

fn pull<L: MutationLog>(
    log: &mut L,
    reader: &Writer,
    after: u64,
    now_ms: u64,
) -> Result<Outcome, SyncError> {
    match log.list(&reader.key, after, LIST_PAGE_LIMIT, now_ms) {
        Ok(page) => Ok(Outcome::Pulled {
            cursor: page.next,
            entries: page.entries,
            reset: None,
        }),
        Err(LogError::CursorExpired { .. }) => {
            let recovered = log.recover(reader, after, now_ms)?;
            Ok(Outcome::Pulled {
                reset: recovered.reset,
                entries: recovered.entries,
                cursor: recovered.next,
            })
        }
        Err(error) => Err(error.into()),
    }
}

fn commit(state: &SyncState, outcome: &Outcome) -> SyncState {
    let mut next = state.clone();
    match outcome {
        Outcome::Appended { next_seq, .. } | Outcome::Duplicate { next_seq } => {
            next.next_seq = *next_seq;
        }
        Outcome::Pulled { cursor, .. } => next.cursor = *cursor,
    }
    next
}
