use crate::error::SyncError;
use crate::model::{Outcome, SyncEvent, SyncState};
use crate::step::{propose, Effect};
use mutation_log::{AppendOutcome, LogError, MutationLog, LIST_PAGE_LIMIT};

/// Apply one participant event through `log`.
///
/// On `Err`, `state` is unchanged. The port's own step also keeps its previous
/// value when an append fails. A pull lists, then acks the scanned delivery
/// sequence. A cursor below the trim floor restarts at the latest snapshot.
pub fn drive<L: MutationLog>(
    state: &SyncState,
    log: &mut L,
    event: SyncEvent,
    policy: &impl mutation_log::NamespacePolicy,
) -> Result<(SyncState, Outcome), SyncError> {
    let effect = propose(state, event, policy)?;
    let outcome = apply(log, effect)?;
    Ok((commit(state, &outcome), outcome))
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

fn append<L: MutationLog>(
    log: &mut L,
    batch: mutation_log::AppendBatch,
) -> Result<Outcome, SyncError> {
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
    reader: &mutation_log::PrincipalId,
    after: u64,
    now_ms: u64,
) -> Result<Outcome, SyncError> {
    let page = match log.list(reader, after, LIST_PAGE_LIMIT, now_ms) {
        Ok(page) => page,
        Err(LogError::CursorExpired { snapshot_through }) => {
            log.list(reader, snapshot_through, LIST_PAGE_LIMIT, now_ms)?
        }
        Err(error) => return Err(error.into()),
    };
    log.ack(reader, page.next)?;
    Ok(Outcome::Pulled {
        cursor: page.next,
        entries: page.entries,
    })
}

pub(crate) fn commit(state: &SyncState, outcome: &Outcome) -> SyncState {
    let mut next = state.clone();
    match outcome {
        Outcome::Appended { next_seq, .. } | Outcome::Duplicate { next_seq } => {
            next.next_seq = *next_seq;
        }
        Outcome::Pulled { cursor, .. } => next.cursor = *cursor,
    }
    next
}
