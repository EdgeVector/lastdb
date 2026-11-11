use crate::auth::{holds_receive, is_publisher, submit_streams};
use crate::{DatabaseKind, LogState, NamespaceKind, NamespacePolicy, PrincipalId, StoredEntry};

/// Sequence space a reader is allowed to observe.
///
/// A pure receiver uses the publisher stream's writer sequence. A pure
/// contributor uses that submission stream's writer sequence. Neither number
/// moves when another stream appends. Readers of more than one stream use
/// delivery sequence, because one writer sequence cannot name both.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CursorSpace {
    Delivery,
    Stream(crate::StreamId),
}

/// An ack stored with the sequence space it counts in.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AckCursor {
    /// Space `through` was read in.
    pub space: CursorSpace,
    /// Highest cursor acknowledged in `space`. Zero means the start.
    pub through: u64,
}

pub fn cursor_space(state: &LogState, reader: &PrincipalId) -> CursorSpace {
    if is_publisher(state, reader) {
        return CursorSpace::Delivery;
    }
    match &state.kind {
        DatabaseKind::Personal => CursorSpace::Delivery,
        DatabaseKind::Shareable {
            publisher_stream, ..
        } => {
            let submits = submit_streams(state, reader);
            let receive = holds_receive(state, reader);
            if receive && submits.is_empty() {
                CursorSpace::Stream(publisher_stream.clone())
            } else if !receive && submits.len() == 1 {
                CursorSpace::Stream(submits[0].clone())
            } else {
                CursorSpace::Delivery
            }
        }
    }
}

pub(crate) fn cursor_of(space: &CursorSpace, stored: &StoredEntry) -> u64 {
    match space {
        CursorSpace::Stream(stream) if &stored.entry.stream == stream => stored.entry.writer_seq,
        CursorSpace::Delivery | CursorSpace::Stream(_) => stored.delivery_seq,
    }
}

pub(crate) fn max_visible_cursor(
    state: &LogState,
    reader: &PrincipalId,
    policy: &impl NamespacePolicy,
) -> u64 {
    let space = cursor_space(state, reader);
    state
        .entries
        .iter()
        .filter(|stored| crate::entry_visible(state, stored, reader, policy))
        .map(|stored| cursor_of(&space, stored))
        .max()
        .unwrap_or(0)
}

/// Global delivery prefix this visible cursor allows the log to drop.
///
/// A reader who has seen every entry they can see covers the raw head, so
/// trailing hidden entries do not stay pinned by that reader. A reader who
/// still has a visible entry ahead covers only through the entry they named.
pub(crate) fn coverage(
    state: &LogState,
    reader: &PrincipalId,
    through: u64,
    policy: &impl NamespacePolicy,
) -> u64 {
    let max_cursor = max_visible_cursor(state, reader, policy);
    let head = state.head.as_ref().map_or(0, |head| head.delivery_seq);
    if through >= max_cursor {
        return head;
    }
    let space = cursor_space(state, reader);
    state
        .entries
        .iter()
        .filter(|stored| {
            crate::entry_visible(state, stored, reader, policy)
                && cursor_of(&space, stored) <= through
        })
        .map(|stored| stored.delivery_seq)
        .max()
        .unwrap_or(0)
}

pub(crate) fn expiry_mark(
    state: &LogState,
    reader: &PrincipalId,
    floor: u64,
    policy: &impl NamespacePolicy,
) -> u64 {
    let space = cursor_space(state, reader);
    match &space {
        CursorSpace::Delivery => floor,
        CursorSpace::Stream(stream) => {
            let trimmed = state
                .streams
                .get(stream)
                .map_or(0, |progress| progress.trimmed_writer_seq);
            let still_held = state
                .entries
                .iter()
                .filter(|stored| {
                    &stored.entry.stream == stream
                        && stored.delivery_seq <= floor
                        && crate::entry_visible(state, stored, reader, policy)
                })
                .map(|stored| stored.entry.writer_seq)
                .max()
                .unwrap_or(0);
            trimmed.max(still_held)
        }
    }
}

pub(crate) fn snapshot_mark(
    state: &LogState,
    reader: &PrincipalId,
    policy: &impl NamespacePolicy,
) -> u64 {
    let through = state
        .snapshot
        .as_ref()
        .map_or(0, |cut| cut.through_delivery_seq);
    expiry_mark(state, reader, through, policy)
}

pub(crate) fn remember_trimmed(state: &mut LogState, floor: u64, policy: &impl NamespacePolicy) {
    let dropped: Vec<(crate::StreamId, u64)> = state
        .entries
        .iter()
        .filter(|stored| stored.delivery_seq <= floor && counts_for_cursor(state, stored, policy))
        .map(|stored| (stored.entry.stream.clone(), stored.entry.writer_seq))
        .collect();
    for (stream, seq) in dropped {
        if let Some(progress) = state.streams.get_mut(&stream) {
            progress.trimmed_writer_seq = progress.trimmed_writer_seq.max(seq);
        }
    }
}

fn counts_for_cursor(
    state: &LogState,
    stored: &StoredEntry,
    policy: &impl NamespacePolicy,
) -> bool {
    if !matches!(state.kind, DatabaseKind::Shareable { .. }) {
        return true;
    }
    policy.classify(&stored.entry.namespace) == NamespaceKind::Official
}
