//! Plan replay over a bounded set of verified mutation log files.
//!
//! Storage owns authentication, inbox persistence and the native write. This
//! module consumes immutable ranges and returns their effective candidates and
//! cursor updates without changing a home.

use crate::portable_mutation::{MutationEntry, PortableMutationError, StreamId, StreamProgress};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

mod range;
pub use range::{encode_log_range, LogRangeId, VerifiedLogRange, MAX_LOG_BYTES, MAX_LOG_ENTRIES};

/// Bound the number and total encoded size of the files in one plan.
pub const MAX_REPLAY_RANGES: usize = 64;
pub const MAX_REPLAY_BYTES: usize = 16 * 1024 * 1024;

#[derive(Debug)]
pub enum ReplayError {
    InvalidRangeName,
    InvalidRange,
    InvalidFrame,
    WrongPosition,
    TimestampRegression,
    HeaderTimestampMismatch,
    ReclaimedHistory,
    OverlappingRanges,
    WindowLimit,
    LogLimit,
    InvalidMutation(PortableMutationError),
}

impl fmt::Display for ReplayError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::InvalidRangeName => "invalid mutation log file name",
            Self::InvalidRange => "invalid mutation log sequence range",
            Self::InvalidFrame => "invalid mutation log file framing",
            Self::WrongPosition => "mutation position differs from its log range",
            Self::TimestampRegression => "mutation log authored timestamp moved backward",
            Self::HeaderTimestampMismatch => "mutation log header differs from its final timestamp",
            Self::ReclaimedHistory => "unapplied mutation predates the admitted delete boundary",
            Self::OverlappingRanges => "mutation log ranges overlap with conflicting input",
            Self::WindowLimit => "mutation log replay window exceeds its bounds",
            Self::LogLimit => "mutation log file exceeds its bounds",
            Self::InvalidMutation(_) => "invalid portable mutation or stream progress",
        })
    }
}

impl std::error::Error for ReplayError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::InvalidMutation(error) => Some(error),
            _ => None,
        }
    }
}

impl From<PortableMutationError> for ReplayError {
    fn from(error: PortableMutationError) -> Self {
        Self::InvalidMutation(error)
    }
}

pub struct ReplayPlan<'a> {
    candidates: Vec<&'a MutationEntry>,
    progress: BTreeMap<StreamId, StreamProgress>,
}

impl<'a> ReplayPlan<'a> {
    /// The newest candidate for each affected primary key, newest first.
    /// The writer must compare each against the live/delete stamp in its write scope.
    pub fn candidates(&self) -> &[&'a MutationEntry] {
        &self.candidates
    }

    /// Updated streams only. Commit this with the winning effects even if
    /// some or all candidates are stale.
    pub fn progress(&self) -> &BTreeMap<StreamId, StreamProgress> {
        &self.progress
    }
}

/// Select complete gap-free suffixes. A restore supplies ranges at or below
/// its captured heads and checks that the committed cursors reach those heads.
/// Large restores can spool candidates by primary key across bounded windows
/// before fetching winning file versions or activating the staged home.
pub fn plan_replay<'a>(
    ranges: &'a [VerifiedLogRange],
    applied: &BTreeMap<StreamId, StreamProgress>,
    delete_reclaimed_through_ms: Option<u64>,
) -> Result<ReplayPlan<'a>, ReplayError> {
    if ranges.len() > MAX_REPLAY_RANGES
        || ranges
            .iter()
            .map(VerifiedLogRange::encoded_bytes)
            .sum::<usize>()
            > MAX_REPLAY_BYTES
    {
        return Err(ReplayError::WindowLimit);
    }
    let mut ordered: Vec<_> = ranges.iter().collect();
    ordered.sort_unstable_by(|left, right| left.id().cmp(right.id()));

    let mut previous: Option<&VerifiedLogRange> = None;
    let mut candidates = Vec::new();
    let mut progress = BTreeMap::new();
    for range in ordered {
        if let Some(prior) = previous.filter(|prior| prior.id().overlaps(range.id())) {
            if !range.same_content(prior) {
                return Err(ReplayError::OverlappingRanges);
            }
            continue;
        }
        previous = Some(range);

        let source = range.id().source_stream();
        let mut cursor = progress
            .get(source)
            .or_else(|| applied.get(source))
            .copied()
            .unwrap_or(StreamProgress {
                applied_through: 0,
                applied_through_ms: None,
                uploaded_through: None,
            });
        cursor.validate()?;
        if let Some(suffix) = range.contiguous_suffix(cursor, delete_reclaimed_through_ms)? {
            candidates.extend(suffix);
            cursor.applied_through = range.id().last_sequence();
            cursor.applied_through_ms = Some(range.last_authored_at_ms());
            progress.insert(source.clone(), cursor);
        }
    }

    candidates.sort_unstable_by(|left, right| right.stamp.cmp(&left.stamp));
    let mut keys = BTreeSet::new();
    candidates.retain(|entry| {
        let entry = *entry;
        keys.insert((&entry.schema, entry.operation.primary_key()))
    });
    Ok(ReplayPlan {
        candidates,
        progress,
    })
}
