//! Plan replay over a bounded set of verified mutation log files.

use crate::canonical::push_field;
use crate::portable_mutation::{
    MutationEntry, PortableMutationError, SchemaIdentity, StreamId, StreamProgress,
    MAX_MUTATION_ENTRY_BYTES,
};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::str::FromStr;

const MAGIC: &[u8] = b"LASTDB-MUTATION-LOG\0\x01";

pub const MAX_LOG_BYTES: usize = 4 * 1024 * 1024;
pub const MAX_LOG_ENTRIES: usize = 256;
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
    UnknownSchema,
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
            Self::UnknownSchema => {
                "mutation names a schema neither installed nor defined earlier in its stream"
            }
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

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct LogRangeId {
    source_stream: StreamId,
    first_sequence: u64,
    last_sequence: u64,
}

impl LogRangeId {
    pub fn new(
        source_stream: StreamId,
        first_sequence: u64,
        last_sequence: u64,
    ) -> Result<Self, ReplayError> {
        if first_sequence == 0 || last_sequence < first_sequence {
            return Err(ReplayError::InvalidRange);
        }
        if last_sequence - first_sequence >= MAX_LOG_ENTRIES as u64 {
            return Err(ReplayError::LogLimit);
        }
        Ok(Self {
            source_stream,
            first_sequence,
            last_sequence,
        })
    }

    pub fn source_stream(&self) -> &StreamId {
        &self.source_stream
    }

    pub fn first_sequence(&self) -> u64 {
        self.first_sequence
    }

    pub fn last_sequence(&self) -> u64 {
        self.last_sequence
    }

    fn entry_count(&self) -> usize {
        (self.last_sequence - self.first_sequence + 1) as usize
    }

    fn overlaps(&self, other: &Self) -> bool {
        self.source_stream == other.source_stream
            && self.first_sequence <= other.last_sequence
            && other.first_sequence <= self.last_sequence
    }
}

impl fmt::Display for LogRangeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}/{:020}-{:020}.log",
            self.source_stream, self.first_sequence, self.last_sequence
        )
    }
}

impl FromStr for LogRangeId {
    type Err = ReplayError;

    fn from_str(path: &str) -> Result<Self, Self::Err> {
        let (source, endpoints) = path
            .strip_suffix(".log")
            .and_then(|stem| stem.split_once('/'))
            .ok_or(ReplayError::InvalidRangeName)?;
        let (first, last) = endpoints
            .split_once('-')
            .ok_or(ReplayError::InvalidRangeName)?;
        let source = source.parse().map_err(|_| ReplayError::InvalidRangeName)?;
        Self::new(source, parse_endpoint(first)?, parse_endpoint(last)?)
    }
}

fn parse_endpoint(value: &str) -> Result<u64, ReplayError> {
    if value.len() != 20 || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(ReplayError::InvalidRangeName);
    }
    value.parse().map_err(|_| ReplayError::InvalidRangeName)
}

pub fn encode_log_range(
    id: &LogRangeId,
    entries: &[MutationEntry],
) -> Result<Vec<u8>, ReplayError> {
    if entries.len() != id.entry_count() {
        return Err(ReplayError::InvalidRange);
    }
    let last = entries.last().ok_or(ReplayError::InvalidRange)?;
    let mut bytes = MAGIC.to_vec();
    bytes.extend_from_slice(&last.stamp.authored_at_ms.to_be_bytes());
    let mut previous = None;
    for (entry, sequence) in entries.iter().zip(id.first_sequence..=id.last_sequence) {
        verify_order(id, entry, sequence, previous)?;
        let entry_bytes = entry.encode()?;
        if bytes.len() + size_of::<u32>() + entry_bytes.len() > MAX_LOG_BYTES {
            return Err(ReplayError::LogLimit);
        }
        push_field(&mut bytes, &entry_bytes);
        previous = Some(entry.stamp.authored_at_ms);
    }
    Ok(bytes)
}

pub struct VerifiedLogRange {
    id: LogRangeId,
    last_authored_at_ms: u64,
    entries: Vec<MutationEntry>,
    encoded_bytes: usize,
    digest: [u8; 32],
}

impl VerifiedLogRange {
    pub fn decode(id: LogRangeId, bytes: &[u8]) -> Result<Self, ReplayError> {
        if bytes.len() > MAX_LOG_BYTES {
            return Err(ReplayError::LogLimit);
        }
        let body = bytes.strip_prefix(MAGIC).ok_or(ReplayError::InvalidFrame)?;
        let (timestamp, mut remaining) =
            body.split_first_chunk().ok_or(ReplayError::InvalidFrame)?;
        let last_authored_at_ms = u64::from_be_bytes(*timestamp);
        let mut entries = Vec::with_capacity(id.entry_count());
        let mut previous = None;
        for sequence in id.first_sequence..=id.last_sequence {
            let (entry, rest) = read_entry(remaining)?;
            verify_order(&id, &entry, sequence, previous)?;
            previous = Some(entry.stamp.authored_at_ms);
            entries.push(entry);
            remaining = rest;
        }
        if !remaining.is_empty() {
            return Err(ReplayError::InvalidFrame);
        }
        if previous != Some(last_authored_at_ms) {
            return Err(ReplayError::HeaderTimestampMismatch);
        }
        Ok(Self {
            id,
            last_authored_at_ms,
            entries,
            encoded_bytes: bytes.len(),
            digest: Sha256::digest(bytes).into(),
        })
    }

    pub fn id(&self) -> &LogRangeId {
        &self.id
    }

    pub fn last_authored_at_ms(&self) -> u64 {
        self.last_authored_at_ms
    }

    pub fn entries(&self) -> &[MutationEntry] {
        &self.entries
    }

    fn encoded_bytes(&self) -> usize {
        self.encoded_bytes
    }

    fn same_content(&self, other: &Self) -> bool {
        self.id == other.id && self.digest == other.digest
    }

    fn contiguous_suffix(
        &self,
        progress: StreamProgress,
        delete_reclaimed_through_ms: Option<u64>,
    ) -> Result<Option<&[MutationEntry]>, ReplayError> {
        if self.id.last_sequence <= progress.applied_through {
            return Ok(None);
        }
        let next = progress.applied_through + 1;
        if next < self.id.first_sequence {
            return Ok(None);
        }
        let suffix = &self.entries[(next - self.id.first_sequence) as usize..];
        let first_authored_at_ms = suffix[0].stamp.authored_at_ms;
        if progress
            .applied_through_ms
            .is_some_and(|time| first_authored_at_ms < time)
        {
            return Err(ReplayError::TimestampRegression);
        }
        if delete_reclaimed_through_ms.is_some_and(|cutoff| first_authored_at_ms <= cutoff) {
            return Err(ReplayError::ReclaimedHistory);
        }
        Ok(Some(suffix))
    }
}

fn read_entry(bytes: &[u8]) -> Result<(MutationEntry, &[u8]), ReplayError> {
    let (length, body) = bytes.split_first_chunk().ok_or(ReplayError::InvalidFrame)?;
    let length = u32::from_be_bytes(*length) as usize;
    if length == 0 || length > MAX_MUTATION_ENTRY_BYTES {
        return Err(ReplayError::InvalidFrame);
    }
    let (entry, rest) = body
        .split_at_checked(length)
        .ok_or(ReplayError::InvalidFrame)?;
    Ok((MutationEntry::decode(entry)?, rest))
}

fn verify_order(
    id: &LogRangeId,
    entry: &MutationEntry,
    sequence: u64,
    previous: Option<u64>,
) -> Result<(), ReplayError> {
    if entry.stamp.position.source_stream != id.source_stream
        || entry.stamp.position.sequence != sequence
    {
        return Err(ReplayError::WrongPosition);
    }
    if previous.is_some_and(|time| entry.stamp.authored_at_ms < time) {
        return Err(ReplayError::TimestampRegression);
    }
    Ok(())
}

pub struct ReplayPlan<'a> {
    definitions: Vec<&'a MutationEntry>,
    candidates: Vec<&'a MutationEntry>,
    progress: BTreeMap<StreamId, StreamProgress>,
}

impl<'a> ReplayPlan<'a> {
    pub fn definitions(&self) -> &[&'a MutationEntry] {
        &self.definitions
    }

    pub fn candidates(&self) -> &[&'a MutationEntry] {
        &self.candidates
    }

    pub fn progress(&self) -> &BTreeMap<StreamId, StreamProgress> {
        &self.progress
    }
}

pub fn plan_replay<'a>(
    ranges: &'a [VerifiedLogRange],
    applied: &BTreeMap<StreamId, StreamProgress>,
    delete_reclaimed_through_ms: Option<u64>,
    installed: impl Fn(&SchemaIdentity) -> bool,
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
    let mut definitions = Vec::new();
    let mut defined = BTreeMap::<&StreamId, BTreeSet<&SchemaIdentity>>::new();
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
            .unwrap_or_default();
        cursor.validate()?;
        if let Some(suffix) = range.contiguous_suffix(cursor, delete_reclaimed_through_ms)? {
            let defined = defined.entry(source).or_default();
            for entry in suffix {
                if entry.definition().is_some() {
                    defined.insert(&entry.schema);
                    definitions.push(entry);
                } else if installed(&entry.schema) || defined.contains(&entry.schema) {
                    candidates.push(entry);
                } else {
                    return Err(ReplayError::UnknownSchema);
                }
            }
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
        definitions,
        candidates,
        progress,
    })
}
