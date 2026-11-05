use super::ReplayError;
use crate::canonical::push_field;
use crate::portable_mutation::{MutationEntry, StreamId, StreamProgress, MAX_MUTATION_ENTRY_BYTES};
use sha2::{Digest, Sha256};
use std::fmt;
use std::str::FromStr;

const MAGIC: &[u8] = b"LASTDB-MUTATION-LOG\0\x01";

/// Maximum complete framed size, excluding separately stored file payloads.
pub const MAX_LOG_BYTES: usize = 4 * 1024 * 1024;
pub const MAX_LOG_ENTRIES: usize = 256;

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

    pub(super) fn overlaps(&self, other: &Self) -> bool {
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

/// Canonical file framing is a version tag, one big-endian u64 header
/// timestamp, then a big-endian u32 length and canonical entry for each sequence.
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

/// Structurally verified plaintext bound to its expected file identity.
/// The caller authenticates the source, range and bytes before admitting it.
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

    pub(super) fn encoded_bytes(&self) -> usize {
        self.encoded_bytes
    }

    pub(super) fn same_content(&self, other: &Self) -> bool {
        self.id == other.id && self.digest == other.digest
    }

    pub(super) fn contiguous_suffix(
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
