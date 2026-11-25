//! Record LWW payload convention.
//!
//! Data entry `payload` bytes are a whole-record envelope:
//!
//! ```text
//! u16 record_key_len (big-endian)
//! record_key UTF-8 bytes
//! value_bytes…          // empty value_bytes = tombstone (delete)
//! ```
//!
//! Canonical record state is whole-record last-writer-wins by position
//! `(lamport, node_key, device_id)`. Deletes are tombstones that carry a
//! position. No field merging.

use thiserror::Error;

use crate::ids::{DeviceId, EntryHash, NodeKey};

/// Failure decoding a record payload envelope.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum PayloadError {
    /// Envelope shorter than the key length prefix.
    #[error("record payload too short")]
    TooShort,
    /// Declared key length exceeds remaining bytes.
    #[error("record key length exceeds payload")]
    KeyTruncated,
    /// Record key is not valid UTF-8.
    #[error("record key is not UTF-8")]
    KeyNotUtf8,
}

/// Decoded whole-record payload.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecordPayload {
    /// Record key within the data entry's namespace.
    pub record_key: String,
    /// Value bytes; empty means tombstone.
    pub value: Vec<u8>,
}

impl RecordPayload {
    /// Encode the envelope.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        encode_record_payload(&self.record_key, &self.value)
    }
}

/// Encode `u16 key_len + key + value`.
#[must_use]
pub fn encode_record_payload(record_key: &str, value: &[u8]) -> Vec<u8> {
    let key = record_key.as_bytes();
    let mut out = Vec::with_capacity(2 + key.len() + value.len());
    out.extend_from_slice(&(key.len() as u16).to_be_bytes());
    out.extend_from_slice(key);
    out.extend_from_slice(value);
    out
}

/// Decode the envelope.
pub fn decode_record_payload(bytes: &[u8]) -> Result<RecordPayload, PayloadError> {
    if bytes.len() < 2 {
        return Err(PayloadError::TooShort);
    }
    let key_len = u16::from_be_bytes([bytes[0], bytes[1]]) as usize;
    let rest = &bytes[2..];
    if rest.len() < key_len {
        return Err(PayloadError::KeyTruncated);
    }
    let key = std::str::from_utf8(&rest[..key_len]).map_err(|_| PayloadError::KeyNotUtf8)?;
    Ok(RecordPayload {
        record_key: key.to_owned(),
        value: rest[key_len..].to_vec(),
    })
}

/// Winning LWW position for one record.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RecordPosition {
    /// Lamport of the winning entry.
    pub lamport: u64,
    /// Writer node key.
    pub node_key: NodeKey,
    /// Writer device id.
    pub device_id: DeviceId,
}

impl RecordPosition {
    /// Compare for whole-record LWW: higher `(lamport, node_key, device_id)` wins.
    #[must_use]
    pub fn beats(self, other: Self) -> bool {
        (self.lamport, self.node_key, self.device_id)
            > (other.lamport, other.node_key, other.device_id)
    }
}

/// Materialized record value after LWW (tombstone retained until replaced).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MaterializedRecord {
    /// Winning position.
    pub position: RecordPosition,
    /// Content author for FoldDB stamps: the signed writer, or the proposal
    /// member when a publisher incorporation cites `incorporated_from`.
    pub author: NodeKey,
    /// Hash of the winning entry. Every node holds the same entry, so this is
    /// the record revision that compare-and-set across nodes names.
    pub entry: EntryHash,
    /// Writer of the earliest accepted write to this key by position, whoever
    /// wins now. A member may change only a record it created, so ownership is
    /// this key on every node, whatever order entries arrive in.
    pub creator: NodeKey,
    /// Position of the write that `creator` signed.
    pub(crate) creator_position: RecordPosition,
    /// Current value; empty = tombstone present.
    pub value: Vec<u8>,
}
