//! One complete stored record, its external addresses and one record stamp.
//! There are no field stamps or partial merge operations in this format.
use crate::record_replacement::RecordReplacement;
use crate::schema::types::KeyValue;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

mod file;
pub use crate::record_replacement::RecordMetadata as RecordMeta;
pub use file::FileMarker;

pub const RECORD_VALUE_VERSION: u8 = 2;
pub const MAX_RECORD_BYTES: usize = 64 * 1024;

/// Local write identity and TTL time of the entire record.
#[derive(Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(from = "StampWire", into = "StampWire")]
pub struct RecordStamp {
    pub written_at: u64,
    pub logical_counter: u64,
    pub device_id: String,
    pub mutation_id: String,
}

#[derive(Serialize, Deserialize)]
struct StampWire(u64, u64, String, String);

impl From<StampWire> for RecordStamp {
    fn from(wire: StampWire) -> Self {
        Self {
            written_at: wire.0,
            logical_counter: wire.1,
            device_id: wire.2,
            mutation_id: wire.3,
        }
    }
}

impl From<RecordStamp> for StampWire {
    fn from(stamp: RecordStamp) -> Self {
        Self(
            stamp.written_at,
            stamp.logical_counter,
            stamp.device_id,
            stamp.mutation_id,
        )
    }
}

impl RecordStamp {
    /// Stable opaque version of this authored record, independent of field contents.
    #[must_use]
    pub fn revision(&self) -> String {
        let mut hash = Sha256::new();
        hash.update(b"lastdb-record-revision-v1\0");
        hash.update(self.written_at.to_be_bytes());
        hash.update(self.logical_counter.to_be_bytes());
        for value in [&self.device_id, &self.mutation_id] {
            hash.update((value.len() as u64).to_be_bytes());
            hash.update(value.as_bytes());
        }
        format!("{:x}", hash.finalize())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RecordValueError {
    Format(String),
    TooLarge { bytes: usize },
}

impl std::fmt::Display for RecordValueError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Format(detail) => write!(f, "invalid record value: {detail}"),
            Self::TooLarge { bytes } => write!(
                f,
                "record value is {bytes} bytes; maximum is {MAX_RECORD_BYTES}"
            ),
        }
    }
}

impl std::error::Error for RecordValueError {}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoredRecord {
    pub v: u8,
    pub key: KeyValue,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub alternates: BTreeMap<String, Option<KeyValue>>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub fields: BTreeMap<String, Value>,
    pub stamp: RecordStamp,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub meta: Option<RecordMeta>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file: Option<FileMarker>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub deleted: bool,
}

impl Default for StoredRecord {
    /// Internal absence only; cannot be encoded until a complete address/stamp is supplied.
    fn default() -> Self {
        Self {
            v: RECORD_VALUE_VERSION,
            key: KeyValue::new(None, None),
            alternates: BTreeMap::new(),
            fields: BTreeMap::new(),
            stamp: RecordStamp::default(),
            meta: None,
            file: None,
            deleted: true,
        }
    }
}

fn is_false(value: &bool) -> bool {
    !*value
}

impl StoredRecord {
    #[must_use]
    pub fn replacement(value: RecordReplacement, stamp: RecordStamp) -> Self {
        Self {
            v: RECORD_VALUE_VERSION,
            key: value.target.key,
            alternates: value.target.alternates,
            fields: value.fields,
            stamp,
            meta: value.metadata,
            file: value.file,
            deleted: false,
        }
    }

    /// An empty payload is still a live record.
    #[must_use]
    pub fn is_live(&self) -> bool {
        !self.deleted
    }

    #[must_use]
    pub fn written_at(&self) -> Option<u64> {
        (self.stamp.written_at != 0).then_some(self.stamp.written_at)
    }

    /// A whole-record erasure. No field can survive or merge across it.
    pub fn delete(&mut self, stamp: RecordStamp) -> bool {
        if !self.is_live() {
            return false;
        }
        self.fields.clear();
        self.meta = None;
        self.file = None;
        self.stamp = stamp;
        self.deleted = true;
        true
    }

    pub fn encode(&self) -> Result<Vec<u8>, RecordValueError> {
        let bytes = self.encode_uncapped()?;
        if bytes.len() > MAX_RECORD_BYTES {
            return Err(RecordValueError::TooLarge { bytes: bytes.len() });
        }
        Ok(bytes)
    }

    /// Final admission still enforces the configured complete-record size cap.
    pub fn encode_uncapped(&self) -> Result<Vec<u8>, RecordValueError> {
        self.validate_structure()?;
        serde_json::to_vec(self).map_err(|error| RecordValueError::Format(error.to_string()))
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, RecordValueError> {
        let record: Self = serde_json::from_slice(bytes)
            .map_err(|error| RecordValueError::Format(error.to_string()))?;
        record.validate_structure()?;
        Ok(record)
    }

    fn validate_structure(&self) -> Result<(), RecordValueError> {
        if self.v != RECORD_VALUE_VERSION
            || self.key.hash.as_deref().is_none_or(str::is_empty)
            || self.stamp.written_at == 0
            || self.stamp.device_id.is_empty()
            || self.stamp.mutation_id.is_empty()
        {
            return Err(RecordValueError::Format(
                "invalid record version, address or stamp".into(),
            ));
        }
        if self.deleted && (!self.fields.is_empty() || self.meta.is_some() || self.file.is_some()) {
            return Err(RecordValueError::Format(
                "a deleted record still has live content".into(),
            ));
        }
        if self
            .file
            .as_ref()
            .is_some_and(|file| !file.has_valid_hash())
        {
            return Err(RecordValueError::Format(
                "the file marker has an invalid sha256".into(),
            ));
        }
        Ok(())
    }
}
