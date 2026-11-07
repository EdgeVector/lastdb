//! One complete stored record, its external addresses and one record stamp.
//! There are no field stamps or partial merge operations in this format.
use crate::portable_mutation::{Position, Stamp};
use crate::record_replacement::RecordReplacement;
use crate::schema::types::KeyValue;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

mod file;
pub use crate::record_replacement::RecordMetadata as RecordMeta;
pub use file::{
    FileMarker, InlineFile, InlineWrite, MAX_INLINE_FILE_BYTES, MAX_INLINE_LABEL_BYTES,
    MAX_INLINE_STORED_BYTES,
};

pub const RECORD_VALUE_VERSION: u8 = 3;
pub const MAX_RECORD_BYTES: usize = 64 * 1024;

pub use crate::portable_mutation::Stamp as RecordStamp;

impl Stamp {
    /// Stable opaque version of this authored record, independent of field contents.
    #[must_use]
    pub fn revision(&self) -> String {
        let mut hash = Sha256::new();
        hash.update(b"lastdb-record-revision-v2\0");
        hash.update(self.authored_at_ms.to_be_bytes());
        hash.update(self.position.source_stream.as_str().as_bytes());
        hash.update(self.position.sequence.to_be_bytes());
        format!("{:x}", hash.finalize())
    }

    /// Return the write time in nanoseconds for retention callers.
    #[must_use]
    pub fn written_at_nanos(&self) -> u64 {
        self.authored_at_ms.saturating_mul(1_000_000)
    }

    #[must_use]
    pub fn mutation_identity(&self) -> String {
        format!("{}/{}", self.position.source_stream, self.position.sequence)
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file_version: Option<Position>,
    /// The marker of the record's inline file (a thumbnail, an icon). The
    /// bytes live in their own row, so a record read never carries them.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inline_file: Option<FileMarker>,
    #[serde(skip)]
    pub deleted: bool,
}

impl StoredRecord {
    #[must_use]
    pub fn replacement(
        value: RecordReplacement,
        stamp: RecordStamp,
        before: Option<&Self>,
    ) -> Self {
        let file_version = value.file.as_ref().map(|marker| {
            before
                .filter(|record| record.file.as_ref() == Some(marker))
                .and_then(|record| record.file_version.clone())
                .unwrap_or_else(|| stamp.position.clone())
        });
        Self {
            v: RECORD_VALUE_VERSION,
            key: value.target.key,
            alternates: value.target.alternates,
            fields: value.fields,
            stamp,
            meta: value.metadata,
            file: value.file,
            file_version,
            inline_file: value.inline_file,
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
        self.is_live().then(|| self.stamp.written_at_nanos())
    }

    /// An erased state for the write coordinator. It has no live record row.
    pub fn erased(stamp: RecordStamp) -> Self {
        Self {
            v: RECORD_VALUE_VERSION,
            key: KeyValue::default(),
            alternates: BTreeMap::new(),
            fields: BTreeMap::new(),
            stamp,
            meta: None,
            file: None,
            file_version: None,
            inline_file: None,
            deleted: true,
        }
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
            || self.deleted
            || self.key.hash.as_deref().is_none_or(str::is_empty)
            || self.stamp.position.validate().is_err()
        {
            return Err(RecordValueError::Format(
                "invalid record version, address or stamp".into(),
            ));
        }
        if self.file.is_some() != self.file_version.is_some()
            || self
                .file
                .as_ref()
                .is_some_and(|file| !file.has_valid_hash())
            || self.file_version.as_ref().is_some_and(|version| {
                version.validate().is_err()
                    || (version.source_stream == self.stamp.position.source_stream
                        && version.sequence > self.stamp.position.sequence)
            })
        {
            return Err(RecordValueError::Format(
                "the file marker has an invalid digest or version".into(),
            ));
        }
        if self
            .inline_file
            .as_ref()
            .is_some_and(|file| !file.fits_inline())
        {
            return Err(RecordValueError::Format(
                "the inline file marker has an invalid digest, label or size".into(),
            ));
        }
        Ok(())
    }
}
