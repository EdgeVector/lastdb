//! One complete stored record, its external addresses, file markers, and record stamp.

use crate::portable_mutation::{Position, Stamp};
use crate::record_replacement::RecordReplacement;
use crate::schema::types::KeyValue;
use base64::Engine as _;
use schema_types::PortableSchemaId;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

pub use crate::portable_mutation::Stamp as RecordStamp;
pub use crate::record_replacement::RecordMetadata as RecordMeta;

pub const RECORD_VALUE_VERSION: u8 = 3;
pub const MAX_RECORD_BYTES: usize = 64 * 1024;
pub const MAX_INLINE_FILE_BYTES: usize = 64 * 1024;
pub const MAX_INLINE_LABEL_BYTES: usize = 255;
const INLINE_MARKER_ALLOWANCE: usize = 1024;
pub const MAX_INLINE_STORED_BYTES: usize =
    MAX_INLINE_FILE_BYTES.div_ceil(3) * 4 + INLINE_MARKER_ALLOWANCE;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileMarker {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub media_type: Option<String>,
    pub size_bytes: u64,
    pub sha256: String,
}

impl FileMarker {
    #[must_use]
    pub fn has_valid_hash(&self) -> bool {
        self.sha256.len() == 64
            && self
                .sha256
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    }

    #[must_use]
    pub fn fits_inline(&self) -> bool {
        let label_fits = |label: &Option<String>| {
            label
                .as_ref()
                .is_none_or(|label| label.len() <= MAX_INLINE_LABEL_BYTES)
        };
        self.has_valid_hash()
            && label_fits(&self.name)
            && label_fits(&self.media_type)
            && usize::try_from(self.size_bytes).is_ok_and(|size| size <= MAX_INLINE_FILE_BYTES)
    }
}

/// Small file of one record stored in its own row in the `records` collection.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InlineFile {
    pub marker: FileMarker,
    pub data: String,
}

impl InlineFile {
    pub fn new(
        bytes: &[u8],
        name: Option<String>,
        media_type: Option<String>,
    ) -> Result<Self, String> {
        if bytes.len() > MAX_INLINE_FILE_BYTES {
            return Err(format!(
                "an inline file is {} bytes; maximum is {MAX_INLINE_FILE_BYTES}; store a larger file as the record's blob file",
                bytes.len()
            ));
        }
        let file = Self {
            marker: FileMarker {
                name,
                media_type,
                size_bytes: bytes.len() as u64,
                sha256: crate::hex::hex_lower(Sha256::digest(bytes)),
            },
            data: base64::engine::general_purpose::STANDARD.encode(bytes),
        };
        if !file.is_well_formed() {
            return Err(format!(
                "an inline file name or media type exceeds {MAX_INLINE_LABEL_BYTES} bytes"
            ));
        }
        Ok(file)
    }

    pub fn bytes(&self) -> Result<Vec<u8>, String> {
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(self.data.as_bytes())
            .map_err(|error| format!("inline file data is invalid base64: {error}"))?;
        if bytes.len() as u64 != self.marker.size_bytes
            || crate::hex::hex_lower(Sha256::digest(&bytes)) != self.marker.sha256
        {
            return Err("inline file bytes differ from their marker".into());
        }
        Ok(bytes)
    }

    #[must_use]
    pub fn stored_allowance(&self) -> usize {
        self.data.len() + INLINE_MARKER_ALLOWANCE
    }

    #[must_use]
    pub fn is_well_formed(&self) -> bool {
        self.marker.fits_inline()
            && usize::try_from(self.marker.size_bytes)
                .is_ok_and(|size| self.data.len() == size.div_ceil(3) * 4)
    }

    pub fn encode(&self) -> Result<Vec<u8>, String> {
        if !self.is_well_formed() {
            return Err("the inline file has an invalid marker, size or encoding".into());
        }
        serde_json::to_vec(self).map_err(|error| format!("encode inline file: {error}"))
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() > MAX_INLINE_STORED_BYTES {
            return Err("an inline file row exceeds its bound".into());
        }
        let file: Self = serde_json::from_slice(bytes)
            .map_err(|error| format!("inline file row format: {error}"))?;
        if !file.is_well_formed() {
            return Err("the inline file has an invalid marker, size or encoding".into());
        }
        file.bytes()?;
        Ok(file)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum InlineWrite {
    Put(InlineFile),
    Clear,
}

impl Stamp {
    #[must_use]
    pub fn revision(&self) -> String {
        let mut hash = Sha256::new();
        hash.update(b"lastdb-record-revision-v2\0");
        hash.update(self.authored_at_ms.to_be_bytes());
        hash.update(self.position.source_stream.as_str().as_bytes());
        hash.update(self.position.sequence.to_be_bytes());
        format!("{:x}", hash.finalize())
    }

    #[must_use]
    pub fn written_at_nanos(&self) -> u64 {
        self.authored_at_ms.saturating_mul(1_000_000)
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inline_file: Option<FileMarker>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub definition_id: Option<PortableSchemaId>,
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
            definition_id: None,
            deleted: false,
        }
    }

    #[must_use]
    pub fn is_live(&self) -> bool {
        !self.deleted
    }

    #[must_use]
    pub fn written_at(&self) -> Option<u64> {
        self.is_live().then(|| self.stamp.written_at_nanos())
    }

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
            definition_id: None,
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
