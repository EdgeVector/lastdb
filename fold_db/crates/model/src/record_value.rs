//! One complete stored record, its external addresses, file markers, and record stamp.

use crate::field_key_codec::record_replacement::RecordReplacement;
use crate::portable_mutation::{Position, Stamp};
use crate::schema::types::KeyValue;
use base64::{engine::general_purpose::STANDARD, Engine as _};
use schema_types::PortableSchemaId;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

pub use crate::field_key_codec::record_replacement::RecordMetadata as RecordMeta;
pub use crate::portable_mutation::Stamp as RecordStamp;

pub const RECORD_VALUE_VERSION: u8 = 3;
pub const MAX_RECORD_BYTES: usize = 64 * 1024;
pub const MAX_INLINE_FILE_BYTES: usize = 64 * 1024;
pub const MAX_INLINE_LABEL_BYTES: usize = 255;
pub const MAX_INLINE_STORED_BYTES: usize = MAX_INLINE_FILE_BYTES.div_ceil(3) * 4 + 1024;

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
                .all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
    }

    #[must_use]
    pub fn fits_inline(&self) -> bool {
        let ok = |s: &Option<String>| s.as_ref().is_none_or(|l| l.len() <= MAX_INLINE_LABEL_BYTES);
        self.has_valid_hash()
            && ok(&self.name)
            && ok(&self.media_type)
            && usize::try_from(self.size_bytes).is_ok_and(|n| n <= MAX_INLINE_FILE_BYTES)
    }
}

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
                sha256: crate::canonical::hex::sha256_hex(bytes),
            },
            data: STANDARD.encode(bytes),
        };
        file.is_well_formed().then_some(file).ok_or_else(|| {
            format!("an inline file name or media type exceeds {MAX_INLINE_LABEL_BYTES} bytes")
        })
    }

    pub fn bytes(&self) -> Result<Vec<u8>, String> {
        let bytes = STANDARD
            .decode(self.data.as_bytes())
            .map_err(|e| format!("inline file data is invalid base64: {e}"))?;
        (bytes.len() as u64 == self.marker.size_bytes
            && crate::canonical::hex::sha256_hex(&bytes) == self.marker.sha256)
            .then_some(bytes)
            .ok_or_else(|| "inline file bytes differ from their marker".into())
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
        serde_json::to_vec(self).map_err(|e| format!("encode inline file: {e}"))
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() > MAX_INLINE_STORED_BYTES {
            return Err("an inline file row exceeds its bound".into());
        }
        let file: Self =
            serde_json::from_slice(bytes).map_err(|e| format!("inline file row format: {e}"))?;
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
            Self::TooLarge { bytes } => {
                write!(
                    f,
                    "record value is {bytes} bytes; maximum is {MAX_RECORD_BYTES}"
                )
            }
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
                .filter(|r| r.file.as_ref() == Some(marker))
                .and_then(|r| r.file_version.clone())
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
        self.is_live()
            .then(|| self.stamp.authored_at_ms.saturating_mul(1_000_000))
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
        let len = bytes.len();
        (len <= MAX_RECORD_BYTES)
            .then_some(bytes)
            .ok_or(RecordValueError::TooLarge { bytes: len })
    }

    pub fn encode_uncapped(&self) -> Result<Vec<u8>, RecordValueError> {
        self.validate_structure()?;
        serde_json::to_vec(self).map_err(|e| RecordValueError::Format(e.to_string()))
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, RecordValueError> {
        let record: Self =
            serde_json::from_slice(bytes).map_err(|e| RecordValueError::Format(e.to_string()))?;
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
            || self.file.as_ref().is_some_and(|f| !f.has_valid_hash())
            || self.file_version.as_ref().is_some_and(|ver| {
                ver.validate().is_err()
                    || (ver.source_stream == self.stamp.position.source_stream
                        && ver.sequence > self.stamp.position.sequence)
            })
        {
            return Err(RecordValueError::Format(
                "the file marker has an invalid digest or version".into(),
            ));
        }
        if self.inline_file.as_ref().is_some_and(|f| !f.fits_inline()) {
            return Err(RecordValueError::Format(
                "the inline file marker has an invalid digest, label or size".into(),
            ));
        }
        Ok(())
    }
}

pub mod author_clock {
    use crate::schema::types::Mutation;
    use sha2::{Digest, Sha256};

    pub const MUTATION_AUTHOR_SIGNATURE_VERSION: u8 = 3;

    #[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
    pub struct MutationAuthorClockState {
        pub physical_nanos: u64,
        pub logical_counter: u64,
    }

    impl MutationAuthorClockState {
        pub fn advance_local_batch(
            &mut self,
            now_nanos: u64,
            count: usize,
        ) -> Option<(std::ops::RangeInclusive<u64>, u64)> {
            let add = u64::try_from(count.checked_sub(1)?).ok()?;
            let counter = self.logical_counter.checked_add(1)?;
            let first = self.physical_nanos.max(now_nanos);
            let last = first.checked_add(add)?;
            self.physical_nanos = last;
            self.logical_counter = counter;
            Some((first..=last, counter))
        }

        pub fn observe_remote(&mut self, physical_nanos: u64, logical_counter: u64) {
            self.physical_nanos = self.physical_nanos.max(physical_nanos);
            self.logical_counter = self.logical_counter.max(logical_counter);
        }
    }

    #[must_use]
    pub fn mutation_author_clock_key(device_id: &str) -> String {
        format!(
            "mutation_author_clock:{:x}",
            Sha256::digest(device_id.as_bytes())
        )
    }

    #[must_use]
    pub fn mutation_author_canonical_bytes(m: &Mutation) -> Vec<u8> {
        let h = m.content_hash();
        crate::canonical::CanonicalWriter::with_capacity(
            h.len() + m.uuid.len() + m.author_clock_writer_id.len() + 32,
        )
        .field(b"lastdb-mutation-author-v3")
        .field(h.as_bytes())
        .field(m.uuid.as_bytes())
        .field(m.author_clock_writer_id.as_bytes())
        .u64(m.imported_written_at.unwrap_or(0))
        .u64(m.logical_counter)
        .finish()
    }
}
