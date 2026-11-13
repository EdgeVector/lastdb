//! Native field key codec, key-space identities, size limits, and record replacement intents.

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use hmac::{Hmac, Mac};
use schema_types::field_range_profile::FieldRangeProfile;
use sha2::Sha256;

pub const KEY_FORMAT: &[u8] = b"typed-ranges";
const SEP: char = '\u{0}';
const ESC: char = '\u{1}';

pub fn current_key_format(marker: &[u8]) -> bool {
    marker == KEY_FORMAT
}

#[derive(Clone)]
pub struct FieldKeyCodec {
    blind_key: [u8; 32],
    range_profile: FieldRangeProfile,
}

impl std::fmt::Debug for FieldKeyCodec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FieldKeyCodec")
            .field("range_profile", &self.range_profile)
            .finish_non_exhaustive()
    }
}

impl FieldKeyCodec {
    #[must_use]
    pub fn from_node_key(
        node_key: &[u8; 32],
        field_key_id: &str,
        range_profile: FieldRangeProfile,
    ) -> Self {
        let mut extract = Hmac::<Sha256>::new_from_slice(b"lastdb-field-blind-key-v1")
            .expect("HMAC accepts any key length");
        extract.update(node_key);
        let prk = extract.finalize().into_bytes();
        let mut expand = Hmac::<Sha256>::new_from_slice(&prk).expect("HMAC accepts any key length");
        expand.update(field_key_id.as_bytes());
        expand.update(&[1u8]);
        Self {
            blind_key: expand.finalize().into_bytes().into(),
            range_profile,
        }
    }

    pub fn require_text_range_prefix(&self) -> Result<(), FieldKeyCodecError> {
        self.range_profile
            .require_text_prefix()
            .map_err(FieldKeyCodecError::InvalidRange)
    }

    pub fn storage_hash(
        &self,
        field_key_id: &str,
        api_hash: &str,
    ) -> Result<String, FieldKeyCodecError> {
        if api_hash.is_empty() {
            return Ok(String::new());
        }
        let mut mac =
            Hmac::<Sha256>::new_from_slice(&self.blind_key).expect("HMAC accepts any key length");
        mac.update(b"hk|v1\0");
        mac.update(field_key_id.as_bytes());
        mac.update(b"\0");
        mac.update(api_hash.as_bytes());
        Ok(URL_SAFE_NO_PAD.encode(&mac.finalize().into_bytes()[..16]))
    }

    pub fn storage_range(
        &self,
        _field_key_id: &str,
        api_range: &str,
    ) -> Result<String, FieldKeyCodecError> {
        self.range_profile
            .encode_value(api_range)
            .map_err(FieldKeyCodecError::InvalidRange)
    }

    pub fn api_range(&self, stored: &str) -> Result<String, FieldKeyCodecError> {
        self.range_profile
            .decode_value(stored)
            .map_err(FieldKeyCodecError::InvalidRange)
    }

    pub fn api_hash_range_member_prefix(
        &self,
        field_key_id: &str,
        api_hash: &str,
        api_range: &str,
    ) -> Result<String, FieldKeyCodecError> {
        let hash = self.storage_hash(field_key_id, api_hash)?;
        let range = self.storage_range(field_key_id, api_range)?;
        Ok(format!(
            "{field_key_id}:{}{SEP}{}",
            escape_segment(&hash),
            escape_segment(&range)
        ))
    }

    pub fn api_hash_range_scan_prefix_for_hash(
        &self,
        field_key_id: &str,
        api_hash: &str,
    ) -> Result<String, FieldKeyCodecError> {
        let h = self.storage_hash(field_key_id, api_hash)?;
        Ok(format!("{field_key_id}:{}{SEP}", escape_segment(&h)))
    }

    pub fn api_record_key(
        &self,
        space: &str,
        api_hash: &str,
        api_range: &str,
    ) -> Result<String, FieldKeyCodecError> {
        let hash = self.storage_hash(space, api_hash)?;
        let range = self.storage_range(space, api_range)?;
        Ok(record_space::record_key(space, &hash, &range))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FieldKeyCodecError {
    InvalidRange(String),
}

impl std::fmt::Display for FieldKeyCodecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidRange(reason) => write!(f, "invalid range key: {reason}"),
        }
    }
}

impl std::error::Error for FieldKeyCodecError {}

pub(crate) fn escape_segment(segment: &str) -> String {
    if !segment.contains([SEP, ESC]) {
        return segment.to_owned();
    }
    let mut out = String::with_capacity(segment.len() + 2);
    for c in segment.chars() {
        match c {
            SEP => out.push_str("\u{1}\u{1}"),
            ESC => out.push_str("\u{1}\u{2}"),
            other => out.push(other),
        }
    }
    out
}

#[must_use]
pub fn decode_hash_range_suffix(suffix: &str) -> Option<(String, String)> {
    let (esc, range) = suffix.split_once(SEP)?;
    if !esc.contains(ESC) {
        return Some((esc.to_owned(), range.to_owned()));
    }
    let mut hash = String::with_capacity(esc.len());
    let mut chars = esc.chars();
    while let Some(c) = chars.next() {
        if c == ESC {
            match chars.next()? {
                '\u{1}' => hash.push(SEP),
                '\u{2}' => hash.push(ESC),
                _ => return None,
            }
        } else {
            hash.push(c);
        }
    }
    Some((hash, range.to_owned()))
}

pub mod field_key_id {
    use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
    use sha2::{Digest, Sha256};

    pub const ALTERNATE_ENTRY_PREFIX: &str = "ak:";

    #[must_use]
    pub fn is_canonical_field_key_id(field_key_id: &str) -> bool {
        field_key_id.len() == 43
            && URL_SAFE_NO_PAD
                .decode(field_key_id)
                .is_ok_and(|d| d.len() == 32 && URL_SAFE_NO_PAD.encode(d) == field_key_id)
    }

    #[must_use]
    pub fn alternate_field_key_id(schema: &crate::schema::types::Schema, name: &str) -> String {
        let revision = schema.alternate_key_revision();
        let mut hash = Sha256::new();
        hash.update(b"lastdb:alternate-key:v1");
        for part in [schema.name.as_str(), revision.as_str(), name] {
            hash.update((part.len() as u64).to_be_bytes());
            hash.update(part.as_bytes());
        }
        URL_SAFE_NO_PAD.encode(hash.finalize())
    }
}

pub mod record_space {
    use crate::field_key_codec::escape_segment;
    use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
    use sha2::{Digest, Sha256};

    pub const RECORD_PREFIX: &str = "rk:";

    #[must_use]
    pub fn record_space_uuid(schema_name: &str) -> String {
        let mut hasher = Sha256::new();
        for part in [b"lastdb:record-space:v1".as_slice(), schema_name.as_bytes()] {
            crate::canonical::push_field(&mut hasher, part);
        }
        URL_SAFE_NO_PAD.encode(hasher.finalize())
    }

    #[must_use]
    pub fn record_key(space: &str, storage_hash: &str, storage_range: &str) -> String {
        format!(
            "{RECORD_PREFIX}{space}:{}\0{storage_range}",
            escape_segment(storage_hash)
        )
    }

    #[must_use]
    pub fn record_hash_prefix(space: &str, storage_hash: &str) -> String {
        format!("{RECORD_PREFIX}{space}:{}\0", escape_segment(storage_hash))
    }
}

pub mod record_replacement {
    use crate::record_value::FileMarker;
    use crate::schema::types::KeyValue;
    use schema_types::record_keys::RecordKeys;
    use serde::{Deserialize, Serialize};
    use serde_json::Value;
    use std::collections::BTreeMap;

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub struct RecordTarget {
        pub schema: String,
        pub key: KeyValue,
        pub alternates: BTreeMap<String, Option<KeyValue>>,
    }

    impl RecordTarget {
        pub fn validate(&self, def: &RecordKeys) -> Result<(), String> {
            def.validate()?;
            if self.schema.is_empty() {
                return Err("a record target requires a schema".into());
            }
            def.primary
                .validate_coordinates(self.key.hash.as_deref(), self.key.range.as_deref())?;
            if def.alternates.keys().ne(self.alternates.keys()) {
                return Err("supply exactly every declared alternate key".into());
            }
            for (name, key) in &self.alternates {
                let declared = &def.alternates[name];
                match key {
                    Some(k) => {
                        declared.validate_coordinates(k.hash.as_deref(), k.range.as_deref())?;
                    }
                    None if declared.allow_absent => {}
                    None => return Err(format!("alternate {name} requires an address")),
                }
            }
            Ok(())
        }
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    #[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
    pub enum RecordExpectation {
        Absent,
        Revision { revision: String },
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub struct RecordMetadata {
        pub source_file_name: Option<String>,
        pub metadata: BTreeMap<String, String>,
    }

    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub struct RecordReplacement {
        pub target: RecordTarget,
        pub expected: RecordExpectation,
        pub fields: BTreeMap<String, Value>,
        pub metadata: Option<RecordMetadata>,
        pub file: Option<FileMarker>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub inline_file: Option<FileMarker>,
    }

    impl RecordReplacement {
        pub fn validate(&self, def: &RecordKeys) -> Result<(), String> {
            self.target.validate(def)?;
            if matches!(&self.expected, RecordExpectation::Revision { revision } if revision.is_empty())
            {
                return Err("a replacement revision must be nonempty".into());
            }
            if self.file.as_ref().is_some_and(|f| !f.has_valid_hash()) {
                return Err("a record file marker requires a lowercase SHA-256".into());
            }
            if self.inline_file.as_ref().is_some_and(|f| !f.fits_inline()) {
                return Err("an inline file marker has an invalid digest, label or size".into());
            }
            Ok(())
        }
    }
}
