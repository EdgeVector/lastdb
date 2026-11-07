use super::{codec, PortableMutationError, Position, Stamp};
use crate::canonical::CanonicalValue;
use crate::record_value::{InlineFile, MAX_RECORD_BYTES, MAX_STORED_RECORD_BYTES};
use crate::schema::types::KeyValue;
use schema_types::record_keys::{RecordKeyDefinition, RecordKeys};
use serde::ser::SerializeMap;
use serde::{Deserialize, Serialize, Serializer};
use serde_json::Value;
use std::collections::BTreeMap;

/// The range uses the canonical API text of its declared scalar type.
/// The writer checks it against the installed schema before applying it.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecordKey {
    pub hash: String,
    pub range: Option<String>,
}

impl RecordKey {
    pub fn validate(&self) -> Result<(), PortableMutationError> {
        if self.hash.is_empty() || self.range.as_ref().is_some_and(String::is_empty) {
            return Err(PortableMutationError::Invalid(
                "record key coordinates must be nonempty when present",
            ));
        }
        Ok(())
    }

    pub(super) fn validate_against(
        &self,
        definition: &RecordKeyDefinition,
    ) -> Result<(), PortableMutationError> {
        let invalid = |_| PortableMutationError::Invalid("invalid coordinates for declared key");
        definition
            .validate_coordinates(Some(&self.hash), self.range.as_deref())
            .map_err(invalid)?;
        if let Some(range) = &self.range {
            let profile = definition.range_profile().map_err(invalid)?;
            let encoded = profile.encode_value(range).map_err(invalid)?;
            if profile.decode_value(&encoded).map_err(invalid)? != *range {
                return Err(PortableMutationError::Invalid(
                    "record range must use canonical key coordinates",
                ));
            }
        }
        Ok(())
    }
}

impl TryFrom<KeyValue> for RecordKey {
    type Error = PortableMutationError;

    fn try_from(key: KeyValue) -> Result<Self, Self::Error> {
        let hash = key.hash.ok_or(PortableMutationError::Invalid(
            "record key requires a nonempty hash",
        ))?;
        let key = Self {
            hash,
            range: key.range,
        };
        key.validate()?;
        Ok(key)
    }
}

impl From<RecordKey> for KeyValue {
    fn from(key: RecordKey) -> Self {
        Self::new(Some(key.hash), key.range)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileMarker {
    pub version: Position,
    pub name: Option<String>,
    pub media_type: Option<String>,
    pub size_bytes: u64,
    pub sha256: String,
}

impl FileMarker {
    fn validate(&self, record_position: &Position) -> Result<(), PortableMutationError> {
        self.version.validate()?;
        if self.sha256.len() != 64
            || !self
                .sha256
                .bytes()
                .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
        {
            return Err(PortableMutationError::Invalid(
                "file marker requires a lowercase SHA-256",
            ));
        }
        if self.version.source_stream == record_position.source_stream
            && self.version.sequence > record_position.sequence
        {
            return Err(PortableMutationError::Invalid(
                "file version cannot follow its record in the same stream",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecordData {
    pub alternates: BTreeMap<String, Option<RecordKey>>,
    #[serde(serialize_with = "serialize_fields")]
    pub fields: BTreeMap<String, Value>,
    #[serde(serialize_with = "serialize_metadata")]
    pub metadata: Option<BTreeMap<String, Value>>,
    pub file: Option<FileMarker>,
    /// The record's inline file travels inside the entry. Omitted when
    /// absent, so an entry without one keeps its earlier canonical bytes.
    /// Boxed so a delete operation stays small.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inline_file: Option<Box<InlineFile>>,
}

impl RecordData {
    pub(super) fn validate(&self, record_position: &Position) -> Result<(), PortableMutationError> {
        for key in self.alternates.values().flatten() {
            key.validate()?;
        }
        if let Some(file) = &self.file {
            file.validate(record_position)?;
        }
        if self
            .inline_file
            .as_ref()
            .is_some_and(|file| !file.is_well_formed())
        {
            return Err(PortableMutationError::Invalid(
                "inline file requires a valid marker, size and encoding",
            ));
        }
        Ok(())
    }

    /// The size cap of the record value: the record cap, plus the room of its
    /// inline file when it has one.
    #[must_use]
    pub fn max_record_bytes(&self) -> usize {
        MAX_RECORD_BYTES
            + self
                .inline_file
                .as_deref()
                .map_or(0, InlineFile::stored_allowance)
    }

    pub(super) fn validate_alternates(
        &self,
        keys: &RecordKeys,
    ) -> Result<(), PortableMutationError> {
        if keys.alternates.keys().ne(self.alternates.keys()) {
            return Err(PortableMutationError::Invalid(
                "record must supply exactly every declared alternate",
            ));
        }
        for (name, key) in &self.alternates {
            let definition = &keys.alternates[name];
            match key {
                Some(key) => key.validate_against(definition)?,
                None if definition.allow_absent => (),
                None => {
                    return Err(PortableMutationError::Invalid(
                        "required alternate key is absent",
                    ));
                }
            }
        }
        Ok(())
    }
}

fn serialize_fields<S: Serializer>(
    fields: &BTreeMap<String, Value>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    let mut map = serializer.serialize_map(Some(fields.len()))?;
    for (name, value) in fields {
        map.serialize_entry(name, &CanonicalValue(value))?;
    }
    map.end()
}

fn serialize_metadata<S: Serializer>(
    metadata: &Option<BTreeMap<String, Value>>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    match metadata {
        Some(metadata) => serialize_fields(metadata, serializer),
        None => serializer.serialize_none(),
    }
}

#[derive(Clone, Debug, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoredRecord {
    pub primary_key: RecordKey,
    pub data: RecordData,
    pub written_at_ms: u64,
    pub last_mutation: Position,
}

impl StoredRecord {
    #[must_use]
    pub fn stamp(&self) -> Stamp {
        Stamp {
            authored_at_ms: self.written_at_ms,
            position: self.last_mutation.clone(),
        }
    }

    pub fn validate(&self) -> Result<(), PortableMutationError> {
        self.last_mutation.validate()?;
        self.primary_key.validate()?;
        self.data.validate(&self.last_mutation)
    }

    pub fn encode(&self) -> Result<Vec<u8>, PortableMutationError> {
        self.validate()?;
        codec::encode(self, self.data.max_record_bytes())
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, PortableMutationError> {
        let record: Self = codec::parse(bytes, MAX_STORED_RECORD_BYTES)?;
        codec::require_canonical(bytes, &record.encode()?)?;
        Ok(record)
    }
}

impl Serialize for StoredRecord {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        RecordValue {
            primary_key: &self.primary_key,
            data: &self.data,
            written_at_ms: self.written_at_ms,
            last_mutation: &self.last_mutation,
        }
        .serialize(serializer)
    }
}

/// The replacement codec checks the exact bytes of the prospective record
/// without cloning its fields or allocating a second record body.
#[derive(Serialize)]
pub(super) struct RecordValue<'a> {
    pub(super) primary_key: &'a RecordKey,
    pub(super) data: &'a RecordData,
    pub(super) written_at_ms: u64,
    pub(super) last_mutation: &'a Position,
}
