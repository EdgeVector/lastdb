use super::{FileMarker, MutationEntry, MutationOperation, PortableMutationError, Position, Stamp};
use crate::canonical::CanonicalValue;
use crate::record_value::{InlineFile, MAX_INLINE_STORED_BYTES, MAX_RECORD_BYTES};
use crate::schema::types::KeyValue;
use schema_types::record_keys::{RecordKeyDefinition, RecordKeys};
use serde::ser::SerializeMap;
use serde::{de::DeserializeOwned, Deserialize, Serialize, Serializer};
use serde_json::Value;
use std::collections::BTreeMap;
use std::io::{self, Write};

const ENTRY_FRAMING_BYTES: usize = 4096;
const MAX_JSON_DEPTH: usize = 132;

pub const MAX_PORTABLE_RECORD_BYTES: usize = MAX_RECORD_BYTES + MAX_INLINE_STORED_BYTES;
pub const MAX_MUTATION_ENTRY_BYTES: usize = MAX_PORTABLE_RECORD_BYTES + ENTRY_FRAMING_BYTES;

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

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecordData {
    pub alternates: BTreeMap<String, Option<RecordKey>>,
    #[serde(serialize_with = "serialize_fields")]
    pub fields: BTreeMap<String, Value>,
    #[serde(serialize_with = "serialize_metadata")]
    pub metadata: Option<BTreeMap<String, Value>>,
    pub file: Option<FileMarker>,
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
        encode(self, self.data.max_record_bytes())
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, PortableMutationError> {
        let record: Self = parse(bytes, MAX_PORTABLE_RECORD_BYTES)?;
        require_canonical(bytes, &record.encode()?)?;
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

#[derive(Serialize)]
struct RecordValue<'a> {
    primary_key: &'a RecordKey,
    data: &'a RecordData,
    written_at_ms: u64,
    last_mutation: &'a Position,
}

pub(super) fn entry_max_encoded_bytes(entry: &MutationEntry) -> usize {
    let record = match &entry.operation {
        MutationOperation::ReplaceRecord { data, .. } => data.max_record_bytes(),
        MutationOperation::DeleteRecord { .. } | MutationOperation::DefineSchema { .. } => {
            MAX_RECORD_BYTES
        }
    };
    record + ENTRY_FRAMING_BYTES
}

pub(super) fn encode_entry(entry: &MutationEntry) -> Result<Vec<u8>, PortableMutationError> {
    entry.validate()?;
    if let MutationOperation::ReplaceRecord { primary_key, data } = &entry.operation {
        let record = RecordValue {
            primary_key,
            data,
            written_at_ms: entry.stamp.authored_at_ms,
            last_mutation: &entry.stamp.position,
        };
        BoundedJson::new(data.max_record_bytes(), None).serialize(&record)?;
    }
    encode(entry, entry.max_encoded_bytes())
}

pub(super) fn decode_entry(bytes: &[u8]) -> Result<MutationEntry, PortableMutationError> {
    let entry: MutationEntry = parse(bytes, MAX_MUTATION_ENTRY_BYTES)?;
    require_canonical(bytes, &entry.encode()?)?;
    Ok(entry)
}

fn encode(value: &impl Serialize, max_bytes: usize) -> Result<Vec<u8>, PortableMutationError> {
    let mut bytes = Vec::new();
    BoundedJson::new(max_bytes, Some(&mut bytes)).serialize(value)?;
    Ok(bytes)
}

fn parse<T: DeserializeOwned>(bytes: &[u8], max_bytes: usize) -> Result<T, PortableMutationError> {
    if bytes.len() > max_bytes {
        return Err(PortableMutationError::TooLarge { max_bytes });
    }
    JsonDepth::default().admit(bytes)?;
    let mut decoder = serde_json::Deserializer::from_slice(bytes);
    decoder.disable_recursion_limit();
    let invalid = |_| PortableMutationError::Invalid("invalid portable JSON");
    let value = T::deserialize(&mut decoder).map_err(invalid)?;
    decoder.end().map_err(invalid)?;
    Ok(value)
}

fn require_canonical(bytes: &[u8], canonical: &[u8]) -> Result<(), PortableMutationError> {
    if bytes != canonical {
        return Err(PortableMutationError::Invalid(
            "portable JSON must be in canonical form",
        ));
    }
    Ok(())
}

struct BoundedJson<'a> {
    max_bytes: usize,
    remaining: usize,
    depth: JsonDepth,
    failure: Option<PortableMutationError>,
    output: Option<&'a mut Vec<u8>>,
}

impl<'a> BoundedJson<'a> {
    fn new(max_bytes: usize, output: Option<&'a mut Vec<u8>>) -> Self {
        Self {
            max_bytes,
            remaining: max_bytes,
            depth: JsonDepth::default(),
            failure: None,
            output,
        }
    }

    fn serialize(mut self, value: &impl Serialize) -> Result<(), PortableMutationError> {
        serde_json::to_writer(&mut self, value).map_err(|_| {
            self.failure.unwrap_or(PortableMutationError::Invalid(
                "cannot encode portable JSON",
            ))
        })
    }
}

impl Write for BoundedJson<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let admission = if bytes.len() <= self.remaining {
            self.depth.admit(bytes)
        } else {
            Err(PortableMutationError::TooLarge {
                max_bytes: self.max_bytes,
            })
        };
        if let Err(error) = admission {
            self.failure = Some(error);
            return Err(io::ErrorKind::InvalidData.into());
        }
        self.remaining -= bytes.len();
        if let Some(output) = self.output.as_mut() {
            output.extend_from_slice(bytes);
        }
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[derive(Default)]
struct JsonDepth {
    containers: usize,
    quoted: bool,
    escaped: bool,
}

impl JsonDepth {
    fn admit(&mut self, bytes: &[u8]) -> Result<(), PortableMutationError> {
        for &byte in bytes {
            if self.quoted {
                self.read_string_byte(byte);
                continue;
            }
            match byte {
                b'"' => self.quoted = true,
                b'[' | b'{' => {
                    self.containers += 1;
                    if self.containers > MAX_JSON_DEPTH {
                        return Err(PortableMutationError::Invalid(
                            "portable JSON exceeds its nesting limit",
                        ));
                    }
                }
                b']' | b'}' => self.containers = self.containers.saturating_sub(1),
                _ => (),
            }
        }
        Ok(())
    }

    fn read_string_byte(&mut self, byte: u8) {
        if self.escaped {
            self.escaped = false;
        } else if byte == b'\\' {
            self.escaped = true;
        } else if byte == b'"' {
            self.quoted = false;
        }
    }
}
