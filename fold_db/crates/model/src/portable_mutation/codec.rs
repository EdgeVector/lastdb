use super::{record::RecordValue, MutationEntry, MutationOperation, PortableMutationError};
use crate::record_value::MAX_RECORD_BYTES;
use serde::{de::DeserializeOwned, Serialize};
use std::io::{self, Write};

/// Room for the exact record value plus its schema and operation framing.
pub const MAX_MUTATION_ENTRY_BYTES: usize = MAX_RECORD_BYTES + 4096;

// The existing JSON decoder allows fewer than 128 containers. Leave space
// for the extra portable envelope objects while bounding both directions.
const MAX_JSON_DEPTH: usize = 132;

impl MutationEntry {
    pub fn encode(&self) -> Result<Vec<u8>, PortableMutationError> {
        self.validate()?;
        if let MutationOperation::ReplaceRecord { primary_key, data } = &self.operation {
            let record = RecordValue {
                primary_key,
                data,
                written_at_ms: self.stamp.authored_at_ms,
                last_mutation: &self.stamp.position,
            };
            BoundedJson::new(MAX_RECORD_BYTES, None).serialize(&record)?;
        }
        encode(self, MAX_MUTATION_ENTRY_BYTES)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, PortableMutationError> {
        let entry: Self = parse(bytes, MAX_MUTATION_ENTRY_BYTES)?;
        require_canonical(bytes, &entry.encode()?)?;
        Ok(entry)
    }
}

pub(super) fn encode(
    value: &impl Serialize,
    max_bytes: usize,
) -> Result<Vec<u8>, PortableMutationError> {
    let mut bytes = Vec::new();
    BoundedJson::new(max_bytes, Some(&mut bytes)).serialize(value)?;
    Ok(bytes)
}

pub(super) fn parse<T: DeserializeOwned>(
    bytes: &[u8],
    max_bytes: usize,
) -> Result<T, PortableMutationError> {
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

pub(super) fn require_canonical(
    bytes: &[u8],
    canonical: &[u8],
) -> Result<(), PortableMutationError> {
    if bytes != canonical {
        return Err(PortableMutationError::Invalid(
            "portable JSON must be in canonical form",
        ));
    }
    Ok(())
}

/// A pure, in-memory serializer sink. Count bytes in place when the caller
/// only needs to admit a prospective record.
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

/// This counts JSON containers, ignoring escaped punctuation in strings.
/// Serde still owns syntax checks. Retain state across serializer writes
/// since quoted strings and their escapes may span several chunks.
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
