use super::{record::RecordValue, MutationEntry, MutationOperation, PortableMutationError};
use crate::record_value::MAX_RECORD_BYTES;
use serde::{de::DeserializeOwned, Serialize};
use std::io::{self, Write};

/// Room for the exact record value plus its schema and operation framing.
pub const MAX_MUTATION_ENTRY_BYTES: usize = MAX_RECORD_BYTES + 4096;

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
    serde_json::from_slice(bytes)
        .map_err(|_| PortableMutationError::Invalid("invalid portable JSON"))
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
    overflowed: bool,
    output: Option<&'a mut Vec<u8>>,
}

impl<'a> BoundedJson<'a> {
    fn new(max_bytes: usize, output: Option<&'a mut Vec<u8>>) -> Self {
        Self {
            max_bytes,
            remaining: max_bytes,
            overflowed: false,
            output,
        }
    }

    fn serialize(mut self, value: &impl Serialize) -> Result<(), PortableMutationError> {
        serde_json::to_writer(&mut self, value).map_err(|_| {
            if self.overflowed {
                PortableMutationError::TooLarge {
                    max_bytes: self.max_bytes,
                }
            } else {
                PortableMutationError::Invalid("cannot encode portable JSON")
            }
        })
    }
}

impl Write for BoundedJson<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let Some(remaining) = self.remaining.checked_sub(bytes.len()) else {
            self.overflowed = true;
            return Err(io::ErrorKind::InvalidData.into());
        };
        self.remaining = remaining;
        if let Some(output) = self.output.as_mut() {
            output.extend_from_slice(bytes);
        }
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
