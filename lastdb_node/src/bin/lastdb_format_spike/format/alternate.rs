//! A named alternate key stores only a pointer to the existing primary key.
use super::{EncodedAddress, EncodedMutation, EncodedRange, Result};
use lastdb_model::record_replacement::RecordValues;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

#[derive(Clone, Copy, Serialize)]
pub(super) struct AlternateFormat {
    pub field: &'static str,
    pub collection: &'static str,
    pub prefix: &'static str,
    pub version: u32,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Pointer {
    version: u32,
    key: String,
}

impl AlternateFormat {
    pub(super) fn value<'a>(&self, fields: &'a Map<String, Value>) -> Result<Option<&'a str>> {
        fields
            .get(self.field)
            .map(|value| {
                value
                    .as_str()
                    .filter(|text| !text.is_empty())
                    .ok_or_else(|| "alternate field must be a nonempty string when present".into())
            })
            .transpose()
    }

    pub(super) fn range(&self, kind: &str, value: &str) -> Result<EncodedRange> {
        if kind.is_empty() || value.is_empty() {
            return Err("alternate kind and value must be nonempty".into());
        }
        // JSON escapes embedded NULs; the single literal NUL closes this exact hash.
        let hash = serde_json::to_string(&(kind, value))?;
        Ok(EncodedRange {
            collection: self.collection.to_owned(),
            prefix: format!("{}{hash}\0", self.prefix),
        })
    }

    fn address(&self, kind: &str, value: &str, primary: &str) -> Result<EncodedAddress> {
        let range = self.range(kind, value)?;
        // Primary identity distinguishes records sharing the same alternate value.
        let suffix = serde_json::to_string(primary)?;
        Ok(EncodedAddress {
            collection: range.collection,
            key: format!("{}{suffix}", range.prefix),
        })
    }

    pub(super) fn replacement(
        &self,
        values: &impl RecordValues,
        previous: Option<&Map<String, Value>>,
    ) -> Result<Vec<EncodedMutation>> {
        let before = previous
            .map(|fields| self.value(fields))
            .transpose()?
            .flatten();
        let after = self.value(values.fields())?;
        let mut operations = Vec::new();
        if let Some(old) = before.filter(|old| Some(*old) != after) {
            operations.push(EncodedMutation {
                address: self.address(values.kind(), old, values.key())?,
                body: None,
            });
        }
        if let Some(new) = after {
            operations.push(EncodedMutation {
                address: self.address(values.kind(), new, values.key())?,
                body: Some(serde_json::to_vec(&Pointer {
                    version: self.version,
                    key: values.key().to_owned(),
                })?),
            });
        }
        Ok(operations)
    }

    pub(super) fn decode(&self, kind: &str, value: &str, key: &str, body: &[u8]) -> Result<String> {
        let pointer: Pointer = serde_json::from_slice(body)?;
        if pointer.version != self.version
            || pointer.key.is_empty()
            || self.address(kind, value, &pointer.key)?.key != key
        {
            return Err("alternate entry does not match its declared primary pointer".into());
        }
        Ok(pointer.key)
    }
}
