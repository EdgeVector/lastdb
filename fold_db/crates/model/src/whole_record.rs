//! Data-only contract for the isolated whole-record mutation spike.
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::BTreeMap;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Key {
    pub hash: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub range: Option<String>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KeyShape {
    Hash,
    HashRange,
}

impl KeyShape {
    pub fn validate(self, key: &Key) -> Result<(), String> {
        if key.hash.is_empty() {
            return Err("key hash must be nonempty".into());
        }
        match (self, &key.range) {
            (Self::Hash, None) => Ok(()),
            (Self::HashRange, Some(range)) if !range.is_empty() => Ok(()),
            _ => Err("key does not match its declared hash/hash_range shape".into()),
        }
    }
}

/// Keys name addresses, never payload fields. Definitions are immutable.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Schema {
    pub primary: KeyShape,
    pub alternates: BTreeMap<String, KeyShape>,
}

impl Schema {
    pub fn validate(&self) -> Result<(), String> {
        if self.alternates.len() > 8 || self.alternates.keys().any(String::is_empty) {
            return Err("schema permits at most eight nonempty alternate names".into());
        }
        Ok(())
    }

    pub fn validate_keys(
        &self,
        key: &Key,
        alternates: &BTreeMap<String, Key>,
    ) -> Result<(), String> {
        self.validate()?;
        self.primary.validate(key)?;
        if self.alternates.keys().ne(alternates.keys()) {
            return Err("supply exactly every declared alternate key".into());
        }
        for (name, shape) in &self.alternates {
            shape.validate(&alternates[name])?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum Expected {
    Absent,
    Revision { revision: String },
}

/// A complete replacement, including every named alternate address.
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PutRecord {
    pub schema: String,
    pub key: Key,
    pub expected: Expected,
    pub alternates: BTreeMap<String, Key>,
    pub record: Map<String, Value>,
}

/// Deletion supplies every alternate and must match the complete current record.
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeleteRecord {
    pub schema: String,
    pub key: Key,
    pub alternates: BTreeMap<String, Key>,
    pub revision: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Record {
    pub revision: String,
    pub alternates: BTreeMap<String, Key>,
    pub record: Map<String, Value>,
}
