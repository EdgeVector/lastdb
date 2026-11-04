//! Complete local record intents. Data and validation only, no write authority.
//!
//! These preparatory types do not activate a new serving API or disk format.
//! A format writer must still validate payload fields, the stored revision,
//! complete key equality on delete and the issuing context for changed files.
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
    pub fn validate(&self, definition: &RecordKeys) -> Result<(), String> {
        definition.validate()?;
        if self.schema.is_empty() {
            return Err("a record target requires a schema".into());
        }
        definition
            .primary
            .validate_coordinates(self.key.hash.as_deref(), self.key.range.as_deref())?;
        if definition.alternates.keys().ne(self.alternates.keys()) {
            return Err("supply exactly every declared alternate key".into());
        }
        for (name, key) in &self.alternates {
            let declared = &definition.alternates[name];
            match key {
                Some(key) => {
                    declared.validate_coordinates(key.hash.as_deref(), key.range.as_deref())?;
                }
                None if declared.allow_absent => (),
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

/// All columns are replacements. Missing metadata/file means absent, never keep-old.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecordReplacement {
    pub target: RecordTarget,
    pub expected: RecordExpectation,
    pub fields: BTreeMap<String, Value>,
    pub metadata: Option<RecordMetadata>,
    pub file: Option<FileMarker>,
}

impl RecordReplacement {
    pub fn validate(&self, definition: &RecordKeys) -> Result<(), String> {
        self.target.validate(definition)?;
        if matches!(&self.expected, RecordExpectation::Revision { revision } if revision.is_empty())
        {
            return Err("a replacement revision must be nonempty".into());
        }
        if self
            .file
            .as_ref()
            .is_some_and(|file| !file.has_valid_hash())
        {
            return Err("a record file marker requires a lowercase SHA-256".into());
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecordDeletion {
    pub target: RecordTarget,
    pub revision: String,
}

impl RecordDeletion {
    pub fn validate(&self, definition: &RecordKeys) -> Result<(), String> {
        self.target.validate(definition)?;
        if self.revision.is_empty() {
            return Err("a deletion revision must be nonempty".into());
        }
        Ok(())
    }
}

/// A bounded retry receipt, committed atomically with its record.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetryReceipt {
    pub intent_hash: String,
    pub revision: String,
}
