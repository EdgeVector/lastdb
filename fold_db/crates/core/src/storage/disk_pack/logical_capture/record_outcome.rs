//! One certified final record effect of a format 7 home (event version 2).
//!
//! The outcome names the schema, the primary key and the changed parts of one
//! record: each field that the request wrote and that still wins, with its
//! value and stamp; the record metadata when the request set it; and the
//! delete floor when the request deleted the record. A receiver merges each
//! part by stamp, so two devices that change two fields of one record keep
//! both changes.

use crate::schema::types::KeyValue;
use lastdb_model::record_value::{FieldStamp, RecordMeta};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AcceptedRecordOutcome {
    pub schema_name: String,
    pub schema_identity: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub alternate_key_revision: Option<String>,
    /// The canonical primary key.
    pub key: KeyValue,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub fields: BTreeMap<String, RecordFieldOutcome>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub meta: Option<RecordMeta>,
    /// The delete floor that the request set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deleted: Option<FieldStamp>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecordFieldOutcome {
    /// The canonical field identity.
    pub identity: String,
    /// The value. Absent for a removed field; a JSON null is a value.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "present"
    )]
    pub value: Option<Value>,
    pub stamp: FieldStamp,
}

impl AcceptedRecordOutcome {
    /// Every stamp of the outcome: the fields, the metadata and the delete.
    pub fn stamps(&self) -> impl Iterator<Item = &FieldStamp> {
        self.fields
            .values()
            .map(|field| &field.stamp)
            .chain(self.meta.as_ref().map(|meta| &meta.stamp))
            .chain(self.deleted.as_ref())
    }
}

/// A present `value` is `Some`, also when it is a JSON null.
fn present<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Option<Value>, D::Error> {
    Value::deserialize(deserializer).map(Some)
}
