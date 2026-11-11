//! Query types, executor, and keyed hash/range processor.

use crate::schema::types::key_value::KeyValue;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};

pub mod hash_range_query;
pub mod query_executor;

pub use hash_range_query::HashRangeQueryProcessor;
pub use query_executor::QueryExecutor;

/// Metadata of one stored record.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct RecordMetadata {
    pub source_file_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<HashMap<String, String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file: Option<lastdb_model::record_value::FileMarker>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inline_file: Option<lastdb_model::record_value::FileMarker>,
}

/// One stored record returned in [`QueryRecords`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Record {
    pub revision: String,
    pub alternates: BTreeMap<String, Option<KeyValue>>,
    pub fields: HashMap<String, Value>,
    pub metadata: Option<RecordMetadata>,
}

/// Map of stored records keyed by canonical API key.
pub type QueryRecords = HashMap<KeyValue, Record>;

impl Record {
    pub fn from_stored(record: &lastdb_model::record_value::StoredRecord) -> Self {
        let meta = record
            .meta
            .as_ref()
            .filter(|meta| meta.source_file_name.is_some() || !meta.metadata.is_empty());
        let metadata = (meta.is_some() || record.file.is_some() || record.inline_file.is_some())
            .then(|| RecordMetadata {
                source_file_name: meta.and_then(|meta| meta.source_file_name.clone()),
                metadata: meta
                    .filter(|meta| !meta.metadata.is_empty())
                    .map(|meta| meta.metadata.clone().into_iter().collect()),
                file: record.file.clone(),
                inline_file: record.inline_file.clone(),
            });
        Self {
            revision: record.stamp.revision(),
            alternates: record.alternates.clone(),
            fields: record.fields.clone().into_iter().collect(),
            metadata,
        }
    }
}
