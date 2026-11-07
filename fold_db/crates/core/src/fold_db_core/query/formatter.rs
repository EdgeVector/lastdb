use crate::schema::types::key_value::KeyValue;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};

/// The metadata of one record (decision D7): the source file name and the
/// metadata map of the last write that carried them, and the marker of the
/// record's file. A field has no metadata of its own.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct RecordMetadata {
    pub source_file_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<HashMap<String, String>>,
    /// The record's file, read with `fetch-blob` by schema and key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file: Option<lastdb_model::record_value::FileMarker>,
    /// The record's inline file. The host keeps `bytes_b64` only for a
    /// query with `hydrate: inline`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inline_file: Option<InlineFileView>,
}

/// The marker of an inline file and, when hydrated, its bytes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InlineFileView {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub media_type: Option<String>,
    pub size_bytes: u64,
    pub sha256: String,
    /// Standard base64 of the plaintext.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bytes_b64: Option<String>,
}

impl From<&lastdb_model::record_value::InlineFile> for InlineFileView {
    fn from(file: &lastdb_model::record_value::InlineFile) -> Self {
        Self {
            name: file.marker.name.clone(),
            media_type: file.marker.media_type.clone(),
            size_bytes: file.marker.size_bytes,
            sha256: file.marker.sha256.clone(),
            bytes_b64: Some(file.data.clone()),
        }
    }
}

/// One stored record, keyed by `KeyValue` in [`QueryRecords`].
/// The `fields` map is every field the record stores. `metadata` is the one
/// metadata block of the record, or `None` when the record has none.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Record {
    /// Token for a conditional replacement of this complete record.
    pub revision: String,
    pub alternates: BTreeMap<String, Option<KeyValue>>,
    pub fields: HashMap<String, Value>,
    pub metadata: Option<RecordMetadata>,
}

/// The reply of one record query: each stored record, by its API key.
pub type QueryRecords = HashMap<KeyValue, Record>;
