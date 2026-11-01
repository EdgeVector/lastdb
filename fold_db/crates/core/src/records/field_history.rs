//! Field history entries of format 7 records and their policy shapes.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum FieldHistoryPolicy {
    #[default]
    Off,
    Keep,
    Ttl {
        seconds: u64,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistoryLocation {
    pub schema: String,
    pub field: String,
    #[serde(default = "empty_key")]
    pub key: crate::schema::types::key_value::KeyValue,
}

fn empty_key() -> crate::schema::types::key_value::KeyValue {
    crate::schema::types::key_value::KeyValue::new(None, None)
}

#[derive(Debug, Clone, Serialize)]
pub struct FieldHistoryEntry {
    pub id: String,
    pub written_at: u64,
    pub mutation: String,
    pub archived_at: u64,
    pub expires_at: Option<u64>,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct FieldHistoryPage {
    pub current: Option<serde_json::Value>,
    pub entries: Vec<FieldHistoryEntry>,
    pub cursor: Option<String>,
}

/// The family of a field history entry of a format 7 home.
pub(crate) const RECORD_HISTORY_PREFIX: &str = "rh:";

/// One field history entry of a format 7 home (decision D5): its own copy of
/// one old field value and the stamp of that value. The key is
/// `{scope:}rh:{S}:{sha(record key)}:{field identity}:{id}`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RecordHistoryEntry {
    pub v: u8,
    pub schema: String,
    pub field: String,
    pub field_identity: String,
    /// The full storage key of the record.
    pub record: String,
    /// The copy of the old value. A file blob pointer in it holds one blob
    /// reference while the entry lives.
    pub value: serde_json::Value,
    /// The stamp of the old value.
    pub stamp: lastdb_model::record_value::FieldStamp,
    pub archived_at: u64,
    pub expires_at: Option<u64>,
    /// `replace` or `delete`.
    pub reason: String,
    pub queue_key: Option<String>,
}

impl RecordHistoryEntry {
    pub(crate) const VERSION: u8 = 1;

    pub(crate) fn entry(&self, key: &str) -> FieldHistoryEntry {
        FieldHistoryEntry {
            id: key.rsplit(':').next().unwrap_or_default().to_string(),
            written_at: self.stamp.written_at,
            mutation: self.stamp.mutation_id.clone(),
            archived_at: self.archived_at,
            expires_at: self.expires_at,
            reason: self.reason.clone(),
        }
    }

    pub(crate) fn expired(&self, now: u64) -> bool {
        self.expires_at.is_some_and(|at| at <= now)
    }

    /// The blobs that the copied value names. Each holds one reference.
    pub(crate) fn blobs(&self) -> Vec<String> {
        lastdb_model::file_pointer::blob_refs_of_value(&self.value, None)
    }
}
