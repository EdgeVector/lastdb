//! Field history archive rows and their policy shapes.

use crate::atom::MutationEvent;
use crate::records::molecule_record::PerKeyRecord;
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

#[derive(Debug, Clone, Default, Copy)]
pub(crate) enum HistoryMode {
    #[default]
    Ignore,
    Capture,
    Erase,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct HistoryBinding {
    pub schema: String,
    pub field: String,
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ArchiveMetadata {
    pub version: u8,
    pub binding: HistoryBinding,
    pub source: PerKeyRecord,
    pub archived_at: u64,
    pub expires_at: Option<u64>,
    pub reason: String,
    pub queue_key: Option<String>,
}

/// The event shape lets the existing reference verifier count this row.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ArchiveRecord {
    #[serde(flatten)]
    pub event: MutationEvent,
    pub archive: ArchiveMetadata,
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
pub struct AtomHistoryEntry {
    pub id: String,
    /// The archived atom. A record history entry of a format 7 home holds a
    /// copy of the value and names no atom (D7).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub atom: Option<String>,
    pub written_at: u64,
    pub mutation: String,
    pub archived_at: u64,
    pub expires_at: Option<u64>,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct AtomHistoryPage {
    pub current: Option<serde_json::Value>,
    pub entries: Vec<AtomHistoryEntry>,
    pub cursor: Option<String>,
}

impl ArchiveRecord {
    pub(crate) fn entry(&self, key: &str) -> AtomHistoryEntry {
        AtomHistoryEntry {
            id: key.rsplit(':').next().unwrap_or_default().to_string(),
            atom: Some(self.event.new_atom_uuid.clone()),
            written_at: self.archive.source.entry.written_at,
            mutation: self.archive.source.entry.mutation_uuid.clone(),
            archived_at: self.archive.archived_at,
            expires_at: self.archive.expires_at,
            reason: self.archive.reason.clone(),
        }
    }

    pub(crate) fn expired(&self, now: u64) -> bool {
        self.archive.expires_at.is_some_and(|at| at <= now)
    }
}

/// True for the key of an archive row in the field history family.
pub(crate) fn is_archive(key: &str) -> bool {
    lastdb_model::kind_partition::rest_of(key, "history")
        .is_some_and(|rest| rest.rsplit(':').nth(1) == Some("archive"))
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

    pub(crate) fn entry(&self, key: &str) -> AtomHistoryEntry {
        AtomHistoryEntry {
            id: key.rsplit(':').next().unwrap_or_default().to_string(),
            atom: None,
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
        crate::atom::file_pointer::blob_refs_of_atom(&self.value, None)
    }
}
