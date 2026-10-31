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
    pub atom: String,
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
            atom: self.event.new_atom_uuid.clone(),
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
