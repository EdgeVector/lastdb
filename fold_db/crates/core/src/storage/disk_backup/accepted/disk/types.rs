//! On-disk control and staging types for the accepted write spool.

use super::super::body;
use serde::{Deserialize, Serialize};

#[derive(Clone)]
pub(in crate::storage) struct Note {
    pub mutation_id: String,
    pub plaintext_sha256: String,
    pub envelope: Vec<u8>,
    pub authored_at_ms: Option<u64>,
    pub file: Option<Vec<u8>>,
}

impl Note {
    pub(in crate::storage) fn byte_weight(&self) -> u64 {
        (self.envelope.len() as u64).saturating_add(self.file.as_ref().map_or(0, Vec::len) as u64)
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::storage::disk_backup::accepted) struct State {
    pub(in crate::storage::disk_backup::accepted) version: u16,
    pub(in crate::storage::disk_backup::accepted) opened_frontier: u64,
    pub(in crate::storage::disk_backup::accepted) head: u64,
    pub(in crate::storage::disk_backup::accepted) acked: u64,
    pub(in crate::storage::disk_backup::accepted) retained_bytes: u64,
    /// The writes of the home from before this stream began are in no entry. A first
    /// snapshot must cover them before any entry may be read for upload or acknowledged.
    /// False for a home that captured from its birth, and for a restore.
    #[serde(default)]
    pub(in crate::storage::disk_backup::accepted) snapshot_required: bool,
    pub(in crate::storage::disk_backup::accepted) gap: bool,
    #[serde(rename = "armed", default, skip_serializing)]
    pub(in crate::storage::disk_backup::accepted) _armed: bool,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::storage::disk_backup::accepted) struct Pending {
    pub(in crate::storage::disk_backup::accepted) version: u16,
    pub(in crate::storage::disk_backup::accepted) frontier_before: u64,
    #[serde(default)]
    pub(in crate::storage::disk_backup::accepted) main_frontier_before: Option<u64>,
    pub(in crate::storage::disk_backup::accepted) notes: Vec<PendingNote>,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::storage::disk_backup::accepted) struct PendingNote {
    pub(in crate::storage::disk_backup::accepted) sequence: u64,
    pub(in crate::storage::disk_backup::accepted) mutation_id: String,
    pub(in crate::storage::disk_backup::accepted) plaintext_sha256: String,
    pub(in crate::storage::disk_backup::accepted) envelope_sha256: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(in crate::storage::disk_backup::accepted) authored_at_ms: Option<u64>,
    pub(in crate::storage::disk_backup::accepted) file_sha256: Option<String>,
    pub(in crate::storage::disk_backup::accepted) envelope_bytes: u64,
    pub(in crate::storage::disk_backup::accepted) file_bytes: u64,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::storage::disk_backup::accepted) struct Meta {
    pub(in crate::storage::disk_backup::accepted) version: u16,
    #[serde(flatten)]
    pub(in crate::storage::disk_backup::accepted) note: PendingNote,
}

impl std::ops::Deref for Meta {
    type Target = PendingNote;
    fn deref(&self) -> &Self::Target {
        &self.note
    }
}

pub(in crate::storage::disk_backup::accepted) fn spool_cap() -> u64 {
    lastdb_model::storage_policy::BACKUP_RESOURCES.durable_spool_bytes
}

pub(in crate::storage::disk_backup::accepted) fn sha_ok(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

pub(in crate::storage::disk_backup::accepted) fn id_ok(id: &str) -> bool {
    body::mutation_id_ok(id)
}
