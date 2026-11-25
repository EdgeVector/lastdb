//! Records of the spool in the store, and the note a captured write hands to it.

use super::super::body;
use super::upload::Checkpoint;
use serde::{Deserialize, Serialize};

/// One captured write, signed, as the commit hands it to the spool.
#[derive(Clone)]
pub(in crate::storage) struct Note {
    /// The sequence number the entry was signed with. It must be the next spool sequence.
    pub seq: u64,
    pub mutation_id: String,
    pub plaintext_sha256: String,
    pub envelope: Vec<u8>,
    pub file: Option<Vec<u8>>,
}

impl Note {
    pub(in crate::storage) fn byte_weight(&self) -> u64 {
        (self.envelope.len() as u64).saturating_add(self.file.as_ref().map_or(0, Vec::len) as u64)
    }
}

/// The spool's own row (`accepted_state/state`). The head is not in it: the head is the
/// last entry record, or `acked` when no entry is left.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::storage::disk_backup::accepted) struct State {
    pub(in crate::storage::disk_backup::accepted) version: u16,
    /// Native frontier when the stream started. A first snapshot must cover it.
    pub(in crate::storage::disk_backup::accepted) opened_frontier: u64,
    /// Highest sequence the relay acknowledged. Entries at or below it are gone.
    pub(in crate::storage::disk_backup::accepted) acked: u64,
    /// The writes of the home from before this stream began are in no entry. A first
    /// snapshot must cover them before any entry may be read for upload or acknowledged.
    pub(in crate::storage::disk_backup::accepted) snapshot_required: bool,
    /// Where the upload of this home stands, written with the acknowledgement it confirms.
    pub(in crate::storage::disk_backup::accepted) upload: Option<Checkpoint>,
}

pub(in crate::storage::disk_backup::accepted) const STATE_VERSION: u16 = 2;

/// What an entry record says about its write, before the signed bytes.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::storage::disk_backup::accepted) struct Meta {
    pub(in crate::storage::disk_backup::accepted) sequence: u64,
    pub(in crate::storage::disk_backup::accepted) mutation_id: String,
    pub(in crate::storage::disk_backup::accepted) plaintext_sha256: String,
    pub(in crate::storage::disk_backup::accepted) file_sha256: Option<String>,
    pub(in crate::storage::disk_backup::accepted) file_bytes: u64,
}

impl Meta {
    /// Bytes this entry holds in the spool: the signed entry and its sealed file.
    pub(in crate::storage::disk_backup::accepted) fn weight(&self, signed_bytes: usize) -> u64 {
        (signed_bytes as u64).saturating_add(self.file_bytes)
    }
}

/// An entry record's value: a big-endian `u32` length, the meta as JSON, the signed entry.
pub(in crate::storage::disk_backup::accepted) fn encode_entry(
    meta: &Meta,
    signed: &[u8],
) -> Option<Vec<u8>> {
    let head = serde_json::to_vec(meta).ok()?;
    let len = u32::try_from(head.len()).ok()?;
    let mut out = Vec::with_capacity(4 + head.len() + signed.len());
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(&head);
    out.extend_from_slice(signed);
    Some(out)
}

pub(in crate::storage::disk_backup::accepted) fn decode_entry(
    value: &[u8],
) -> Option<(Meta, &[u8])> {
    let len = u32::from_be_bytes(value.get(..4)?.try_into().ok()?) as usize;
    let end = 4usize.checked_add(len)?;
    let meta = serde_json::from_slice(value.get(4..end)?).ok()?;
    Some((meta, value.get(end..)?))
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
