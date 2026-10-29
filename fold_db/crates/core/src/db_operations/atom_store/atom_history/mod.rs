//! Optional field history. Sidecar rows own counted atom and file references.

mod capture;
mod policy;
mod purge;
mod read;
mod release;
mod types;

pub(crate) use types::{ArchiveRecord, HistoryBinding, HistoryMode};
pub use types::{AtomHistoryEntry, AtomHistoryPage, FieldHistoryPolicy, HistoryLocation};

use super::time_queue::TimeQueue;
use base64::Engine as _;
use sha2::{Digest, Sha256};

fn history_walk(
    slot: &str,
    cursor: Option<&str>,
    limit: usize,
) -> Result<super::RangeWalk, crate::schema::SchemaError> {
    let base = prefix(slot)?;
    let (start, end) = crate::kind_partition::exact_prefix_bounds(&base);
    let start = match cursor {
        None => start.into_bytes(),
        Some(cursor) => base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(cursor)
            .map_err(|_| {
                crate::schema::SchemaError::InvalidData("invalid history cursor".into())
            })?,
    };
    if !start.starts_with(base.as_bytes()) || start.as_slice() >= end.as_bytes() {
        return Err(crate::schema::SchemaError::InvalidData(
            "history cursor is outside this key".into(),
        ));
    }
    Ok(super::RangeWalk::new(
        &start,
        end.as_bytes(),
        limit.clamp(1, 100),
    ))
}

fn next_cursor(walk: &super::RangeWalk) -> Option<String> {
    walk.request()
        .map(|(start, _, _)| base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(start))
}

pub(crate) fn queue() -> TimeQueue {
    TimeQueue::with_slice("ahq:v1", "ahb:v1", 3_600_000_000_000)
}

pub(crate) fn digest(value: &[u8]) -> String {
    crate::hex::hex_lower(&Sha256::digest(value))
}

pub(crate) fn is_archive(key: &str) -> bool {
    crate::kind_partition::rest_of(key, "history")
        .is_some_and(|rest| rest.rsplitn(3, ':').nth(1) == Some("archive"))
}

pub(crate) fn prefix(slot: &str) -> Result<String, crate::schema::SchemaError> {
    let molecule = crate::atom::molecule_key_codec::molecule_uuid_from_storage_key(slot)
        .ok_or_else(|| crate::schema::SchemaError::InvalidData("invalid history slot".into()))?;
    let scope = crate::storage::current_keys::exact_scope(slot)?;
    Ok(crate::schema::types::field::build_storage_key(
        scope,
        &format!("history\0{molecule}:{}:archive:", digest(slot.as_bytes())),
    ))
}

pub(crate) fn files(bytes: &[u8]) -> Result<Vec<String>, serde_json::Error> {
    let record: ArchiveRecord = serde_json::from_slice(bytes)?;
    Ok(super::blob_counter::tip_blobs(&record.archive.source))
}
