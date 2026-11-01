//! Optional field history of format 7 records. Each entry keeps its own copy of
//! one old field value, and each file blob that the copy names holds one
//! reference while the entry lives.

mod key_counts;
mod policy;
mod record_reads;
mod records;
mod release;
pub use crate::records::atom_history::{
    AtomHistoryEntry, AtomHistoryPage, FieldHistoryPolicy, HistoryLocation,
};
pub(crate) use records::RecordHistoryRows;

impl super::RecordStore {
    pub(crate) fn history_activation(&self) -> &tokio::sync::Notify {
        &self.history_activation
    }

    pub(crate) async fn atom_history_enabled(&self) -> Result<bool, crate::schema::SchemaError> {
        let marker = self
            .main_store
            .inner()
            .get(Self::CURRENT_KEY_FORMAT_KEY.as_bytes())
            .await
            .map_err(|e| crate::schema::SchemaError::InvalidData(e.to_string()))?;
        Ok(marker.as_deref() == Some(crate::atom::molecule_key_codec::HISTORY_KEY_FORMAT))
    }
}

use super::time_queue::TimeQueue;
use base64::Engine as _;
use sha2::{Digest, Sha256};

/// A bounded walk of the history rows under one exact prefix, from a cursor.
fn prefix_walk(
    base: &str,
    cursor: Option<&str>,
    limit: usize,
) -> Result<super::RangeWalk, crate::schema::SchemaError> {
    let (start, end) = lastdb_model::kind_partition::exact_prefix_bounds(base);
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

/// A history entry identity is the hex SHA-256 of its storage key.
fn validate_id(id: &str) -> Result<(), crate::schema::SchemaError> {
    if id.len() == 64 && id.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Ok(());
    }
    Err(crate::schema::SchemaError::InvalidData(
        "invalid history entry identity".into(),
    ))
}

pub(crate) fn queue() -> TimeQueue {
    TimeQueue::with_slice("ahq:v1", "ahb:v1", 3_600_000_000_000)
}

pub(crate) fn digest(value: &[u8]) -> String {
    lastdb_model::hex::hex_lower(Sha256::digest(value))
}
