//! Optional field history of records. Each entry keeps its own copy of one old
//! field value. A file is not a field value, so no entry holds a file. The
//! home row `layout:field-history` turns it on.

mod key_counts;
mod policy;
mod record_reads;
mod records;
mod release;
pub use crate::records::field_history::{
    FieldHistoryEntry, FieldHistoryPage, FieldHistoryPolicy, HistoryLocation,
};
pub(crate) use records::RecordHistoryRows;

impl super::RecordStore {
    pub(crate) fn history_activation(&self) -> &tokio::sync::Notify {
        &self.history_activation
    }

    pub(crate) async fn field_history_enabled(&self) -> Result<bool, crate::schema::SchemaError> {
        let setting = self
            .main_store
            .inner()
            .get(crate::storage::format::control::FIELD_HISTORY_KEY.as_bytes())
            .await
            .map_err(|e| crate::schema::SchemaError::InvalidData(e.to_string()))?;
        Ok(setting.as_deref() == Some(crate::storage::format::control::FIELD_HISTORY_ON))
    }
}

use base64::Engine as _;

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

pub(crate) use crate::storage::format::history::queue;
