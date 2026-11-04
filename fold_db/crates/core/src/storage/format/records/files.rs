//! Compose the existing record-owned file row with its record write.

use super::RecordAddress;
use crate::schema::SchemaError;
use crate::storage::current_keys::families::record_file_path;
use crate::storage::KvMutation;
use lastdb_model::record_value::StoredRecord;
use std::sync::Arc;

/// A changed file marker puts its sealed row; losing a marker deletes its
/// path. Sealing the file bytes remains the existing account-key operation.
pub(super) fn encode(
    key: &RecordAddress,
    before: Option<&StoredRecord>,
    after: &StoredRecord,
    row: Option<Arc<Vec<u8>>>,
) -> Result<Option<KvMutation>, SchemaError> {
    let had = before.is_some_and(|record| record.file.is_some());
    let stamp_of = |record: Option<&StoredRecord>| {
        record.and_then(|record| record.file.as_ref().map(|file| file.stamp.clone()))
    };
    let set = after.file.is_some() && stamp_of(before) != stamp_of(Some(after));
    Ok(match row {
        Some(row) if set => Some(KvMutation::put(
            record_file_path(key.as_str())?.into_bytes(),
            Arc::try_unwrap(row).unwrap_or_else(|row| row.as_ref().clone()),
        )),
        _ if had && after.file.is_none() => Some(KvMutation::delete(
            record_file_path(key.as_str())?.into_bytes(),
        )),
        _ => None,
    })
}
