//! Replay remembers an erasure without leaving a record at the live key.

use super::{RecordAddress, RecordFormat};
use crate::storage::error::StorageResult;
use crate::storage::{KvMutation, StorageError};
use lastdb_model::portable_mutation::{AppliedDelete, Stamp};

pub(in crate::storage) const APPLIED_DELETE_FAMILY: &str = "ad";
const MAX_DELETE_BYTES: usize = 512;

pub(super) fn encode(key: &RecordAddress, stamp: &Stamp) -> StorageResult<KvMutation> {
    let value = serde_json::to_vec(&AppliedDelete::from(stamp.clone())).map_err(invalid)?;
    Ok(KvMutation::put(address(key.as_str()), value))
}

pub(in crate::storage) fn decode(bytes: &[u8]) -> StorageResult<Stamp> {
    if bytes.len() > MAX_DELETE_BYTES {
        return Err(invalid("encoded value exceeds its bound"));
    }
    let entry: AppliedDelete = serde_json::from_slice(bytes).map_err(invalid)?;
    entry.delete_position.validate().map_err(invalid)?;
    Ok(entry.into())
}

pub(in crate::storage) fn record_key(key: &str) -> Option<&str> {
    let (family, record) = key.split_once(':')?;
    (family == APPLIED_DELETE_FAMILY).then_some(record)
}

fn address(record: &str) -> String {
    format!("{APPLIED_DELETE_FAMILY}:{record}")
}

impl RecordFormat {
    pub(crate) async fn applied_deletes(
        &self,
        keys: &[&RecordAddress],
    ) -> StorageResult<Vec<Option<Stamp>>> {
        let rows = self
            .main_store
            .inner()
            .get_many(
                keys.iter()
                    .map(|key| address(key.as_str()).into_bytes())
                    .collect(),
            )
            .await?;
        if rows.len() != keys.len() {
            return Err(invalid("incomplete applied-delete read"));
        }
        rows.iter()
            .map(|row| row.as_deref().map(decode).transpose())
            .collect()
    }
}

fn invalid(error: impl std::fmt::Display) -> StorageError {
    StorageError::InvalidOperation(format!("applied delete: {error}"))
}
