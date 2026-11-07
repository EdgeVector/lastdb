//! The inline file of a record lives in its own row next to the record
//! (`decision-2026-11-07-lastdb-inline-file-own-row`).
//!
//! The row key is `fi:` and the record key, so the row is in the `records`
//! collection and the backup sends it with the records (R2). The record value
//! holds only the marker: a record read never fetches the bytes, and a write
//! that keeps the inline file leaves the row alone.

use super::{RecordAddress, RecordFormat};
use crate::error::FoldDbError;
use crate::storage::error::StorageResult;
use crate::storage::{KvMutation, StorageError};
use lastdb_model::record_value::{FileMarker, InlineFile};
use std::collections::BTreeMap;

pub(in crate::storage) const INLINE_FILE_FAMILY: &str = "fi";

pub(super) fn put(key: &RecordAddress, file: &InlineFile) -> StorageResult<KvMutation> {
    let value = file.encode().map_err(StorageError::InvalidOperation)?;
    Ok(KvMutation::put(address(key.as_str()), value))
}

pub(super) fn delete(key: &RecordAddress) -> KvMutation {
    KvMutation::delete(address(key.as_str()))
}

/// The record key of an inline file row.
pub(crate) fn record_key(key: &str) -> Option<&str> {
    let (family, record) = key.split_once(':')?;
    (family == INLINE_FILE_FAMILY).then_some(record)
}

/// The record key that an applied-delete or inline file row belongs to, or
/// the key itself.
pub(in crate::storage) fn owning_record(key: &str) -> &str {
    super::applied_deletes::record_key(key)
        .or_else(|| record_key(key))
        .unwrap_or(key)
}

pub(in crate::storage) fn address(record: &str) -> String {
    format!("{INLINE_FILE_FAMILY}:{record}")
}

/// Decode a row and require the marker that its record holds.
pub(in crate::storage) fn decode_for(
    bytes: &[u8],
    marker: &FileMarker,
) -> StorageResult<InlineFile> {
    let file = InlineFile::decode(bytes).map_err(StorageError::InvalidOperation)?;
    if &file.marker != marker {
        return Err(StorageError::InvalidOperation(
            "the inline file row differs from its record marker".into(),
        ));
    }
    Ok(file)
}

impl RecordFormat {
    /// The plaintext of the inline file of the record at `record_key`,
    /// checked against its marker. `None` when the row is absent.
    pub(crate) async fn open_inline_file(
        &self,
        record_key: &str,
        marker: &FileMarker,
    ) -> Result<Option<Vec<u8>>, FoldDbError> {
        let Some(row) = self
            .main_store
            .inner()
            .get(address(record_key).as_bytes())
            .await?
        else {
            return Ok(None);
        };
        let file = decode_for(&row, marker)?;
        Ok(Some(file.bytes().map_err(FoldDbError::Database)?))
    }

    /// The inline files of the given records, read in one batch. Each record
    /// key names the marker its record holds; a missing row is an error.
    pub(super) async fn inline_files(
        &self,
        wanted: Vec<(String, FileMarker)>,
    ) -> StorageResult<BTreeMap<String, InlineFile>> {
        if wanted.is_empty() {
            return Ok(BTreeMap::new());
        }
        let keys = wanted
            .iter()
            .map(|(key, _)| address(key).into_bytes())
            .collect();
        let rows = self.main_store.inner().get_many(keys).await?;
        if rows.len() != wanted.len() {
            return Err(StorageError::InvalidOperation(
                "the inline file read batch is incomplete".into(),
            ));
        }
        wanted
            .into_iter()
            .zip(rows)
            .map(|((key, marker), row)| {
                let row = row.ok_or_else(|| {
                    StorageError::InvalidOperation("a record's inline file row is missing".into())
                })?;
                Ok((key, decode_for(&row, &marker)?))
            })
            .collect()
    }
}
