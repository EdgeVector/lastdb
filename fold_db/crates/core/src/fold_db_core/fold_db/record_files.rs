//! The file of one record, by schema and key (design
//! `design-lastdb-no-shared-file-blobs`).
//!
//! `put-blob` seals the bytes here and writes them with the record in one
//! native request (`FileWrite::Put`). `fetch-blob` reads the record, takes its
//! marker, reads the record's file path and checks the bytes against the
//! marker. No request names a pointer, a path or a key.

use std::sync::Arc;

use crate::db_operations::record_store::record_scope;
use crate::error::FoldDbError;
use crate::schema::types::{FileWrite, KeyValue};
use crate::schema::SchemaError;
use lastdb_model::record_value::FileMarker;

use super::FoldDB;

/// What one record holds of a file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecordFileState {
    /// No live record at the key.
    NoRecord,
    /// A live record without a file.
    NoFile,
    /// A live record with this file.
    File(FileMarker),
}

impl FoldDB {
    /// Seal `plaintext` under the account key for a `put-blob` write. The
    /// returned file write carries the marker and the sealed row; the record
    /// write puts the row at the record's file path in its own batch.
    pub fn seal_record_file(
        &self,
        plaintext: &[u8],
        name: Option<String>,
        media_type: Option<String>,
    ) -> Result<FileWrite, FoldDbError> {
        if plaintext.len() as u64
            > lastdb_storage_policy::native_write::LOCAL_FILE_INPUT_MEMORY_BYTES
        {
            return Err(FoldDbError::Schema(SchemaError::WriteResourceLimit {
                detail: "local file input exceeds native pending memory allowance".into(),
            }));
        }
        let (marker, row) = self
            .db_ops()
            .records()
            .seal_record_file(plaintext, name, media_type)?;
        Ok(FileWrite::Put {
            marker,
            row: Arc::new(row),
        })
    }

    /// The file state of the record at `key`, and its storage key.
    pub async fn record_file_state(
        &self,
        schema_name: &str,
        key: &KeyValue,
    ) -> Result<(String, RecordFileState), FoldDbError> {
        let schema = self.schema_manager().require_schema_metadata(schema_name)?;
        let records = self.db_ops().records();
        let record_key = records.record_key(&schema, key, record_scope(&schema).as_deref())?;
        let record = records
            .read_records(std::slice::from_ref(&record_key))
            .await?
            .pop()
            .flatten();
        let state = match record {
            Some(record) if record.is_live() => match &record.file {
                Some(file) => RecordFileState::File(file.marker()),
                None => RecordFileState::NoFile,
            },
            _ => RecordFileState::NoRecord,
        };
        Ok((record_key, state))
    }

    /// The file of the record at `key`: its marker and its plaintext, checked
    /// against the marker. `None` when the record or its file is absent.
    pub async fn read_record_file(
        &self,
        schema_name: &str,
        key: &KeyValue,
    ) -> Result<Option<(FileMarker, Vec<u8>)>, FoldDbError> {
        let (record_key, state) = self.record_file_state(schema_name, key).await?;
        let RecordFileState::File(marker) = state else {
            return Ok(None);
        };
        let bytes = self
            .db_ops()
            .records()
            .open_record_file(&record_key, &marker)
            .await?;
        Ok(bytes.map(|bytes| (marker, bytes)))
    }
}
