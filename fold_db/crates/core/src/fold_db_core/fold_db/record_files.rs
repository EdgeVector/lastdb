//! The file of one record, by schema and key (design
//! `design-lastdb-no-shared-file-blobs`).
//!
//! `put-blob` seals the bytes here and writes them with the record in one
//! native request (`FileWrite::Put`). `fetch-blob` reads the record, takes its
//! marker, reads the record's file path and checks the bytes against the
//! marker. No request names a pointer, a path or a key.

use crate::error::FoldDbError;
use crate::schema::types::{FileWrite, KeyValue};
use crate::schema::SchemaError;
use lastdb_model::record_value::{FileMarker, StoredRecord};
use std::sync::Arc;

use super::FoldDB;

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
        self.db_ops()
            .records()
            .seal_record_file(plaintext, name, media_type)
    }

    async fn read_file_record(
        &self,
        schema_name: &str,
        key: &KeyValue,
    ) -> Result<(String, Option<Arc<StoredRecord>>), FoldDbError> {
        let schema = self.schema_manager().require_schema_metadata(schema_name)?;
        let records = self.db_ops().records();
        let record_key = records.record_key(&schema, key)?;
        let record = records
            .read_records(std::slice::from_ref(&record_key))
            .await?
            .pop()
            .flatten();
        Ok((record_key, record))
    }

    /// The file of the record at `key`: its marker and its plaintext, checked
    /// against the marker. `None` when the record or its file is absent.
    pub async fn read_record_file(
        &self,
        schema_name: &str,
        key: &KeyValue,
    ) -> Result<Option<(FileMarker, Vec<u8>)>, FoldDbError> {
        self.read_record_file_checked(schema_name, key, None).await
    }

    /// Read the file only if the exact record revision is still available.
    /// A promised file with missing bytes is an error.
    pub async fn read_record_file_at_revision(
        &self,
        schema_name: &str,
        key: &KeyValue,
        revision: &str,
    ) -> Result<Option<(FileMarker, Vec<u8>)>, FoldDbError> {
        self.read_record_file_checked(schema_name, key, Some(revision))
            .await
    }

    async fn read_record_file_checked(
        &self,
        schema_name: &str,
        key: &KeyValue,
        revision: Option<&str>,
    ) -> Result<Option<(FileMarker, Vec<u8>)>, FoldDbError> {
        self.db_ops()
            .read_scope()
            .await?
            .run_result(async {
                let (record_key, record) = self.read_file_record(schema_name, key).await?;
                require_revision(schema_name, key, record.as_deref(), revision)?;
                let Some((marker, version)) = record
                    .as_ref()
                    .and_then(|record| record.file.as_ref().zip(record.file_version.as_ref()))
                else {
                    return Ok(None);
                };
                let bytes = self
                    .db_ops()
                    .records()
                    .open_record_file(&record_key, version, marker)
                    .await?;
                if revision.is_some() && bytes.is_none() {
                    return Err(FoldDbError::Database(
                        "record file bytes are missing".into(),
                    ));
                }
                Ok(bytes.map(|bytes| (marker.clone(), bytes)))
            })
            .await
    }

    /// The inline file of the record at `key`: its marker and its plaintext,
    /// checked against the marker. `None` when the record has no inline file.
    /// With `revision`, the record must still be at that revision.
    pub async fn read_record_inline_file(
        &self,
        schema_name: &str,
        key: &KeyValue,
        revision: Option<&str>,
    ) -> Result<Option<(FileMarker, Vec<u8>)>, FoldDbError> {
        self.db_ops()
            .read_scope()
            .await?
            .run_result(async {
                let (_, record) = self.read_file_record(schema_name, key).await?;
                require_revision(schema_name, key, record.as_deref(), revision)?;
                let Some(file) = record
                    .as_ref()
                    .and_then(|record| record.inline_file.as_ref())
                else {
                    return Ok(None);
                };
                let bytes = file.bytes().map_err(FoldDbError::Database)?;
                Ok(Some((file.marker.clone(), bytes)))
            })
            .await
    }
}

/// A file read at a revision requires the record to be at that revision.
fn require_revision(
    schema_name: &str,
    key: &KeyValue,
    record: Option<&StoredRecord>,
    revision: Option<&str>,
) -> Result<(), FoldDbError> {
    let Some(expected) = revision else {
        return Ok(());
    };
    let actual = record.map(|record| record.stamp.revision());
    if actual.as_deref() != Some(expected) {
        return Err(SchemaError::CasConflict {
            schema: schema_name.to_owned(),
            key: key.to_storage_key(),
            expected: expected.to_owned(),
            actual,
        }
        .into());
    }
    Ok(())
}
