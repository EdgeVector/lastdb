//! The file of one record, by schema and key (design
//! `design-lastdb-no-shared-file-blobs`).
//!
//! `put-blob` seals the bytes here and writes them with the record in one
//! native request (`FileWrite::Put`). `fetch-blob` reads the record, takes its
//! marker, reads the record's file path and checks the bytes against the
//! marker. No request names a pointer, a path or a key.
//!
//! A home restored from a backup has the records but not their blob rows
//! (design `design-lastdb-blob-files-on-demand-20261107`). When the record
//! selects a file version whose row is not local, the read asks the registered
//! [`crate::fold_db_core::RecordBlobSource`] for the sealed row at the record's
//! cloud path, checks it as a local read does, and installs it locally
//! (`download.rs`). A dedicated shared database never downloads.

use crate::error::FoldDbError;
use crate::schema::types::{FileWrite, KeyValue};
use crate::schema::SchemaError;
use lastdb_model::record_value::{FileMarker, StoredRecord};
use std::sync::Arc;

use super::FoldDB;

mod download;
use download::LocalFile;

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
        // The key codec comes from the schema's field key bundles. After a
        // restart they load on first use, as the query path does.
        self.db_ops()
            .field_keys()
            .load_schema(&schema)
            .await
            .map_err(|error| {
                SchemaError::InvalidData(format!(
                    "load field key bundles for schema '{schema_name}': {error}"
                ))
            })?;
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
        let local = self
            .db_ops()
            .read_scope()
            .await?
            .run_result(async {
                let (record_key, record) = self.read_file_record(schema_name, key).await?;
                require_revision(schema_name, key, record.as_deref(), revision)?;
                let Some((marker, version)) = record
                    .as_ref()
                    .and_then(|record| record.file.as_ref().zip(record.file_version.as_ref()))
                else {
                    return Ok(LocalFile::None);
                };
                let bytes = self
                    .db_ops()
                    .records()
                    .open_record_file(&record_key, version, marker)
                    .await?;
                Ok::<_, FoldDbError>(match bytes {
                    Some(bytes) => LocalFile::Bytes(marker.clone(), bytes),
                    None => LocalFile::Missing {
                        record_key,
                        marker: marker.clone(),
                        version: version.clone(),
                    },
                })
            })
            .await?;
        match local {
            LocalFile::None => Ok(None),
            LocalFile::Bytes(marker, bytes) => Ok(Some((marker, bytes))),
            LocalFile::Missing {
                record_key,
                marker,
                version,
            } => {
                if self.db_ops().records().dedicated_shared_home() {
                    // A shared database keeps its files in its own home.
                    return match revision {
                        Some(_) => Err(FoldDbError::Database(
                            "record file bytes are missing".into(),
                        )),
                        None => Ok(None),
                    };
                }
                let bytes = self
                    .download_record_file(&record_key, &version, &marker)
                    .await?;
                Ok(Some((marker, bytes)))
            }
        }
    }

    /// The inline file of the record at `key`: its marker and its plaintext
    /// from the inline file row, checked against the marker. `None` when the
    /// record has no inline file. With `revision`, the record must still be at
    /// that revision. A marker without its row is an error.
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
                let (record_key, record) = self.read_file_record(schema_name, key).await?;
                require_revision(schema_name, key, record.as_deref(), revision)?;
                let Some(marker) = record
                    .as_ref()
                    .and_then(|record| record.inline_file.as_ref())
                else {
                    return Ok(None);
                };
                let bytes = self
                    .db_ops()
                    .records()
                    .open_inline_file(&record_key, marker)
                    .await?
                    .ok_or_else(|| {
                        FoldDbError::Database("record inline file bytes are missing".into())
                    })?;
                Ok(Some((marker.clone(), bytes)))
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
