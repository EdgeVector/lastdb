//! Record file sealing, local references, inline reads, and on-demand cloud downloads.

use super::FoldDB;
use crate::error::FoldDbError;
use crate::fold_db_core::{BlobFetch, SIGN_IN_TO_FETCH_FILE};
use crate::schema::types::{FileWrite, KeyValue, SchemaError};
use crate::storage::current_keys::families::record_file_path;
use crate::storage::format::records::RecordFormat;
use lastdb_model::portable_mutation::Position;
use lastdb_model::record_value::{FileMarker, StoredRecord};
use std::path::Path;
use std::sync::Arc;

enum LocalFile {
    None,
    Bytes(FileMarker, Vec<u8>),
    Missing {
        record_key: String,
        marker: FileMarker,
        version: Position,
    },
}

fn not_downloaded(reason: impl std::fmt::Display) -> FoldDbError {
    FoldDbError::InProgress(format!(
        "the record's file is not on this device and could not be downloaded: {reason}"
    ))
}

impl FoldDB {
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

    pub async fn reference_local_file(
        &self,
        path: &Path,
        name: Option<String>,
        media_type: Option<String>,
    ) -> Result<FileWrite, FoldDbError> {
        self.db_ops()
            .records()
            .reference_local_file(path, name, media_type)
            .await
    }

    pub async fn remember_local_file(
        &self,
        schema_name: &str,
        key: &KeyValue,
        path: &Path,
        marker: &FileMarker,
    ) -> Result<(), FoldDbError> {
        self.db_ops()
            .read_scope()
            .await?
            .run_result(async {
                let (record_key, record) = self.read_file_record(schema_name, key).await?;
                let record = record.ok_or_else(|| {
                    FoldDbError::Database("local file reference has no record".into())
                })?;
                let (stored, version) = record
                    .file
                    .as_ref()
                    .zip(record.file_version.as_ref())
                    .ok_or_else(|| {
                        FoldDbError::Database("local file reference has no file version".into())
                    })?;
                if stored != marker {
                    return Err(FoldDbError::Database(
                        "local file reference does not match the committed marker".into(),
                    ));
                }
                let cas_key = record_file_path(&record_key, version)?;
                crate::storage::local_file_ref::remember(
                    &self.db_ops().native_runtime(),
                    &cas_key,
                    path,
                    &marker.sha256,
                    marker.size_bytes,
                )?;
                Ok(())
            })
            .await
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

    pub async fn read_record_file(
        &self,
        schema_name: &str,
        key: &KeyValue,
    ) -> Result<Option<(FileMarker, Vec<u8>)>, FoldDbError> {
        self.read_record_file_checked(schema_name, key, None).await
    }

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
                let bytes = self
                    .download_record_file(&record_key, &version, &marker)
                    .await?;
                Ok(Some((marker, bytes)))
            }
        }
    }

    async fn download_record_file(
        &self,
        record_key: &str,
        version: &Position,
        marker: &FileMarker,
    ) -> Result<Vec<u8>, FoldDbError> {
        let Some(source) = self.record_blob_source() else {
            return Err(not_downloaded("no cloud backup is configured on this node"));
        };
        let records = self.db_ops().records();
        let path = records.record_blob_path(record_key, version)?;
        let sealed_bytes = RecordFormat::sealed_file_bytes(marker)?;
        match source.fetch(&path, sealed_bytes).await {
            BlobFetch::Bytes(row) => {
                records
                    .install_record_file(record_key, version, marker, row)
                    .await
            }
            BlobFetch::Absent => Err(SchemaError::NotFound(
                "the record's file is not in the cloud: another writer deleted it, or it was \
                 never uploaded"
                    .into(),
            )
            .into()),
            BlobFetch::Unavailable(reason) => {
                if reason == SIGN_IN_TO_FETCH_FILE {
                    return Err(FoldDbError::InProgress(reason));
                }
                tracing::warn!(
                    target: "fold_db::record_files",
                    %reason,
                    "record file download failed"
                );
                Err(not_downloaded(reason))
            }
        }
    }

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
