//! Record external and inline file readers and local-file reference registration on [`FoldDB`].

use super::FoldDB;
use crate::error::FoldDbError;
use crate::schema::types::{KeyValue, SchemaError};
use crate::storage::current_keys::families::record_file_path;
use lastdb_model::record_value::{FileMarker, StoredRecord};
use std::path::Path;
use std::sync::Arc;

impl FoldDB {
    pub async fn remember_local_file(
        &self,
        schema_name: &str,
        key: &KeyValue,
        path: &Path,
        marker: &FileMarker,
    ) -> Result<(), FoldDbError> {
        self.db_ops
            .read_scope()
            .await?
            .run_result(async {
                let (rk, record) = self.read_file_record(schema_name, key).await?;
                let record = record.ok_or_else(|| {
                    FoldDbError::Database("local file reference has no record".into())
                })?;
                let (stored, ver) = record
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
                crate::storage::local_file_ref::remember(
                    &self.db_ops.native_runtime(),
                    &record_file_path(&rk, ver)?,
                    path,
                    &marker.sha256,
                    marker.size_bytes,
                )
                .map_err(FoldDbError::from)
            })
            .await
    }

    async fn read_file_record(
        &self,
        schema_name: &str,
        key: &KeyValue,
    ) -> Result<(String, Option<Arc<StoredRecord>>), FoldDbError> {
        let schema = self.schema_manager.require_schema_metadata(schema_name)?;
        let records = self.db_ops.records();
        let rk = records.record_key(&schema, key)?;
        let rec = records
            .read_records(std::slice::from_ref(&rk))
            .await?
            .pop()
            .flatten();
        Ok((rk, rec))
    }

    pub async fn read_record_file(
        &self,
        schema_name: &str,
        key: &KeyValue,
        revision: Option<&str>,
    ) -> Result<Option<(FileMarker, Vec<u8>)>, FoldDbError> {
        self.db_ops
            .read_scope()
            .await?
            .run_result(async {
                let (rk, record) = self.read_file_record(schema_name, key).await?;
                require_revision(schema_name, key, record.as_deref(), revision)?;
                let Some((marker, ver)) = record
                    .as_ref()
                    .and_then(|r| r.file.as_ref().zip(r.file_version.as_ref()))
                else {
                    return Ok(None);
                };
                let bytes = self
                    .db_ops
                    .records()
                    .format
                    .open_record_file(&rk, ver, marker)
                    .await?
                    .ok_or_else(|| {
                        FoldDbError::InProgress(
                            "the record's file is not on this device and could not be downloaded: no cloud backup is configured on this node".into(),
                        )
                    })?;
                Ok(Some((marker.clone(), bytes)))
            })
            .await
    }

    pub async fn read_record_inline_file(
        &self,
        schema_name: &str,
        key: &KeyValue,
        revision: Option<&str>,
    ) -> Result<Option<(FileMarker, Vec<u8>)>, FoldDbError> {
        self.db_ops
            .read_scope()
            .await?
            .run_result(async {
                let (rk, record) = self.read_file_record(schema_name, key).await?;
                require_revision(schema_name, key, record.as_deref(), revision)?;
                let Some(marker) = record.as_ref().and_then(|r| r.inline_file.as_ref()) else {
                    return Ok(None);
                };
                let bytes = self
                    .db_ops
                    .records()
                    .format
                    .open_inline_file(&rk, marker)
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
    if let Some(expected) = revision {
        let actual = record.map(|r| r.stamp.revision());
        if actual.as_deref() != Some(expected) {
            return Err(SchemaError::CasConflict {
                schema: schema_name.to_owned(),
                key: key.to_storage_key(),
                expected: expected.to_owned(),
                actual,
            }
            .into());
        }
    }
    Ok(())
}
