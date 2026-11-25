//! Where a record's file is on this machine, for callers that take a path and not bytes.

use std::path::PathBuf;

use lastdb_model::record_value::FileMarker;

use super::FoldDB;
use crate::error::FoldDbError;
use crate::schema::types::KeyValue;
use crate::storage::current_keys::families::record_file_path;

/// The path LastDB holds for a record's file, and whether the file is still
/// there at the size it had when the record was written.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalRecordFile {
    pub path: PathBuf,
    pub intact: bool,
}

impl FoldDB {
    /// The local file a record refers to. `None` when the record has no file,
    /// or holds its file as bytes in the home and not as a reference.
    pub async fn record_file_local_path(
        &self,
        schema_name: &str,
        key: &KeyValue,
    ) -> Result<Option<LocalRecordFile>, FoldDbError> {
        self.db_ops
            .read_scope()
            .await?
            .run_result(async {
                let (rk, record) = self.read_file_record(schema_name, key).await?;
                let Some((marker, ver)) = record
                    .as_ref()
                    .and_then(|r| r.file.as_ref().zip(r.file_version.as_ref()))
                else {
                    return Ok(None);
                };
                let found = crate::storage::local_file_ref::referenced_path(
                    &self.db_ops.native_runtime(),
                    &record_file_path(&rk, ver)?,
                    marker,
                )?;
                Ok(found.map(|(path, intact)| LocalRecordFile { path, intact }))
            })
            .await
    }

    /// Bytes LastDB holds for a record's file. `None` when the record has no
    /// file, names a local reference, or does not hold the bytes in its home.
    pub async fn record_file_owned_bytes(
        &self,
        schema_name: &str,
        key: &KeyValue,
    ) -> Result<Option<(FileMarker, Vec<u8>)>, FoldDbError> {
        self.db_ops
            .read_scope()
            .await?
            .run_result(async {
                let (rk, record) = self.read_file_record(schema_name, key).await?;
                let Some((marker, ver)) = record
                    .as_ref()
                    .and_then(|r| r.file.as_ref().zip(r.file_version.as_ref()))
                else {
                    return Ok(None);
                };
                let referenced = crate::storage::local_file_ref::referenced_path(
                    &self.db_ops.native_runtime(),
                    &record_file_path(&rk, ver)?,
                    marker,
                )?;
                if referenced.is_some() {
                    return Ok(None);
                }
                let bytes = self
                    .db_ops
                    .records()
                    .format
                    .open_owned_record_file(&rk, ver, marker)
                    .await?;
                Ok(bytes.map(|bytes| (marker.clone(), bytes)))
            })
            .await
    }
}
