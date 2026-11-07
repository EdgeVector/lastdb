//! Install a record file that the node downloaded on request (design
//! `design-lastdb-blob-files-on-demand-20261107`).
//!
//! A home restored from a backup has the records but not their blob rows.
//! `fetch-blob` downloads the sealed row of the selected version from the
//! cloud, checks it here exactly as a local read does, and writes it back to
//! the record's file path. The install is a plain local write of that row and
//! its uploaded marker: it does not change the record and makes no record
//! mutation, and backup never uploads the row again.

use super::file_crypto::{require_row, sealed_row_bytes};
use super::RecordFormat;
use crate::error::FoldDbError;
use crate::storage::current_keys::families::{
    record_blob_path, record_file_path, record_file_uploaded_path,
};
use crate::storage::disk_pack::transaction;
use crate::storage::KvMutation;
use cloud_backup_protocol::v2::blobs::BlobPath;
use lastdb_model::portable_mutation::Position;
use lastdb_model::record_value::{FileMarker, StoredRecord};

fn differs(detail: &str) -> FoldDbError {
    FoldDbError::SecurityError(format!("downloaded record file {detail}"))
}

impl RecordFormat {
    /// True when this home is a dedicated shared database. Its files never
    /// come from the owner's cloud blob store.
    pub(crate) fn dedicated_shared_home(&self) -> bool {
        self.runtime.is_dedicated_shared_home()
    }

    /// The cloud object of one record file version, under this home's
    /// database identity (the main run home identity, which a restore keeps).
    pub(crate) fn record_blob_path(
        &self,
        record_key: &str,
        version: &Position,
    ) -> Result<BlobPath, FoldDbError> {
        let database =
            cloud_backup_protocol::Id::try_from(self.runtime.native_home_id().to_string())
                .map_err(|_| FoldDbError::Database("the home identity is not a cloud id".into()))?;
        Ok(record_blob_path(&database, record_key, version)?)
    }

    /// The exact length of the sealed row of a file with this marker.
    pub(crate) fn sealed_file_bytes(marker: &FileMarker) -> Result<u64, FoldDbError> {
        sealed_row_bytes(marker).ok_or_else(|| differs("size overflows"))
    }

    /// Check a downloaded sealed row against the record's marker and return
    /// its plaintext. While the record still selects this version and its
    /// path holds no row, write the row and its uploaded marker in one batch.
    pub(crate) async fn install_record_file(
        &self,
        record_key: &str,
        version: &Position,
        marker: &FileMarker,
        row: Vec<u8>,
    ) -> Result<Vec<u8>, FoldDbError> {
        if Self::sealed_file_bytes(marker)? != row.len() as u64 {
            return Err(differs("has a different length than its marker"));
        }
        require_row(self.content_key.as_ref(), marker, &row)
            .map_err(|_| differs("differs from its format or marker"))?;
        let plaintext = self.open_file_row(&row, marker)?;
        let path = record_file_path(record_key, version)?.into_bytes();
        let uploaded = record_file_uploaded_path(&path)?;
        transaction::request_scope(self.runtime(), async {
            // The write gate is held for the whole scope, so the record cannot
            // move to another version between this read and the staged row.
            let current = self
                .main_store
                .inner()
                .get(record_key.as_bytes())
                .await?
                .map(|bytes| StoredRecord::decode(&bytes))
                .transpose()
                .map_err(super::record_value_error)?;
            let selected = current.as_ref().is_some_and(|record| {
                record.file_version.as_ref() == Some(version)
                    && record.file.as_ref() == Some(marker)
            });
            if !selected {
                return Ok::<_, FoldDbError>(());
            }
            let store = self.file_store().await?;
            if store.get(&path).await?.is_some() {
                return Ok(());
            }
            store
                .batch_mutate(vec![
                    KvMutation::put(path, row),
                    KvMutation::put(uploaded, vec![1]),
                ])
                .await?;
            Ok(())
        })
        .await?;
        Ok(plaintext)
    }
}
