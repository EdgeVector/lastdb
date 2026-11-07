//! The record provider delegates file encoding/reading to its format capability.

use super::RecordStore;
use crate::error::FoldDbError;
use crate::schema::{types::FileWrite, SchemaError};
use crate::storage::format::records::RecordAddress;
use cloud_backup_protocol::v2::blobs::BlobPath;
use lastdb_model::portable_mutation::Position;
use lastdb_model::record_value::FileMarker;

impl RecordStore {
    pub(crate) async fn portable_file(
        &self,
        key: &RecordAddress,
        version: &Position,
        marker: &FileMarker,
    ) -> Result<Vec<u8>, SchemaError> {
        self.format.portable_file(key, version, marker).await
    }

    pub(crate) fn admit_portable_file(
        &self,
        marker: FileMarker,
        sealed: Vec<u8>,
    ) -> Result<FileWrite, SchemaError> {
        self.format.admit_portable_file(marker, sealed)
    }

    pub(crate) fn seal_record_file(
        &self,
        plaintext: &[u8],
        name: Option<String>,
        media_type: Option<String>,
    ) -> Result<FileWrite, FoldDbError> {
        self.format.seal_record_file(plaintext, name, media_type)
    }

    pub(crate) async fn open_inline_file(
        &self,
        record_key: &str,
        marker: &FileMarker,
    ) -> Result<Option<Vec<u8>>, FoldDbError> {
        self.format.open_inline_file(record_key, marker).await
    }

    /// True when this home is a dedicated shared database.
    pub(crate) fn dedicated_shared_home(&self) -> bool {
        self.format.dedicated_shared_home()
    }

    /// The cloud object of one record file version.
    pub(crate) fn record_blob_path(
        &self,
        record_key: &str,
        version: &Position,
    ) -> Result<BlobPath, FoldDbError> {
        self.format.record_blob_path(record_key, version)
    }

    /// Check a downloaded sealed row and install it at the record's file path.
    pub(crate) async fn install_record_file(
        &self,
        record_key: &str,
        version: &Position,
        marker: &FileMarker,
        row: Vec<u8>,
    ) -> Result<Vec<u8>, FoldDbError> {
        self.format
            .install_record_file(record_key, version, marker, row)
            .await
    }

    pub(crate) async fn reference_local_file(
        &self,
        path: &std::path::Path,
        name: Option<String>,
        media_type: Option<String>,
    ) -> Result<FileWrite, FoldDbError> {
        self.format
            .reference_local_file(path, name, media_type)
            .await
    }

    pub(crate) async fn open_record_file(
        &self,
        record_key: &str,
        version: &Position,
        marker: &FileMarker,
    ) -> Result<Option<Vec<u8>>, FoldDbError> {
        self.format
            .open_record_file(record_key, version, marker)
            .await
    }
}
