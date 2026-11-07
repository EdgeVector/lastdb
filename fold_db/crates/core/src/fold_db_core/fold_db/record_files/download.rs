//! Download a record file that is not on this device and install it locally
//! (design `design-lastdb-blob-files-on-demand-20261107`).
//!
//! Results for the caller:
//! - the plaintext, after the downloaded row passes the same decryption and
//!   marker checks as a local read, and after the row is installed;
//! - `SchemaError::NotFound` (404) when the cloud has no object at the path:
//!   another writer released it, or it was never uploaded;
//! - `FoldDbError::InProgress` (503, owner-visible) when no cloud source is
//!   registered or the cloud could not answer, with the reason;
//! - `FoldDbError::SecurityError` when the bytes differ from the marker.

use crate::error::FoldDbError;
use crate::fold_db_core::BlobFetch;
use crate::schema::SchemaError;
use crate::storage::format::records::RecordFormat;
use lastdb_model::portable_mutation::Position;
use lastdb_model::record_value::FileMarker;

use crate::fold_db_core::FoldDB;

/// What the local read found for the record's blob file.
pub(super) enum LocalFile {
    /// The record has no blob file.
    None,
    /// The local row, checked against the marker.
    Bytes(FileMarker, Vec<u8>),
    /// The record selects a file version whose row is not local.
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
    /// Fetch the sealed row of the selected version from the registered
    /// source, check it, install it, and return its plaintext.
    pub(super) async fn download_record_file(
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
                tracing::warn!(
                    target: "fold_db::record_files",
                    %reason,
                    "record file download failed"
                );
                Err(not_downloaded(reason))
            }
        }
    }
}
