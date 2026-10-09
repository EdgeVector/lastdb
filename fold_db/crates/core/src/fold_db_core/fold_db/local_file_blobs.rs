//! Local file-blob PUT: store one blob in the node's own `cas_blobs` plane.
//!
//! Nothing here touches the network or the sync engine. The cloud mode
//! ([`FoldDB::put_personal_file_blob`](super::FoldDB)) uploads first and then
//! writes a record; this path only makes the bytes durable and hands back the
//! `$lastdb_file` pointer. The caller writes the pointer into a record of its
//! choice, in a batch it controls (for example together with a commit marker).

use std::sync::Arc;

use serde_json::Value;

use crate::durable_flush::{self, BatchPlacementLog};
use crate::error::FoldDbError;
use crate::sharing::blob_cas::{self, SealedPutOutcome};
use crate::sharing::delivery_wire::lastdb_file_pointer_with_access_and_thumbnail;
use crate::sharing::file_blob_seal::local_file_blob_ref;

use super::FoldDB;

/// The namespace that holds the rows. Must match `blob_cas`.
const CAS_BLOBS_NAMESPACE: &str = "cas_blobs";

/// Largest blob one local put accepts, unless the owner raises it.
///
/// One slab of the LastGit pack design. A sealed row is about 1.33 times the
/// plaintext, and a compaction or a cold load holds a few rows of one group at
/// once, so this keeps both near tens of MB. Larger blobs belong in slabs.
pub const LOCAL_FILE_BLOB_MAX_BYTES_DEFAULT: usize = 16 * 1024 * 1024;

/// Environment knob that overrides [`LOCAL_FILE_BLOB_MAX_BYTES_DEFAULT`]. A
/// value that is not a positive integer is ignored.
pub const LOCAL_FILE_BLOB_MAX_BYTES_ENV: &str = "LASTDB_LOCAL_FILE_BLOB_MAX_BYTES";

/// The largest plaintext one local put accepts on this node.
#[must_use]
pub fn local_file_blob_max_bytes() -> usize {
    env_flag::var_parsed::<usize>(LOCAL_FILE_BLOB_MAX_BYTES_ENV)
        .filter(|max| *max > 0)
        .unwrap_or(LOCAL_FILE_BLOB_MAX_BYTES_DEFAULT)
}

/// Result of [`FoldDB::put_local_file_blob`].
#[derive(Debug, Clone)]
pub struct LocalFileBlobPut {
    /// The `$lastdb_file` field value. Write it as the whole value of a field
    /// of type `Any`; a pointer inside a JSON string is not a reference.
    pub pointer: Value,
    pub blob_ref: String,
    pub file_hash: String,
    /// Plaintext length.
    pub bytes: usize,
    pub outcome: SealedPutOutcome,
}

impl LocalFileBlobPut {
    /// True when this call wrote a row that did not exist. False when an
    /// intact row for the same bytes was already there.
    #[must_use]
    pub fn stored(&self) -> bool {
        !self.outcome.existed()
    }
}

impl FoldDB {
    /// Store `plaintext` in the local `cas_blobs` plane and return its pointer.
    ///
    /// - The row is sealed under the convergent per-blob key, so identical
    ///   bytes give an identical `blob_ref`, DEK and pointer on every node.
    /// - The pointer carries no `name` or `media_type` unless the caller passes
    ///   them. Without them, identical bytes in one schema give the same atom,
    ///   so a second record that holds the pointer adds one tip and no atom.
    /// - The row is on disk before this returns. A durable record batch only
    ///   flushes the groups it wrote, not `cas_blobs`, so without this barrier a
    ///   crash could keep the record that names the blob and lose the blob. The
    ///   barrier syncs the one group that holds the row, not the whole store.
    /// - A repeat put of bytes that are already stored does not append a second
    ///   copy; see [`blob_cas::put_blob_sealed_under_dek_dedup_in_ops`].
    /// - A blob over [`local_file_blob_max_bytes`] is refused.
    ///
    /// No record is written. The row is unreferenced until the caller writes
    /// the pointer, and `gc-file-blobs` will not reclaim it for at least 300 s.
    pub async fn put_local_file_blob(
        &self,
        plaintext: &[u8],
        name: Option<&str>,
        media_type: Option<&str>,
    ) -> Result<LocalFileBlobPut, FoldDbError> {
        let max = local_file_blob_max_bytes();
        if plaintext.len() > max {
            return Err(FoldDbError::Config(format!(
                "local file blob is {} bytes, over the {max} byte limit \
                 (store large files as slabs, or raise {LOCAL_FILE_BLOB_MAX_BYTES_ENV})",
                plaintext.len()
            )));
        }
        let blob_ref = local_file_blob_ref(plaintext)?;
        let access = blob_ref.to_access();
        // Record which group the write lands in, so the barrier below syncs
        // that group and not every dirty group on the node.
        let placements = BatchPlacementLog::shared();
        let outcome = durable_flush::scope(
            Arc::clone(&placements),
            blob_cas::put_blob_sealed_under_dek_dedup_in_ops(
                self.db_ops(),
                plaintext,
                media_type.map(str::to_string),
                name.map(str::to_string),
                &blob_ref.dek,
            ),
        )
        .await?;
        self.flush_local_blob_row(&placements, &blob_ref.blob_ref)
            .await?;
        let pointer = lastdb_file_pointer_with_access_and_thumbnail(
            &blob_ref.blob_ref,
            name,
            media_type,
            Some(&access),
            None,
        );
        Ok(LocalFileBlobPut {
            pointer,
            blob_ref: blob_ref.blob_ref,
            file_hash: blob_ref.file_hash,
            bytes: plaintext.len(),
            outcome,
        })
    }

    /// Make the row for `blob_ref` durable.
    ///
    /// A put that wrote placed its group in `placements`. A put that found an
    /// intact young row wrote nothing, but that row may itself be unflushed (a
    /// cloud-cache write or a delivery import has no barrier), so its group is
    /// named from the key. Only a store with no LastStore falls back to the
    /// store-wide flush.
    async fn flush_local_blob_row(
        &self,
        placements: &BatchPlacementLog,
        blob_ref: &str,
    ) -> Result<(), FoldDbError> {
        let mut written = placements.written_keys();
        if written.is_empty() {
            written.extend(
                self.db_ops()
                    .namespace_row_group(CAS_BLOBS_NAMESPACE, blob_ref),
            );
        }
        if written.is_empty() {
            self.db_ops().flush().await?;
        } else {
            self.db_ops().flush_dirty_scope(&written).await?;
        }
        Ok(())
    }
}
