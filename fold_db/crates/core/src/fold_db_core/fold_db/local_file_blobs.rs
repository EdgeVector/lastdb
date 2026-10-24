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

use super::FoldDB;

/// The namespace that holds the rows. Must match `blob_cas`.
const CAS_BLOBS_NAMESPACE: &str = "cas_blobs";

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
    /// - Identical bytes in one account give the same file key and ciphertext
    ///   reference. The account key stays private inside the atom store.
    /// - The pointer carries no `name` or `media_type` unless the caller passes
    ///   them. Without them, identical bytes in one schema give the same atom,
    ///   so a second record that holds the pointer adds one tip and no atom.
    /// - The row is on disk before this returns. A durable record batch only
    ///   flushes the groups it wrote, not `cas_blobs`, so without this barrier a
    ///   crash could keep the record that names the blob and lose the blob. The
    ///   barrier syncs the one group that holds the row, not the whole store.
    /// - A repeat put of bytes that are already stored does not append a second
    ///   copy; see [`blob_cas::put_blob_sealed_with_access_dedup_in_ops`].
    /// - The native record format, pending memory and disk space admit the write.
    ///
    /// No record is written. The put registers the upload with the blob counter,
    /// in the same native request as the row. The blob has no reference until the
    /// caller writes the pointer, so it stays for the dwell
    /// (`LASTDB_BLOB_DWELL_SECS`, one hour by default). The dwell pass deletes it
    /// after that if no atom has named it.
    pub async fn put_local_file_blob(
        &self,
        plaintext: &[u8],
        name: Option<&str>,
        media_type: Option<&str>,
    ) -> Result<LocalFileBlobPut, FoldDbError> {
        let put = self.put_local_file_blob_admitted(plaintext, name, media_type);
        self.acquire_local_mutation_admission()
            .await
            .run(Box::pin(put))
            .await
    }

    async fn put_local_file_blob_admitted(
        &self,
        plaintext: &[u8],
        name: Option<&str>,
        media_type: Option<&str>,
    ) -> Result<LocalFileBlobPut, FoldDbError> {
        if plaintext.len() as u64
            > lastdb_storage_policy::native_write::LOCAL_FILE_INPUT_MEMORY_BYTES
        {
            return Err(FoldDbError::Schema(
                crate::schema::SchemaError::WriteResourceLimit {
                    detail: "local file input exceeds native pending memory allowance".into(),
                },
            ));
        }
        let (blob_ref, envelope) = self.db_ops().atoms().seal_local_file_blob(plaintext)?;
        let access = blob_ref.to_access();
        // Record which group the write lands in, so the barrier below syncs
        // that group and not every dirty group on the node.
        let placements = BatchPlacementLog::shared();
        let outcome = durable_flush::scope(
            Arc::clone(&placements),
            blob_cas::put_blob_sealed_with_access_dedup_in_ops(
                self.db_ops(),
                plaintext.len() as u64,
                media_type.map(str::to_string),
                name.map(str::to_string),
                &access,
                &envelope,
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
