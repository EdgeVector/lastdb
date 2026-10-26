//! Local file-blob PUT: store one blob in the node's own `cas_blobs` plane.
//!
//! Nothing here touches the network or the sync engine. The cloud mode
//! ([`FoldDB::put_personal_file_blob`](super::FoldDB)) uploads first and then
//! writes a record; this path only makes the bytes durable and hands back the
//! `$lastdb_file` pointer. The caller writes the pointer into a record of its
//! choice, in a batch it controls (for example together with a commit marker).

use serde_json::Value;

use crate::error::FoldDbError;
use crate::sharing::blob_cas::{self, SealedPutOutcome};
use crate::sharing::delivery_wire::lastdb_file_pointer_with_access_and_thumbnail;

use super::FoldDB;

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
    /// - The native request commits the row and upload counter before returning.
    ///   The explicit store flush also remains before returning the pointer.
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
        let outcome = blob_cas::put_blob_sealed_with_access_dedup_in_ops(
            self.db_ops(),
            plaintext.len() as u64,
            media_type.map(str::to_string),
            name.map(str::to_string),
            &access,
            &envelope,
        )
        .await?;
        self.db_ops().flush().await?;
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
}
