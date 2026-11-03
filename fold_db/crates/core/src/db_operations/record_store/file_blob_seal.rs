//! A narrow file-encryption operation that keeps the account root private.

use super::RecordStore;
use crate::error::FoldDbError;
use crate::sharing::file_blob_seal::{account_file_blob_dek_hex, seal_file_blob, FileBlobRef};

impl RecordStore {
    fn account_key(&self) -> Result<&[u8; 32], FoldDbError> {
        self.content_key.as_ref().ok_or_else(|| {
            FoldDbError::SecurityError("local file encryption requires the account key".into())
        })
    }

    pub(crate) fn seal_local_file_blob(
        &self,
        plaintext: &[u8],
    ) -> Result<(FileBlobRef, Vec<u8>), FoldDbError> {
        seal_file_blob(plaintext, self.account_key()?).map_err(FoldDbError::from)
    }

    /// The file key of a record file, derived again from the plaintext hash of
    /// its marker. The key is never stored.
    pub(crate) fn local_file_dek_hex(&self, file_hash: &str) -> Result<String, FoldDbError> {
        account_file_blob_dek_hex(self.account_key()?, file_hash).map_err(FoldDbError::from)
    }
}
