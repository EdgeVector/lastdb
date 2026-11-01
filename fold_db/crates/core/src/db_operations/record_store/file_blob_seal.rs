//! A narrow file-encryption operation that keeps the account root private.

use super::RecordStore;
use crate::error::FoldDbError;
use crate::sharing::file_blob_seal::{seal_file_blob, FileBlobRef};

impl RecordStore {
    pub(crate) fn seal_local_file_blob(
        &self,
        plaintext: &[u8],
    ) -> Result<(FileBlobRef, Vec<u8>), FoldDbError> {
        let account_key = self.content_key.as_ref().ok_or_else(|| {
            FoldDbError::SecurityError("local file encryption requires the account key".into())
        })?;
        seal_file_blob(plaintext, account_key).map_err(FoldDbError::from)
    }
}
