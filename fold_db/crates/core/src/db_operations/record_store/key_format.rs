//! The key format marker of a node home.
//!
//! The record counter makes record bodies the only authority for record references,
//! and the field key counters and the blob counters do the same for field keys and
//! for local file blobs. A new home holds no reference of any kind, so no
//! reference-completeness proof is needed. The format and account-key proofs
//! authorize the new home.
//! The storage layer refuses earlier physical layouts. A native home also needs
//! the logical identity marker `typed-ranges` before it can serve. Home
//! creation writes it. A record row is plain JSON, and the block encryption of
//! each run file protects it at rest. Tips and control values have no outer
//! envelope. Startup never rewrites a home.

use super::RecordStore;
use crate::storage::StorageError;

impl RecordStore {
    /// The marker key lives in `crate::records::key_format`; the restore proof
    /// and run admission read it without the store.
    pub(crate) const CURRENT_KEY_FORMAT_KEY: &str =
        crate::records::key_format::CURRENT_KEY_FORMAT_KEY;

    pub(crate) async fn require_current_key_format(&self) -> Result<(), StorageError> {
        let stored = self
            .main_store
            .inner()
            .get(Self::CURRENT_KEY_FORMAT_KEY.as_bytes())
            .await?;
        if !stored
            .as_deref()
            .is_some_and(lastdb_model::field_key_codec::current_key_format)
        {
            return Err(StorageError::InvalidOperation(
                "the logical key format does not match this node home".into(),
            ));
        }
        self.minimum_replay_time().await.map(|_| ())
    }
}
