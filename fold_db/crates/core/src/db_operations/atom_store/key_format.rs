//! The key format marker of a node home.
//!
//! The atom counter makes atom bodies the only authority for atom references,
//! and the molecule counters and the blob counters do the same for molecules and
//! for local file blobs. A new home holds no reference of any kind, so no
//! reference-completeness proof is needed. The format and account-key proofs
//! authorize the new home.
//! The storage layer refuses earlier physical layouts. A native home also needs
//! the logical identity marker `7;typed-ranges=v1` before it can serve. Home
//! creation writes it. An atom row is plain JSON, and the block encryption of
//! each run file protects it at rest. Tips and control values have no outer
//! envelope. Startup never rewrites a home.

use super::AtomStore;
use crate::storage::StorageError;

pub(crate) use super::delete_replay::CUTOFF_KEY as DELETE_REPLAY_CUTOFF_KEY;

impl AtomStore {
    /// This logical format includes framed molecule identities and exact
    /// tuple-based binding sources. Snapshots require the same marker key.
    pub(crate) const CURRENT_KEY_FORMAT_KEY: &str = "layout:v3:keys";

    pub(crate) async fn require_current_key_format(&self) -> Result<(), StorageError> {
        let stored = self
            .main_store
            .inner()
            .get(Self::CURRENT_KEY_FORMAT_KEY.as_bytes())
            .await?;
        if stored.as_deref() != Some(crate::atom::molecule_key_codec::TYPED_KEY_FORMAT) {
            return Err(StorageError::InvalidOperation(
                "the logical key format does not match this node home".into(),
            ));
        }
        self.minimum_replay_time().await.map(|_| ())
    }
}
