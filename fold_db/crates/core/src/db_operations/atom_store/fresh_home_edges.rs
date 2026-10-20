//! One complete current-format boundary for a constructor-proven empty home.

use super::{
    AtomStore, BlobRefCompleteness, MoleculeRefCompleteness, BLOB_REF_COMPLETE_KEY,
    MOLECULE_REF_COMPLETE_KEY, PEER_APPLY_KEY,
};
use crate::atom::molecule_key_codec::{ATOM_REF_V2_COMPLETE_KEY, ATOM_REF_V2_HISTORY_COMPLETE_KEY};
use crate::storage::{KvMutation, StorageError};

const KEY_FORMAT_KEY: &str = "layout:v2:keys";
const KEY_FORMAT: &[u8] = b"2";

impl AtomStore {
    pub(crate) async fn initialize_current_reference_proofs(&self) -> Result<(), StorageError> {
        let blob = serde_json::to_vec(&BlobRefCompleteness { version: 2 })
            .map_err(|error| StorageError::SerializationError(error.to_string()))?;
        let molecule = serde_json::to_vec(&MoleculeRefCompleteness {
            version: 2,
            schema_fields: 0,
            protein_members: 0,
        })
        .map_err(|error| StorageError::SerializationError(error.to_string()))?;
        self.main_store
            .inner()
            .batch_mutate(vec![
                KvMutation::put(KEY_FORMAT_KEY.as_bytes(), KEY_FORMAT),
                KvMutation::put(
                    ATOM_REF_V2_COMPLETE_KEY.as_bytes(),
                    super::atom_ref_edges::ATOM_REF_V2_COMPLETE_MARKER,
                ),
                KvMutation::put(
                    ATOM_REF_V2_HISTORY_COMPLETE_KEY.as_bytes(),
                    super::atom_ref_edges::ATOM_REF_V2_COMPLETE_MARKER,
                ),
                KvMutation::put(MOLECULE_REF_COMPLETE_KEY.as_bytes(), molecule),
                KvMutation::put(BLOB_REF_COMPLETE_KEY.as_bytes(), blob),
                KvMutation::put(PEER_APPLY_KEY.as_bytes(), b"never"),
            ])
            .await
    }

    pub(crate) async fn require_current_key_format(&self) -> Result<(), StorageError> {
        if self
            .main_store
            .inner()
            .get(KEY_FORMAT_KEY.as_bytes())
            .await?
            .as_deref()
            != Some(KEY_FORMAT)
        {
            return Err(StorageError::InvalidOperation(
                "this home does not use the current key format; create a new empty home".into(),
            ));
        }
        Ok(())
    }

    pub(crate) fn invalidate_atom_ref_readiness_after_restore(&self) -> Result<(), StorageError> {
        *self.atom_ref_v2_read_ready.lock().map_err(|error| {
            StorageError::BackendError(format!("lock atom reference readiness: {error}"))
        })? = false;
        Ok(())
    }
}
