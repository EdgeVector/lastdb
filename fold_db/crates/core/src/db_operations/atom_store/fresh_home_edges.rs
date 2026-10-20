//! Key format marker and completeness proofs for a constructor-proven empty home.
//!
//! The atom counter makes atom bodies the only authority for atom references.
//! A new home holds no blob or molecule reference, so the proofs that both
//! reference planes are complete are true by construction. An older home has no
//! feature bit for atom records and the storage layer refuses it before this
//! code runs.

use super::{
    AtomStore, BlobRefCompleteness, MoleculeRefCompleteness, BLOB_REF_COMPLETE_KEY,
    MOLECULE_REF_COMPLETE_KEY,
};
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
                KvMutation::put(MOLECULE_REF_COMPLETE_KEY.as_bytes(), molecule),
                KvMutation::put(BLOB_REF_COMPLETE_KEY.as_bytes(), blob),
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
}
