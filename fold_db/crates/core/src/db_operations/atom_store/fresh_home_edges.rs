//! One complete current-format boundary for a constructor-proven empty home.

use super::{
    AtomStore, BlobRefCompleteness, MoleculeRefCompleteness, BLOB_REF_COMPLETE_KEY,
    MOLECULE_REF_COMPLETE_KEY, PEER_APPLY_KEY,
};
use crate::atom::molecule_key_codec::{ATOM_REF_V2_COMPLETE_KEY, ATOM_REF_V2_HISTORY_COMPLETE_KEY};
use crate::storage::{KvMutation, StorageError};

const KEY_FORMAT_KEY: &str = "layout:v2:keys";
const KEY_FORMAT: &[u8] = b"2";

/// The backend permits these exact keys together while current creation is
/// pending. No ordinary source record can enter that home before this boundary.
pub(crate) const CURRENT_NATIVE_INITIALIZATION_KEYS: &[&str] = &[
    crate::atom::atom_key_codec::ATOM_KEY_ENCODING_MARKER_KEY,
    KEY_FORMAT_KEY,
    ATOM_REF_V2_COMPLETE_KEY,
    ATOM_REF_V2_HISTORY_COMPLETE_KEY,
    MOLECULE_REF_COMPLETE_KEY,
    BLOB_REF_COMPLETE_KEY,
    PEER_APPLY_KEY,
];

impl AtomStore {
    /// A crash can leave either no proofs or the complete durable proof batch.
    /// Validate the latter through the encryption seam and preserve its selected
    /// layout. A partial boundary is refused rather than inferred from data.
    pub(crate) async fn fresh_native_initialization_needed(&self) -> Result<bool, StorageError> {
        let keys = CURRENT_NATIVE_INITIALIZATION_KEYS
            .iter()
            .map(|key| key.as_bytes().to_vec())
            .collect();
        let values = self.main_store.inner().get_many(keys).await?;
        if values.len() != CURRENT_NATIVE_INITIALIZATION_KEYS.len() {
            return Err(StorageError::BackendError(
                "current creation received an incomplete key-proof read".into(),
            ));
        }
        if values.iter().all(Option::is_none) {
            return Ok(true);
        }
        let proofs = current_reference_proofs()?;
        if values[0].is_none()
            || proofs.iter().zip(&values[1..]).any(|(proof, stored)| {
                let KvMutation::Put { value, .. } = proof else {
                    unreachable!("initialization proofs only contain puts");
                };
                stored.as_ref() != Some(value)
            })
        {
            return Err(StorageError::InvalidOperation(
                "current creation has an incomplete or different key-proof batch".into(),
            ));
        }
        self.validate_restored_atom_layout().await?;
        Ok(false)
    }

    pub(crate) async fn initialize_current_reference_proofs(&self) -> Result<(), StorageError> {
        self.main_store
            .inner()
            .batch_mutate(current_reference_proofs()?)
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

fn current_reference_proofs() -> Result<Vec<KvMutation>, StorageError> {
    let blob = serde_json::to_vec(&BlobRefCompleteness { version: 2 })
        .map_err(|error| StorageError::SerializationError(error.to_string()))?;
    let molecule = serde_json::to_vec(&MoleculeRefCompleteness {
        version: 2,
        schema_fields: 0,
        protein_members: 0,
    })
    .map_err(|error| StorageError::SerializationError(error.to_string()))?;
    Ok(vec![
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
}
