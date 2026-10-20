//! Key format marker and completeness proofs for a constructor-proven empty home.
//!
//! The atom counter makes atom bodies the only authority for atom references,
//! and the molecule counters do the same for molecules. A new home holds no blob
//! reference, so the proof that the blob reference plane is complete is true by
//! construction. The storage layer refuses earlier physical layouts. A native
//! home also needs the current logical identity marker before it can serve.

use super::{AtomStore, BlobRefCompleteness, BLOB_REF_COMPLETE_KEY};
use crate::storage::{KvMutation, StorageError};

const KEY_FORMAT: &[u8] = b"3";

/// The backend permits these exact keys together while current creation is
/// pending. No ordinary source record can enter that home before this boundary.
pub(crate) const CURRENT_NATIVE_INITIALIZATION_KEYS: &[&str] =
    &[AtomStore::CURRENT_KEY_FORMAT_KEY, BLOB_REF_COMPLETE_KEY];

impl AtomStore {
    /// This logical format includes framed molecule identities and exact
    /// tuple-based binding sources. Snapshots require the same marker key.
    pub(crate) const CURRENT_KEY_FORMAT_KEY: &str = "layout:v3:keys";

    /// Validate the explicit key settings before the native store creates files.
    pub(crate) fn validate_boot_key_encoding_settings() -> Result<(), StorageError> {
        for (name, choices) in [
            ("LASTDB_HASH_KEY_ENCODING", ["plain", "blind_v1"]),
            ("LASTDB_RANGE_KEY_ENCODING", ["plain", "ope_v1"]),
        ] {
            match std::env::var(name) {
                Ok(value)
                    if choices
                        .iter()
                        .any(|choice| value.eq_ignore_ascii_case(choice)) => {}
                Err(std::env::VarError::NotPresent) => {}
                _ => {
                    return Err(StorageError::ConfigurationError(format!(
                        "unsupported {name}"
                    )))
                }
            }
        }
        Ok(())
    }

    /// A crash can leave either no proofs or the complete durable proof batch.
    /// Validate the latter through the encryption seam. A partial boundary is
    /// refused rather than inferred from data.
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
        if proofs.iter().zip(&values).any(|(proof, stored)| {
            let KvMutation::Put { value, .. } = proof else {
                unreachable!("initialization proofs only contain puts");
            };
            stored.as_ref() != Some(value)
        }) {
            return Err(StorageError::InvalidOperation(
                "current creation has an incomplete or different key-proof batch".into(),
            ));
        }
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
            .get(Self::CURRENT_KEY_FORMAT_KEY.as_bytes())
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

/// The proof rows in the order of [`CURRENT_NATIVE_INITIALIZATION_KEYS`].
fn current_reference_proofs() -> Result<Vec<KvMutation>, StorageError> {
    let blob = serde_json::to_vec(&BlobRefCompleteness { version: 2 })
        .map_err(|error| StorageError::SerializationError(error.to_string()))?;
    Ok(vec![
        KvMutation::put(AtomStore::CURRENT_KEY_FORMAT_KEY.as_bytes(), KEY_FORMAT),
        KvMutation::put(BLOB_REF_COMPLETE_KEY.as_bytes(), blob),
    ])
}
