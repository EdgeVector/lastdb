//! Key format marker and completeness proof for a constructor-proven empty home.
//!
//! The atom counter makes atom bodies the only authority for atom references,
//! and the molecule counters and the blob counters do the same for molecules and
//! for local file blobs. A new home holds no reference of any kind, so no
//! completeness proof is stored and the format marker is the only key written.
//! An older home has no feature bit for atom records and the storage layer
//! refuses it before this code runs. A home from a build that kept blob
//! reference edges has the key format `2`: it has no blob counters, so this
//! build refuses it and asks for a new empty home.

use super::AtomStore;
use crate::storage::{KvMutation, StorageError};

const KEY_FORMAT_KEY: &str = "layout:v2:keys";
/// `3`: local file blobs have counters (`bref:v3:`). Format `2` kept blob edges.
const KEY_FORMAT: &[u8] = b"3";

/// The backend permits these exact keys together while current creation is
/// pending. No ordinary source record can enter that home before this boundary.
pub(crate) const CURRENT_NATIVE_INITIALIZATION_KEYS: &[&str] = &[KEY_FORMAT_KEY];

impl AtomStore {
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

/// The proof rows in the order of [`CURRENT_NATIVE_INITIALIZATION_KEYS`].
fn current_reference_proofs() -> Result<Vec<KvMutation>, StorageError> {
    Ok(vec![KvMutation::put(KEY_FORMAT_KEY.as_bytes(), KEY_FORMAT)])
}
