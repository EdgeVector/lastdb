//! The key format marker of a new empty home.
//!
//! The atom counter makes atom bodies the only authority for atom references,
//! and the molecule counters and the blob counters do the same for molecules and
//! for local file blobs. A new home holds no reference of any kind, so no
//! completeness proof is stored and the format marker is the only key written.
//! The storage layer refuses earlier physical layouts. A native home also needs
//! the current logical identity marker before it can serve. A home from a build
//! that kept blob reference edges has the key format `3`: it has no blob
//! counters, so this build refuses it and asks for a new empty home.
//! Format `5` permits direct atom/CAS records with their existing inner seal.
//! The encrypted marker advances durably before the node can serve new writes,
//! so a format-4 reader refuses the home rather than hiding those records.

use super::AtomStore;
use crate::storage::{KvMutation, StorageError};

/// `5`: element-protected atom/CAS records need no outer namespace envelope.
/// `4` has the same counters and keys, with an additional outer value envelope.
pub(crate) const KEY_FORMAT: &[u8] = b"5";
const PREVIOUS_KEY_FORMAT: &[u8] = b"4";

pub(crate) fn is_supported_key_format(value: &[u8]) -> bool {
    value == KEY_FORMAT || value == PREVIOUS_KEY_FORMAT
}

/// The backend permits these exact keys together while current creation is
/// pending. No ordinary source record can enter that home before this boundary.
pub(crate) const CURRENT_NATIVE_INITIALIZATION_KEYS: &[&str] = &[AtomStore::CURRENT_KEY_FORMAT_KEY];

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
        if values.iter().any(|stored| {
            stored
                .as_deref()
                .is_none_or(|value| !is_supported_key_format(value))
        }) {
            return Err(StorageError::InvalidOperation(
                "current creation has an incomplete or different key-proof batch".into(),
            ));
        }
        Ok(false)
    }

    pub(crate) async fn initialize_key_format(&self) -> Result<(), StorageError> {
        self.main_store
            .inner()
            .batch_mutate(current_key_format_rows()?)
            .await
    }

    pub(crate) async fn require_current_key_format(&self) -> Result<(), StorageError> {
        let stored = self
            .main_store
            .inner()
            .get(Self::CURRENT_KEY_FORMAT_KEY.as_bytes())
            .await?;
        match stored.as_deref() {
            Some(value) if value == KEY_FORMAT => Ok(()),
            Some(value) if value == PREVIOUS_KEY_FORMAT => {
                // The normal boot owns a native request scope here. Its durable
                // commit finishes before construction returns or data writes run.
                self.initialize_key_format().await
            }
            _ => Err(StorageError::InvalidOperation(
                "this home does not use the current key format; create a new empty home".into(),
            )),
        }
    }
}

/// The rows of the key format marker, in the order of [`CURRENT_NATIVE_INITIALIZATION_KEYS`].
fn current_key_format_rows() -> Result<Vec<KvMutation>, StorageError> {
    Ok(vec![KvMutation::put(
        AtomStore::CURRENT_KEY_FORMAT_KEY.as_bytes(),
        KEY_FORMAT,
    )])
}
