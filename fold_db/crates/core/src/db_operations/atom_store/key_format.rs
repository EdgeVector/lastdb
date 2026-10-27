//! The key format marker of a new empty home.
//!
//! The atom counter makes atom bodies the only authority for atom references,
//! and the molecule counters and the blob counters do the same for molecules and
//! for local file blobs. A new home holds no reference of any kind, so no
//! reference-completeness proof is needed. The format and account-key proofs
//! authorize the new home.
//! The storage layer refuses earlier physical layouts. A native home also needs
//! the current logical identity marker before it can serve. A home from a build
//! that kept blob reference edges has the key format `3`: it has no blob
//! counters, so this build refuses it and asks for a new empty home.
//! Format `7;typed-ranges=v1` selects typed ranges for new native homes.
//! Legacy format `6` wraps only local CAS records. An atom row is plain JSON, and the
//! block encryption of each run file protects it at rest. Tips and control
//! values have no outer envelope. Startup never rewrites an earlier home.

use super::AtomStore;
use crate::storage::{KvMutation, StorageError};

/// `6`: a storage envelope exists only on local CAS records.
pub(crate) const KEY_FORMAT: &[u8] = b"6";
pub(crate) use super::delete_replay::CUTOFF_KEY as DELETE_REPLAY_CUTOFF_KEY;

pub(crate) fn is_supported_key_format(value: &[u8]) -> bool {
    value == KEY_FORMAT || value == super::delete_replay::FENCED_KEY_FORMAT
}

/// Legacy creation permits these exact keys together in its first proof batch.
/// Typed creation commits its format and account proof before its node marker.
/// Neither path permits ordinary source records before logical admission.
pub(crate) const CURRENT_NATIVE_INITIALIZATION_KEYS: &[&str] = &[AtomStore::CURRENT_KEY_FORMAT_KEY];

impl AtomStore {
    fn supports_home_key_format(&self, value: &[u8]) -> bool {
        if self.typed_ranges {
            value == crate::atom::molecule_key_codec::TYPED_KEY_FORMAT
        } else {
            is_supported_key_format(value)
        }
    }

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
    /// Validate the exact stored format bytes. A partial boundary is
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
                .is_none_or(|value| !self.supports_home_key_format(value))
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
            .batch_mutate(current_key_format_rows(self.typed_ranges)?)
            .await
    }

    pub(crate) async fn require_current_key_format(&self) -> Result<(), StorageError> {
        let stored = self
            .main_store
            .inner()
            .get(Self::CURRENT_KEY_FORMAT_KEY.as_bytes())
            .await?;
        if stored
            .as_deref()
            .is_none_or(|value| !self.supports_home_key_format(value))
        {
            return Err(StorageError::InvalidOperation(
                "the logical key format does not match this node home; convert it offline".into(),
            ));
        }
        self.minimum_replay_time().await.map(|_| ())
    }
}

/// The rows of the key format marker, in the order of [`CURRENT_NATIVE_INITIALIZATION_KEYS`].
fn current_key_format_rows(typed: bool) -> Result<Vec<KvMutation>, StorageError> {
    Ok(vec![KvMutation::put(
        AtomStore::CURRENT_KEY_FORMAT_KEY.as_bytes(),
        if typed {
            crate::atom::molecule_key_codec::TYPED_KEY_FORMAT
        } else {
            KEY_FORMAT
        },
    )])
}
