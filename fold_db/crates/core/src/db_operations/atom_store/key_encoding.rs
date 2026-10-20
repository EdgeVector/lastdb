//! Exact atom placement selection for a current-format home.

use super::AtomStore;
use crate::atom::{atom_key_codec::*, AtomKeyEncoding};
use crate::storage::StorageError;
use std::sync::atomic::Ordering;

impl AtomStore {
    /// Validate all explicit key settings before the native store creates files.
    pub(crate) fn validate_boot_key_encoding_settings() -> Result<(), StorageError> {
        validate_molecule_encoding_settings()?;
        requested_encoding().map(|_| ())
    }

    #[must_use]
    pub(crate) fn atom_key_encoding(&self) -> AtomKeyEncoding {
        if self.atom_keys_partition_prefixed.load(Ordering::Acquire) {
            AtomKeyEncoding::PartitionPrefix
        } else {
            AtomKeyEncoding::Flat
        }
    }

    /// Called only inside the constructor-proven empty-home transaction.
    pub(crate) async fn initialize_fresh_atom_layout(&self) -> Result<(), StorageError> {
        Self::validate_boot_key_encoding_settings()?;
        let encoding = requested_encoding()?.unwrap_or_default();
        self.main_store
            .put_item(
                ATOM_KEY_ENCODING_MARKER_KEY,
                &AtomKeyEncodingMarker {
                    version: 2,
                    encoding: encoding.as_marker_str().to_owned(),
                    stamped_at_unix: crate::clock::unix_secs(),
                },
            )
            .await
    }

    pub(crate) async fn resolve_boot_encoding(self) -> Result<Self, StorageError> {
        self.validate_restored_atom_layout().await?;
        self.hydrate_automatic_gc_atoms_generation().await?;
        Ok(self)
    }

    /// A restore must carry the exact current layout marker. No conversion runs.
    pub(crate) async fn validate_restored_atom_layout(&self) -> Result<(), StorageError> {
        self.require_current_key_format().await?;
        validate_molecule_encoding_settings()?;
        let marker = self
            .main_store
            .get_item::<AtomKeyEncodingMarker>(ATOM_KEY_ENCODING_MARKER_KEY)
            .await?
            .ok_or_else(|| configuration_error("missing current atom layout marker"))?;
        if marker.version != 2 {
            return Err(configuration_error(
                "unsupported atom layout marker version",
            ));
        }
        let encoding = AtomKeyEncoding::from_marker_str(&marker.encoding)
            .filter(|encoding| encoding.as_marker_str() == marker.encoding)
            .ok_or_else(|| configuration_error("unsupported atom layout encoding"))?;
        if requested_encoding()?.is_some_and(|requested| requested != encoding) {
            return Err(configuration_error(
                "atom placement differs from the home; format conversion is not supported",
            ));
        }
        self.atom_keys_partition_prefixed
            .store(encoding.writes_partition_prefix(), Ordering::Release);
        Ok(())
    }
}

fn requested_encoding() -> Result<Option<AtomKeyEncoding>, StorageError> {
    match std::env::var(ATOM_KEY_ENCODING_ENV) {
        Ok(value) => AtomKeyEncoding::from_marker_str(&value)
            .map(Some)
            .ok_or_else(|| configuration_error("unsupported LASTDB_ATOM_KEY_ENCODING")),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(_) => Err(configuration_error("invalid LASTDB_ATOM_KEY_ENCODING")),
    }
}

fn validate_molecule_encoding_settings() -> Result<(), StorageError> {
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
            _ => return Err(configuration_error(&format!("unsupported {name}"))),
        }
    }
    Ok(())
}

fn configuration_error(message: &str) -> StorageError {
    StorageError::ConfigurationError(message.to_owned())
}
