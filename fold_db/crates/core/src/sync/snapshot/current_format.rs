//! Validate current photographs without key rewriting or source conversion.

use super::*;
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};

pub(super) fn validate_photograph(
    namespaces: Vec<NamespaceData>,
) -> SyncResult<Vec<NamespaceData>> {
    let mut names = HashSet::with_capacity(namespaces.len());
    for namespace in &namespaces {
        if !names.insert(namespace.name.as_str()) {
            return Err(SyncError::Serialization(format!(
                "snapshot contains duplicate namespace '{}'",
                namespace.name
            )));
        }
        let mut keys = HashSet::with_capacity(namespace.entries.len());
        for entry in &namespace.entries {
            let key = BASE64.decode(&entry.key).map_err(|error| {
                SyncError::Serialization(format!("invalid key base64: {error}"))
            })?;
            crate::storage::current_keys::validate(&namespace.name, &key)?;
            if !keys.insert(key) {
                return Err(SyncError::Serialization(format!(
                    "snapshot namespace '{}' contains a duplicate key",
                    namespace.name
                )));
            }
        }
    }
    Ok(namespaces)
}

impl Snapshot {
    pub(super) fn validate_full_format_markers(&self) -> SyncResult<()> {
        let namespace = self
            .namespaces
            .iter()
            .find(|namespace| namespace.name == "main")
            .ok_or_else(|| {
                SyncError::Serialization("backup lacks the current main namespace".to_string())
            })?;
        let required = [
            "layout:v2:keys",
            crate::atom::atom_key_codec::ATOM_KEY_ENCODING_MARKER_KEY,
        ];
        let mut found = [false; 2];
        for entry in &namespace.entries {
            let key = BASE64.decode(&entry.key).map_err(|error| {
                SyncError::Serialization(format!("invalid key base64: {error}"))
            })?;
            for (i, required) in required.iter().enumerate() {
                if key == required.as_bytes() {
                    found[i] = true;
                }
            }
        }
        if found.iter().any(|found| !found) {
            return Err(SyncError::Serialization(
                "backup lacks the current key format markers".to_string(),
            ));
        }
        Ok(())
    }
}
