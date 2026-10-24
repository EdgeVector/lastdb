//! Validate current photographs without key rewriting or source conversion.

use super::*;
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};

pub(super) fn validate_photograph(
    namespaces: Vec<NamespaceData>,
) -> SyncResult<Vec<NamespaceData>> {
    let mut names = HashSet::with_capacity(namespaces.len());
    for namespace in &namespaces {
        refuse_counter_namespace(&namespace.name)?;
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
            refuse_pending_marker(&namespace.name, &key)?;
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

pub(super) fn refuse_counter_namespace(namespace: &str) -> SyncResult<()> {
    if crate::storage::current_keys::counter_namespace_alias(namespace) {
        return Err(SyncError::Serialization(
            "backup uses a physical counter namespace alias; use a current native backup".into(),
        ));
    }
    Ok(())
}

pub(super) fn refuse_pending_marker(namespace: &str, key: &[u8]) -> SyncResult<()> {
    if namespace == "main"
        && key == crate::storage::disk_pack::restore_safety::PENDING_KEY.as_bytes()
    {
        return Err(SyncError::Serialization(
            "backup contains an incomplete counter restore".into(),
        ));
    }
    Ok(())
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
        let required = crate::db_operations::AtomStore::CURRENT_KEY_FORMAT_KEY;
        let mut found = false;
        for entry in &namespace.entries {
            let key = BASE64.decode(&entry.key).map_err(|error| {
                SyncError::Serialization(format!("invalid key base64: {error}"))
            })?;
            if key == required.as_bytes() {
                found = true;
                break;
            }
        }
        if !found {
            return Err(SyncError::Serialization(
                "backup lacks the current key format marker".to_string(),
            ));
        }
        Ok(())
    }
}
