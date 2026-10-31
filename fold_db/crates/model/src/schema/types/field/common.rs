use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldCommon {
    pub molecule_uuid: Option<String>,
    #[serde(default = "default_writable")]
    pub writable: bool,
    /// Org hash inherited from the parent schema.
    /// When set, all storage keys for this field's data are prefixed with `{storage_prefix}:`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_prefix: Option<String>,
}

fn default_writable() -> bool {
    true
}

impl Default for FieldCommon {
    fn default() -> Self {
        Self {
            molecule_uuid: None,
            writable: true,
            storage_prefix: None,
        }
    }
}

impl FieldCommon {
    // Convenience methods to avoid repetition
    pub fn molecule_uuid(&self) -> Option<&String> {
        self.molecule_uuid.as_ref()
    }

    pub fn set_molecule_uuid(&mut self, uuid: String) {
        self.molecule_uuid = Some(uuid);
    }

    pub fn writable(&self) -> bool {
        self.writable
    }

    /// Build a storage key, prepending the storage-scope prefix when present.
    ///
    /// - Personal: `base_key` (e.g. `atom:{uuid}`, `mk:…`)
    /// - Scoped: `{prefix}:{base_key}` — share receive namespaces use
    ///   `from:{sender_hash}`; historical org hashes used the same mechanism
    pub fn storage_key(&self, base_key: &str) -> String {
        build_storage_key(self.storage_prefix.as_deref(), base_key)
    }

    pub fn storage_prefix(&self) -> Option<&str> {
        self.storage_prefix.as_deref()
    }

    pub fn set_storage_prefix(&mut self, storage_prefix: Option<String>) {
        self.storage_prefix = storage_prefix;
    }
}

/// Build a storage key, prepending an optional storage-scope prefix.
///
/// Used for personal (no prefix), share receive (`from:{sender}`), and
/// residual historical org-prefixed keys. Prefix is an isolation boundary —
/// readers must not dual-read bare keys when a prefix is set.
pub fn build_storage_key(storage_prefix: Option<&str>, base_key: &str) -> String {
    match storage_prefix {
        Some(hash) => format!("{hash}:{base_key}"),
        None => base_key.to_string(),
    }
}
