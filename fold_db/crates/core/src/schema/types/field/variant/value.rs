use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::HashMap;

/// Resolved field value returned by query resolution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldValue {
    pub value: JsonValue,
    pub atom_uuid: String,
    pub source_file_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<HashMap<String, String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub molecule_uuid: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub molecule_version: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub writer_pubkey: Option<String>,
    /// Nanoseconds since the Unix epoch at which the underlying atom was
    /// first written. Populated from `Atom::created_at` during query
    /// resolution so downstream callers (view compute-as-mutations) can
    /// build canonical `MoleculeRef` leaves for the source Merkle tree.
    /// Additive — legacy persisted FieldValues deserialize with `None`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub written_at: Option<u64>,
}
