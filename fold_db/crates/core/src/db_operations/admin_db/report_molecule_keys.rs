//! Molecule-key, hash-bucket, schema-record-key and legacy key-fork report types.

use super::*;

/// One storage row under a molecule's `mk:{M}:` prefix.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MoleculeKeyRow {
    /// Full storage key as stored (may include a storage_prefix).
    pub storage_key: String,
    /// Decoded hash segment when the unified HashRange encoding parses.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hash: Option<String>,
    /// Decoded range segment (empty string for hash-only slots).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub range: Option<String>,
    /// True when another row in the same molecule decodes to the same
    /// `(hash, range)` — encoding duality residue (H2).
    pub collision: bool,
}

/// Read-only listing of one molecule's live `mk:` keys.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct MoleculeKeysReport {
    pub molecule: String,
    /// Raw storage-row count returned (may be capped by `max_keys`).
    pub keys: u64,
    /// Distinct decoded logical keys (+ each undecodable row as its own).
    pub unique_keys: u64,
    /// Rows that participate in a multi-row logical identity.
    pub collision_rows: u64,
    pub more_remaining: bool,
    pub rows: Vec<MoleculeKeyRow>,
}

/// One raw `mk:` row from a single API HashKey partition.
///
/// This report exists for isolated development-node diagnostics. The caller
/// supplies the API hash, and the node maps it through the molecule's active
/// blind-index key before it reads the bounded storage prefix.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MoleculeHashBucketRow {
    /// Full `mk:` storage key, including its blinded hash and typed range payload.
    pub storage_key: String,
    /// Blinded storage hash decoded from `storage_key`.
    pub storage_hash: String,
    /// Typed range payload decoded from `storage_key`.
    pub storage_range: String,
    /// API range decoded from the typed range payload.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_range: Option<String>,
    /// Raw current-tip value stored in the `mk:` row.
    pub tip: crate::atom::AtomEntry,
    /// Raw per-key metadata stored beside the tip.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<crate::atom::KeyMetadata>,
    /// Opened atom content. This lets a developer correlate sibling field
    /// indexes without exporting node key material.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub atom_content: Option<Value>,
}

/// Read-only raw `mk:` dump for one molecule and one API HashKey.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MoleculeHashBucketReport {
    pub molecule: String,
    pub api_hash: String,
    /// Exact raw prefixes that the node derived with its in-process keys.
    pub storage_prefixes: Vec<String>,
    pub rows: u64,
    pub more_remaining: bool,
    pub entries: Vec<MoleculeHashBucketRow>,
}

/// One live record identity from a schema's key-field molecule.
///
/// Product `lastdb list` returns these — never atom bodies. `range` is empty
/// for HashKey / hash-only slots.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SchemaRecordKey {
    pub hash: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub range: String,
}

/// Paged API keys for one schema's current records.
/// Schema-facing reads check the selected field bodies for membership and hash
/// recovery. Physical-key cursors retain progress across rejected candidates.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaRecordKeysReport {
    /// Caller-facing name (descriptive / `app/Name` when that is what was asked).
    pub schema: String,
    /// Canonical schema identity the walk used.
    pub schema_id: String,
    pub key_field: String,
    /// Hash half of the schema key, when declared.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hash_field: Option<String>,
    /// Range half of the schema key, when declared.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub range_field: Option<String>,
    /// API hash this page was restricted to, when the caller passed `hash=`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hash_filter: Option<String>,
    pub molecule: String,
    pub keys: Vec<SchemaRecordKey>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
    pub has_more: bool,
    /// Same as `has_more` — honesty flag so a page is never mistaken for a census.
    pub truncated: bool,
}

/// Exact schema identity and page controls for a schema-facing key list.
#[derive(Debug, Clone, Copy)]
pub struct SchemaRecordKeysRequest<'a> {
    pub requested: &'a str,
    pub schema_id: &'a str,
    pub limit: usize,
    pub cursor: Option<&'a str>,
    pub hash_filter: Option<&'a str>,
}
