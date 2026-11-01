//! Schema record key report types.

use super::*;

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
