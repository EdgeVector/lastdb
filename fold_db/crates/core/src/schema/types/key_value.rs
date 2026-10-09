use super::key_config::KeyConfig;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

/// Represents resolved key values for hash and range components.
///
/// `deny_unknown_fields` is load-bearing: serde's default drops unknown
/// keys silently, so a typo like `{"hash_key": "foo"}` would deserialize
/// into `KeyValue { hash: None, range: None }`. On a `Delete` mutation
/// that empty selector resolves to the empty-content hash, writing a
/// phantom tombstone while the intended record survives — the exact
/// silent-data-loss footgun tombstoning was meant to eliminate.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(deny_unknown_fields)]
pub struct KeyValue {
    pub hash: Option<String>,
    pub range: Option<String>,
}

impl KeyValue {
    pub fn new(hash: Option<String>, range: Option<String>) -> Self {
        Self { hash, range }
    }

    /// Creates a KeyValue from a mutation by extracting hash and range values
    /// based on the key configuration. Supports dotted nested paths (e.g., "departure.date").
    pub fn from_mutation(mutation_fields: &HashMap<String, Value>, key_config: &KeyConfig) -> Self {
        let mut key_value = Self::new(None, None);

        if let Some(hash_field) = &key_config.hash_field {
            key_value.hash = resolve_field_as_string(mutation_fields, hash_field);
        }

        if let Some(range_field) = &key_config.range_field {
            key_value.range = resolve_field_as_string(mutation_fields, range_field);
        }

        key_value
    }

    /// Lossless round-trippable string encoding for storage keys.
    ///
    /// `Display` cannot serve this purpose: it is content-addressable for
    /// `atom::input_snapshot::hash_input_snapshot` (pinned forever by
    /// `Provenance::Derived::encoding_version`), so the `"hash:range"`
    /// layout can't grow extra disambiguators. But that layout is lossy:
    /// `(Some("a"), Some("b"))`, `(None, Some("a:b"))`, and
    /// `(Some("a:b"), None)` all serialize to `"a:b"`, and any single-
    /// segment `Display` output is ambiguous between hash-only and
    /// range-only. Range values containing `:` (e.g. ISO datetimes like
    /// `"2025-03-15T14:30:00Z"`, URLs, Windows paths) hit this every time.
    ///
    /// Used by the view-override storage path
    /// (`view_orchestrator::redirect_view_write_to_override` writes,
    /// `view::resolver::apply_overrides_to_field_map` reads) so a stored
    /// override lands at the same `KeyValue` slot the source data uses,
    /// instead of silently appearing under a corrupted twin key.
    #[must_use]
    pub fn to_storage_key(&self) -> String {
        serde_json::to_string(self).expect("KeyValue is always JSON-serializable")
    }

    /// Inverse of [`Self::to_storage_key`].
    ///
    /// The empty string maps to `KeyValue::new(None, None)` — preserving
    /// the prior `parse_key_str` contract for the trivial empty-key case.
    /// Malformed input degrades to the empty `KeyValue` rather than
    /// panicking; the override apply path then no-ops the entry instead
    /// of inserting at a corrupted key.
    #[must_use]
    pub fn from_storage_key(s: &str) -> Self {
        if s.is_empty() {
            return Self::new(None, None);
        }
        serde_json::from_str(s).unwrap_or_else(|_| Self::new(None, None))
    }

    /// The canonical **page order** for keyed reads: ascending `range`, then
    /// ascending `hash` as tiebreaker. Absent components sort as `""`.
    ///
    /// This is the one order that pagination depends on, and three places have
    /// to agree on it or a page silently overlaps or drops rows: the `Page` /
    /// `PageAfter` slice in `apply_hash_range_filter`, the pre-hydrate window
    /// applied to a key-restricted read, and the node's final `format_rows`
    /// sort. They used to each spell it out; this is the single definition.
    ///
    /// `hash` stays ascending even under a descending `sort_order` — it is an
    /// opaque content hash, so reversing it is churn with no reader-visible
    /// meaning. Descending reads reverse `range` only, and the node maps a
    /// descending page back onto the matching ascending window before it ever
    /// reaches this comparison.
    ///
    /// Under order-preserving range encoding (OPE) the storage-form and
    /// API-form ranges sort identically, so it does not matter which form a
    /// call site holds — that equivalence is the whole point of OPE, and it is
    /// what lets the window be applied before keys are decoded back to
    /// plaintext.
    #[must_use]
    pub fn cmp_page_order(&self, other: &Self) -> std::cmp::Ordering {
        self.range
            .as_deref()
            .unwrap_or("")
            .cmp(other.range.as_deref().unwrap_or(""))
            .then_with(|| {
                self.hash
                    .as_deref()
                    .unwrap_or("")
                    .cmp(other.hash.as_deref().unwrap_or(""))
            })
    }
}

/// Resolve a field value as a string, supporting dotted nested paths (e.g., "departure.date").
fn resolve_field_as_string(fields: &HashMap<String, Value>, field_name: &str) -> Option<String> {
    // 1. Try direct field access
    if let Some(value) = fields.get(field_name) {
        return value_to_string(value);
    }
    // 2. Try dotted path (e.g., "parent.child")
    if let Some(dot) = field_name.find('.') {
        let (parent, child) = (&field_name[..dot], &field_name[dot + 1..]);
        if let Some(parent_val) = fields.get(parent) {
            if let Some(obj) = parent_val.as_object() {
                if let Some(child_val) = obj.get(child) {
                    return value_to_string(child_val);
                }
            }
        }
    }
    None
}

fn value_to_string(value: &Value) -> Option<String> {
    match value {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        Value::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

impl std::fmt::Display for KeyValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match (&self.hash, &self.range) {
            (Some(hash), Some(range)) => write!(f, "{hash}:{range}"),
            (Some(hash), None) => write!(f, "{hash}"),
            (None, Some(range)) => write!(f, "{range}"),
            (None, None) => Ok(()),
        }
    }
}
