use super::key_config::KeyConfig;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::borrow::Cow;
use std::collections::HashMap;

/// Represents resolved key values for hash and range components.
///
/// Unknown coordinates are refused instead of disappearing into an empty key.
/// The logical mutation boundary also refuses missing required coordinates.
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
    /// Aggregate guards and portable query slices use this representation to
    /// retain the original lookup coordinates without separator ambiguity.
    #[must_use]
    pub fn to_storage_key(&self) -> String {
        serde_json::to_string(self).expect("KeyValue is always JSON-serializable")
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
    /// These are API coordinates under the captured schema comparison.
    /// Raw stored payloads require `cmp_storage_order`; typed API text and
    /// ordered disk payloads use different representations.
    ///
    /// The range comparison is an explicit argument: the model holds no
    /// ambient context. Core's `KeyValuePageOrder::cmp_page_order` passes the
    /// comparison of the key comparison context that the read installed.
    #[must_use]
    pub fn cmp_page_order_with(
        &self,
        other: &Self,
        compare_ranges: impl FnOnce(&str, &str) -> std::cmp::Ordering,
    ) -> std::cmp::Ordering {
        compare_ranges(
            self.range.as_deref().unwrap_or(""),
            other.range.as_deref().unwrap_or(""),
        )
        .then_with(|| {
            self.hash
                .as_deref()
                .unwrap_or("")
                .cmp(other.hash.as_deref().unwrap_or(""))
        })
    }

    /// Compare raw storage coordinates without a schema's API comparison context.
    #[must_use]
    pub fn cmp_storage_order(&self, other: &Self) -> std::cmp::Ordering {
        self.cmp_page_order_with(other, str::cmp)
    }

    /// Match a scalar field value against this key's original API hash.
    #[must_use]
    pub fn hash_matches_scalar(&self, value: &Value) -> bool {
        self.hash
            .as_deref()
            .is_some_and(|hash| scalar_key_coordinate(value).as_deref() == Some(hash))
    }
}

/// Resolve a field value as a string, supporting dotted nested paths (e.g., "departure.date").
fn resolve_field_as_string(fields: &HashMap<String, Value>, field_name: &str) -> Option<String> {
    // 1. Try direct field access
    if let Some(value) = fields.get(field_name) {
        return scalar_key_coordinate(value).map(Cow::into_owned);
    }
    // 2. Try dotted path (e.g., "parent.child")
    if let Some(dot) = field_name.find('.') {
        let (parent, child) = (&field_name[..dot], &field_name[dot + 1..]);
        if let Some(parent_val) = fields.get(parent) {
            if let Some(obj) = parent_val.as_object() {
                if let Some(child_val) = obj.get(child) {
                    return scalar_key_coordinate(child_val).map(Cow::into_owned);
                }
            }
        }
    }
    None
}

/// The same scalar representation supplies lookup coordinates and comparisons.
pub fn scalar_key_coordinate(value: &Value) -> Option<Cow<'_, str>> {
    match value {
        Value::String(s) => Some(Cow::Borrowed(s)),
        Value::Number(n) => Some(Cow::Owned(n.to_string())),
        Value::Bool(b) => Some(Cow::Owned(b.to_string())),
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
