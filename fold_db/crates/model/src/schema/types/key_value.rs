use serde::{Deserialize, Serialize};

/// Represents resolved key values for hash and range components.
///
/// Unknown coordinates are refused instead of disappearing into an empty key.
/// The logical mutation boundary also refuses missing required coordinates.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, Default)]
#[serde(deny_unknown_fields)]
pub struct KeyValue {
    pub hash: Option<String>,
    pub range: Option<String>,
}

impl KeyValue {
    pub fn new(hash: Option<String>, range: Option<String>) -> Self {
        Self { hash, range }
    }

    /// Lossless round-trippable string encoding for storage keys.
    ///
    /// `Display` cannot serve this purpose: its `"hash:range"` layout is lossy:
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
    /// `PageAfter` slice of a record range read, the pre-hydrate window
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
