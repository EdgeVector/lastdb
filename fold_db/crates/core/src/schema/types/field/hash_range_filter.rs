use crate::schema::types::key_value::KeyValue;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

/// Hard cap on how many records a [`HashRangeFilter::SampleN`] peek ever
/// returns. `SampleN` is a "see what the data looks like" primitive, not a
/// fetch — every apply site clamps the requested `n` to this, so it can never
/// be abused as a bulk loader (the `SampleN(10_000)` that ballooned the :9001
/// brain to 13 GB). Real fetches use key/range filters or
/// [`HashRangeFilter::Page`].
pub const SAMPLE_PEEK_CAP: usize = 10;

/// HashRange filter operations for querying hash-range fields
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum HashRangeFilter {
    /// Filter by exact hash and range key match
    HashRangeKey { hash: String, range: String },
    /// Filter by hash value only (returns all range keys for that hash)
    HashKey(String),
    /// Filter by exact range key value (across all hash groups for HashRange schemas)
    RangeKey(String),
    /// Filter by range key prefix within a specific hash group
    HashRangePrefix { hash: String, prefix: String },
    /// Filter by range key prefix across all hash groups
    RangePrefix(String),
    /// Filter by range key range within a specific hash group
    HashRangeRange {
        hash: String,
        start: String,
        end: String,
    },
    /// Filter by range key range across all hash groups
    RangeRange { start: String, end: String },
    /// Take a small **peek** at the data — the first [`SAMPLE_PEEK_CAP`] records
    /// in stable key order, regardless of the `n` requested. This is a
    /// "what does the data look like" primitive, **not** a fetch/pagination
    /// mechanism: `n` is clamped to [`SAMPLE_PEEK_CAP`] at every apply site, so
    /// `SampleN(10_000)` returns at most [`SAMPLE_PEEK_CAP`] rows. For real
    /// fetches use a key/range filter or [`HashRangeFilter::Page`].
    SampleN(usize),
    /// Paginated fetch: the stable-sorted records `[offset, offset + limit)`.
    /// This is the bounded "list all, by page" primitive that replaced the old
    /// `SampleN(big)` bulk-fetch abuse — it materializes only the requested
    /// page, never the whole field. An accurate `total_count` for the page is
    /// computed separately from a cheap key count (no atom bodies loaded), so
    /// pagination stays exact without the O(field-size) memory blowup.
    Page { offset: usize, limit: usize },
    /// Keyset paginated fetch: the first `limit` stable-sorted records whose
    /// `(range, hash)` key is strictly greater than `after`.
    ///
    /// Unlike [`HashRangeFilter::Page`], continuation cost and correctness do
    /// not depend on how many rows came before the cursor. This is the "scan
    /// after the last key seen" primitive behind `/api/query` cursor
    /// pagination, so concurrent inserts before the cursor cannot shift a
    /// later page into returning duplicates.
    PageAfter { after: KeyValue, limit: usize },
    /// Filter by multiple hash-range key pairs
    HashRangeKeys(Vec<(String, String)>),
    /// Filter by range key pattern within a specific hash group
    HashRangePattern { hash: String, pattern: String },
    /// Filter by range key pattern across all hash groups
    RangePattern(String),
    /// Filter by hash key pattern (supports glob-style matching)
    HashPattern(String),
    /// Filter by hash range (inclusive start, exclusive end) - for hash values
    HashRange { start: String, end: String },
}

/// A page window over the key set selected by a caller's key-restricted filter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KeyWindow {
    /// Offset page over canonical `(range, hash)` key order.
    Offset { offset: usize, limit: usize },
    /// Keyset page after the last key a caller already received.
    After { after: KeyValue, limit: usize },
}

impl KeyWindow {
    #[must_use]
    pub fn offset(offset: usize, limit: usize) -> Self {
        Self::Offset { offset, limit }
    }

    #[must_use]
    pub fn after(after: KeyValue, limit: usize) -> Self {
        Self::After { after, limit }
    }
}

impl HashRangeFilter {
    /// A **log-safe** view of this filter: the variant and its shape, with every
    /// key string replaced by its [`observability::redact_id!`] token.
    ///
    /// Use this instead of `{:?}` in any `tracing` macro. `Debug` stays faithful
    /// — it is the right thing for a debugger, a test failure, or an assertion —
    /// but a filter is built from *user key material*, and a log line is not the
    /// place for it: `HashKey("meeting-notes-with-alice")` is the user's data,
    /// and on this node it lands in a plaintext file beside a database that is
    /// encrypted at rest.
    ///
    /// Redaction is `redact_id!`, not `redact!`, on purpose. The keys are what
    /// makes these lines worth having — you need to see that the same key was
    /// queried twice, or that a scan repeated across pages — and an xxhash token
    /// keeps that correlation without the content. Counts, offsets and limits
    /// stay in the clear: they carry no user content and they are the numbers an
    /// operator actually reads.
    pub fn redacted(&self) -> RedactedHashRangeFilter<'_> {
        RedactedHashRangeFilter(self)
    }

    /// Does this filter restrict the read to a bounded set of **keys**?
    ///
    /// True for the Dynamo-style access patterns product apps are told to use
    /// (`HashKey`, `HashRange*`, `RangeKey` / `RangePrefix` / `RangeRange`).
    /// False for `Page` / `PageAfter` / `SampleN`, which bound how many rows
    /// come back but still walk the whole schema, and false for the pattern
    /// variants, whose match set cannot be derived without a scan.
    ///
    /// Two separate policies key off this and must not drift apart:
    /// the node's full-schema-scan deprecation gate, and the count-then-fetch
    /// push-down (a key-restricted read counts and pages its **partition**,
    /// where an unfiltered one counts and pages the whole schema). When those
    /// two disagreed, a key-restricted read was allowed through the gate but
    /// denied the push-down, so every page request re-materialized the whole
    /// partition.
    #[must_use]
    pub fn is_key_restricted(&self) -> bool {
        matches!(
            self,
            Self::HashKey(_)
                | Self::HashRangeKey { .. }
                | Self::HashRangePrefix { .. }
                | Self::HashRangeRange { .. }
                | Self::HashRangeKeys(_)
                | Self::RangeKey(_)
                | Self::RangePrefix(_)
                | Self::RangeRange { .. }
                | Self::HashRange { .. }
        )
    }
}

/// Log-safe [`std::fmt::Display`] wrapper for a [`HashRangeFilter`], produced by
/// [`HashRangeFilter::redacted`]. Deliberately has no `Debug`-style escape hatch
/// back to the raw key.
pub struct RedactedHashRangeFilter<'a>(&'a HashRangeFilter);

impl std::fmt::Display for RedactedHashRangeFilter<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // One helper so a new variant cannot accidentally format a key raw:
        // every string field below goes through it.
        fn id(s: &str) -> String {
            observability::redact_id!(s)
        }
        match self.0 {
            HashRangeFilter::HashRangeKey { hash, range } => {
                write!(
                    f,
                    "HashRangeKey {{ hash: {}, range: {} }}",
                    id(hash),
                    id(range)
                )
            }
            HashRangeFilter::HashKey(h) => write!(f, "HashKey({})", id(h)),
            HashRangeFilter::RangeKey(r) => write!(f, "RangeKey({})", id(r)),
            HashRangeFilter::HashRangePrefix { hash, prefix } => write!(
                f,
                "HashRangePrefix {{ hash: {}, prefix: {} }}",
                id(hash),
                id(prefix)
            ),
            HashRangeFilter::RangePrefix(p) => write!(f, "RangePrefix({})", id(p)),
            HashRangeFilter::HashRangeRange { hash, start, end } => write!(
                f,
                "HashRangeRange {{ hash: {}, start: {}, end: {} }}",
                id(hash),
                id(start),
                id(end)
            ),
            HashRangeFilter::RangeRange { start, end } => {
                write!(f, "RangeRange {{ start: {}, end: {} }}", id(start), id(end))
            }
            // No key material — the number is the whole point of the line.
            HashRangeFilter::SampleN(n) => write!(f, "SampleN({n})"),
            HashRangeFilter::Page { offset, limit } => {
                write!(f, "Page {{ offset: {offset}, limit: {limit} }}")
            }
            // `after` is a cursor built from a real key, so it redacts; `limit`
            // does not.
            HashRangeFilter::PageAfter { after, limit } => write!(
                f,
                "PageAfter {{ after: {}, limit: {limit} }}",
                id(&after.to_string())
            ),
            // Only the count: a per-pair list would be a key dump by another
            // name, and the length is what tells you the fan-out.
            HashRangeFilter::HashRangeKeys(pairs) => {
                write!(f, "HashRangeKeys(len={})", pairs.len())
            }
            HashRangeFilter::HashRangePattern { hash, pattern } => write!(
                f,
                "HashRangePattern {{ hash: {}, pattern: {} }}",
                id(hash),
                id(pattern)
            ),
            HashRangeFilter::RangePattern(p) => write!(f, "RangePattern({})", id(p)),
            HashRangeFilter::HashPattern(p) => write!(f, "HashPattern({})", id(p)),
            HashRangeFilter::HashRange { start, end } => {
                write!(f, "HashRange {{ start: {}, end: {} }}", id(start), id(end))
            }
        }
    }
}

/// Result of a hash-range filter operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HashRangeFilterResult {
    /// Matches with composite keys in format "KeyValue" -> atom_uuid
    pub matches: HashMap<KeyValue, String>,
    /// Total count of matches found
    pub total_count: usize,
    /// Number of hash groups that had matches
    pub hash_groups_count: usize,
}

impl HashRangeFilterResult {
    /// Creates an empty result
    pub fn empty() -> Self {
        Self {
            matches: HashMap::new(),
            total_count: 0,
            hash_groups_count: 0,
        }
    }

    /// Creates a result with matches
    pub fn new(matches: HashMap<KeyValue, String>) -> Self {
        // `hash_groups_count` is the number of distinct hash groups across
        // the matches — not the total number of (hash, range) pairs. A
        // single-hash filter (e.g. `HashKey("user1")`) over a HashRange
        // field can return many `(user1, range_*)` matches but still
        // touches only one hash group. Range-only keys (no hash) don't
        // belong to any hash group, so they're excluded.
        let hash_groups_count = matches
            .keys()
            .filter_map(|kv| kv.hash.as_deref())
            .collect::<HashSet<_>>()
            .len();

        Self {
            total_count: matches.len(),
            matches,
            hash_groups_count,
        }
    }
}
