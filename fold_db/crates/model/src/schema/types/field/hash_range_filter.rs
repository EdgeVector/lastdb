use crate::schema::types::key_value::KeyValue;
use serde::{Deserialize, Serialize};

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
    /// Take a bounded peek of at most [`SAMPLE_PEEK_CAP`] records.
    /// A sample does not certify complete membership or global key order.
    /// Storage candidates refill until enough valid members appear or their
    /// source is exhausted. Resource limits refuse an incomplete short sample.
    /// Use a key selector or an administrative Page for an ordered result.
    SampleN(usize),
    /// Paginated fetch: the stable-sorted records `[offset, offset + limit)`.
    /// Administrative pages establish a complete bounded logical candidate
    /// set before ordering or slicing. They refuse above the inspection key
    /// or byte allowance. Product reads select one hash or explicit keys.
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
    /// A page of an unfiltered record walk after one record key position:
    /// the record key suffix (`{esc(blind(hash))}\0{range}`) that a walk
    /// page ended at. A run of deleted records can end a page before it
    /// reaches a live record, and a deleted record has no API key.
    AfterRecord { record: String, limit: usize },
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

    #[must_use]
    pub fn after_record(record: String, limit: usize) -> Self {
        Self::AfterRecord { record, limit }
    }
}

impl HashRangeFilter {
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
