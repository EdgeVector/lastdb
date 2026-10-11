//! Background tip regeneration for same-product multi-key schemas.
//!
//! Field mappers share **payload** molecules across keyed siblings. Partition
//! tips under a new/repaired key layout must still be built by walking source
//! records and emitting keyed membership rows — async/reindex job class
//! (preference-schema-expand-same-product-different-keys).
//!
//! # Shipped entry point
//!
//! [`KeyedMembershipIndex::reindex`] is the operator Mini / fkanban heal /
//! background workers call: plan tips from source records, persist them into
//! a partition-queryable index. Tests and callers query via
//! [`KeyedMembershipIndex::query_partition`] — not by re-planning.

use std::collections::HashMap;

/// One source-of-truth record (e.g. a Card) used to regenerate membership tips.
#[derive(Debug, Clone)]
pub struct SourceRecord {
    pub fields: HashMap<String, String>,
}

/// One regenerated tip under the target key layout.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegeneratedTip {
    /// Partition key (HashKey / HashRange hash component).
    pub hash_key: String,
    /// Range key when the target is HashRange; None for Hash-only.
    pub range_key: Option<String>,
    /// Thin row fields to dual-write (shared mapped fields + key fields).
    pub fields: HashMap<String, String>,
}
