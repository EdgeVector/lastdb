use crate::schema::types::key_value::KeyValue;
use serde::{Deserialize, Serialize};

/// Keyed filter selectors for Hash and HashRange queries.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum HashRangeFilter {
    /// Filter by exact hash and range key match.
    HashRangeKey { hash: String, range: String },
    /// Filter by hash value only (returns all range keys for that hash).
    HashKey(String),
    /// Filter by range key prefix within a specific hash group.
    HashRangePrefix { hash: String, prefix: String },
    /// Filter by range key range within a specific hash group.
    HashRangeRange {
        hash: String,
        start: String,
        end: String,
    },
    /// Filter by multiple hash-range key pairs.
    HashRangeKeys(Vec<(String, String)>),
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
    /// Every supported [`HashRangeFilter`] variant restricts the read to explicit keys or one hash.
    #[must_use]
    pub const fn is_key_restricted(&self) -> bool {
        true
    }
}
