//! Lossless in-memory range tokens with the original storage-byte order.

use std::cmp::Ordering;
use std::sync::Arc;

/// Every original UTF-8 byte of a storage range.
#[derive(Clone, Debug)]
pub struct CompactRangeKey {
    bytes: Arc<[u8]>,
}

impl CompactRangeKey {
    pub fn from_storage(value: &str) -> Self {
        Self {
            bytes: value.as_bytes().into(),
        }
    }

    /// Includes Arc counters, allocator alignment, and allocator bookkeeping.
    pub fn allocation_bound(value: &str) -> usize {
        value.len().saturating_add(63) & !15
    }

    pub fn to_storage(&self) -> String {
        // Construction accepts only str input, and no method changes bytes.
        String::from_utf8_lossy(&self.bytes).into_owned()
    }

    pub fn starts_with(&self, prefix: &str) -> bool {
        self.bytes.starts_with(prefix.as_bytes())
    }
}

impl Ord for CompactRangeKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.bytes.cmp(&other.bytes)
    }
}

impl PartialOrd for CompactRangeKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for CompactRangeKey {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}

impl Eq for CompactRangeKey {}
