//! Key slot coordinates in the unified HashRange molecule.
//!
//! These are plain values. The schema field model selects a slot layout and a
//! changed key from its key shape; the store reads and writes the same slots.

/// One-dimensional slot layout for narrowed Hash-only / Range-only filter loads
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OneDSlot {
    /// `(hash, "")` records.
    Hash,
    /// `("", range)` records.
    Range,
}

/// Filter/load layout for a field — independent of the on-disk header kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilterLayout {
    /// 1-D hash-only or range-only slots on the unified key layout.
    OneD(OneDSlot),
    /// Full composite HashRange (and Single empty-slot) filter path.
    Composite,
}

/// Empty key component for hash-only or range-only slots in the unified layout.
pub const EMPTY_KEY_COMPONENT: &str = "";

/// Identifies a single key within a per-key molecule — the unit the live
/// write path persists. Unified shape matching [`KeyValue`] / [`FieldKey`]:
/// optional hash + optional range (missing components are empty string on disk).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ChangedKey {
    pub hash: Option<String>,
    pub range: Option<String>,
}

impl ChangedKey {
    #[must_use]
    pub fn hash(hash: impl Into<String>) -> Self {
        Self {
            hash: Some(hash.into()),
            range: None,
        }
    }

    #[must_use]
    pub fn range(range: impl Into<String>) -> Self {
        Self {
            hash: None,
            range: Some(range.into()),
        }
    }

    #[must_use]
    pub fn hash_range(hash: impl Into<String>, range: impl Into<String>) -> Self {
        Self {
            hash: Some(hash.into()),
            range: Some(range.into()),
        }
    }
}
