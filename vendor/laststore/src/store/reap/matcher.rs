//! Match raw key bytes against prefix and exact rules.

use std::collections::HashSet;

/// The compiled rules of one collection.
///
/// The prefix list has no entry that another entry already covers. For such a
/// list, at most one prefix matches a key, and that prefix is the greatest
/// prefix that sorts at or before the key. A binary search finds it.
pub struct Matcher {
    prefixes: Vec<Vec<u8>>,
    exact: HashSet<Vec<u8>>,
}

impl Matcher {
    /// Compile the rules. Empty prefixes must not reach this function.
    pub(super) fn new(mut prefixes: Vec<Vec<u8>>, exact: HashSet<Vec<u8>>) -> Self {
        prefixes.sort_unstable();
        prefixes.dedup();
        let mut kept: Vec<Vec<u8>> = Vec::with_capacity(prefixes.len());
        for prefix in prefixes {
            let covered = kept
                .last()
                .is_some_and(|last| prefix.starts_with(last.as_slice()));
            if !covered {
                kept.push(prefix);
            }
        }
        Self {
            prefixes: kept,
            exact,
        }
    }

    /// True when a rule matches the raw key bytes.
    pub fn matches(&self, key: &[u8]) -> bool {
        if self.exact.contains(key) {
            return true;
        }
        let after = self
            .prefixes
            .partition_point(|prefix| prefix.as_slice() <= key);
        after > 0 && key.starts_with(self.prefixes[after - 1].as_slice())
    }
}
