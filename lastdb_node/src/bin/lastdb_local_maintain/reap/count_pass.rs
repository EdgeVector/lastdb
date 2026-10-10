//! Keys-only counting passes over the other collections of the plan.
//!
//! Each pass reads the stored bytes, never the decrypted values. It counts
//! the keys that the rule set matches and the bytes they hold. These are the
//! numbers that the engine must match later.

use std::sync::Arc;

use fold_db::storage::traits::KvStore;
use serde::{Deserialize, Serialize};

use super::rules::RuleSet;
use super::walk::{engine_id, Walker};
use super::ReapError;

/// What one counting pass found in one collection.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct CollectionCount {
    /// False when the collection is not on disk. The counts are then zero.
    pub present: bool,
    pub scanned_keys: u64,
    pub scanned_bytes: u64,
    pub matched_keys: u64,
    pub matched_bytes: u64,
}

/// Count the keys of one collection that `rules` match.
pub(crate) async fn count_matches(
    kv: Arc<dyn KvStore>,
    rules: &RuleSet,
) -> Result<CollectionCount, ReapError> {
    let mut count = CollectionCount {
        present: true,
        ..CollectionCount::default()
    };
    let mut walker = Walker::new(kv);
    while let Some(page) = walker.next_page().await? {
        for (key, stored) in &page.rows {
            let id = engine_id(key);
            let bytes = (id.len() + stored.len()) as u64;
            count.scanned_keys += 1;
            count.scanned_bytes += bytes;
            if rules.matches(&id) {
                count.matched_keys += 1;
                count.matched_bytes += bytes;
            }
        }
    }
    Ok(count)
}
