//! Process-lifetime memo of empty current mutation-history prefixes.
//!
//! Events use `history\0{molecule}:{timestamp}` keys. Conflict and suppressed-Put
//! writers invalidate the matching prefix after a successful event batch.
//! Purge and history inspection reuse a confirmed empty result until then.
//! No older key spelling or archived-tip conversion is involved.
//!
//! Each observation belongs to one opened AtomStore. Stores can share molecule
//! identities without sharing data, so a prefix alone cannot identify a memo
//! entry. Clones share the store identity; a reopen receives a new identity.
//! Invalidation clears every store's observation for that exact prefix because
//! a raw event writer need not hold the AtomStore identity.
//!
//! Only emptiness is cached. Prefixes with events always read current storage.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};

/// Identifies the store an emptiness observation was made against.
///
/// Minted per [`crate::db_operations::atom_store::AtomStore`] construction, so
/// it is per *opened store*, not per home path. Reopening the same home in one
/// process mints a second id and merely loses cache hits — the safe direction.
/// Cloning an `AtomStore` copies the id, which is correct: a clone reads the
/// same bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct StoreId(u64);

impl StoreId {
    /// Mint a fresh id. Called once per store construction.
    #[must_use]
    pub fn next() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        Self(NEXT.fetch_add(1, Ordering::Relaxed))
    }
}

/// Storage-key prefixes (already `build_storage_key`-qualified, so share
/// namespaces are distinct entries) mapped to the stores that observed them
/// empty.
///
/// Keyed prefix-first so [`invalidate_for_event_key`], which has no store id in
/// reach, stays one `remove` rather than a scan of every entry.
fn memo() -> &'static Mutex<HashMap<String, HashSet<StoreId>>> {
    static MEMO: OnceLock<Mutex<HashMap<String, HashSet<StoreId>>>> = OnceLock::new();
    MEMO.get_or_init(|| Mutex::new(HashMap::new()))
}

/// True when `prefix` was observed empty **in this store** and nothing has
/// written under it since.
pub fn is_known_empty(store: StoreId, prefix: &str) -> bool {
    memo()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(prefix)
        .is_some_and(|stores| stores.contains(&store))
}

/// Record that a scan of `prefix` in `store` returned no rows.
///
/// Callers must only reach this with the result of a scan that actually ran;
/// memoizing a skipped scan would make the memo self-confirming.
pub fn mark_empty(store: StoreId, prefix: &str) {
    memo()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .entry(prefix.to_string())
        .or_default()
        .insert(store);
}

/// Drop the memo entry for the prefix that `event_key` was written under, for
/// **every** store.
///
/// Takes the full row key (`…history:{mol}:{ts}`) rather than the prefix so a
/// writer cannot get the truncation wrong: the entry cleared is the one whose
/// scan would otherwise have missed this write. A key with no `history:`
/// segment clears nothing.
///
/// Clearing every store rather than one is deliberate — see the module docs.
/// The live writer (`store_merge_conflicts`) holds a raw kv handle and has no
/// store id to pass, and an extra scan is cheaper than a missed row.
pub fn invalidate_for_event_key(event_key: &str) {
    let Some(start) = event_key.find("history\0") else {
        return;
    };
    // Current `history\0{mol}:`, through the separator after the molecule UUID.
    let after_tag = start + 8;
    let Some(colon) = event_key[after_tag..].find(':') else {
        return;
    };
    let prefix = &event_key[..after_tag + colon + 1];
    memo()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .remove(prefix);
}
