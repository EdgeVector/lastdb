//! Process-lifetime memo of which legacy `history:` prefixes are empty.
//!
//! # Why this exists
//!
//! `history:{molecule}:{ts}` rows are the retired mutation log. The production
//! write path stopped writing them when the `tv:` chain replaced them, so on a
//! current store **every** such prefix is empty. The only live writer left is
//! sync-replay conflict resolution
//! ([`crate::sync::engine::replay::merge::store_merge_conflicts`]).
//!
//! Reading them is not free, and the cost does not scale with what is there —
//! it scales with the store. The prefix is built with `:` separators and
//! therefore contains no [`crate::atom::molecule_key_codec::PARTITION_SEP`],
//! so LastStore's `handles_for_prefix` cannot pin it to a partition and falls
//! back to enumerating **every group in the collection**. Proving the prefix
//! empty costs a full sweep; finding one row costs the same. Measured on the
//! primary 2026-08-09 (`0.23.3-454-gd8cac4689`): a purge that deleted **zero**
//! records held its schema's exclusive barrier 60.3s, and one that deleted a
//! single record held it 310.7s, because the purge planner runs this scan once
//! per field inside that barrier.
//!
//! # Why a memo is sound
//!
//! Emptiness here is **monotone within a store**: a prefix that holds no rows
//! keeps holding none until something writes one, and both writers call
//! [`invalidate_for_event_key`] when they do. So the memo only ever answers "empty" for a
//! prefix that was observed empty and has not been written since.
//!
//! # Why the key carries a store id
//!
//! Emptiness is monotone within a **store**, not within a process, and the two
//! are not the same thing here. Molecule uuids are
//! [`crate::atom::deterministic_molecule_uuid`] — `sha256("{schema}:{field}")` —
//! so two stores that merely share a schema and field name derive the *same*
//! `history:{mol}:` prefix. `build_storage_key` qualifies share namespaces but
//! not stores. A memo keyed on the prefix alone therefore lets store A's
//! observation answer store B's question.
//!
//! That is not a hypothetical arrangement: `Host::boot` is explicitly tested to
//! keep two in-process hosts independent (it must not stamp `LASTDB_HOME`
//! process-globally), and every `#[tokio::test]` in this crate builds a fresh
//! tempdir store inside one shared test process, most of them reusing a handful
//! of schema names.
//!
//! A false "empty" is not merely a missed read. [`crate::fold_db_core::purge`]
//! uses these events twice: once to trace which records legacy history touches,
//! and once to build `history_referenced_by_retained`, the set of atom uuids
//! that retained history still points at and which the storage-slot purge must
//! therefore **keep**. An empty answer to the second question is a retention
//! guard that silently holds nothing.
//!
//! So `mark_empty`/`is_known_empty` are scoped by [`StoreId`].
//! `invalidate_for_event_key` deliberately is **not**: it clears the prefix for
//! every store. Over-invalidating costs one extra scan, which is the safe
//! direction; under-invalidating would reintroduce the defect this scoping
//! exists to remove, and the one live writer holds a raw kv handle with no
//! store id in reach.
//!
//! Only emptiness is cached. A prefix that *has* rows is never memoized, so a
//! legacy store carrying real `history:` rows takes exactly the path it takes
//! today, every time, and rows that appear later are always seen.
//!
//! # Why not fix the key shape instead
//!
//! Giving the key a partition separator (`history:{mol}\0{ts}`) would let the
//! walk prune to one partition and remove the sweep at the source. It would
//! also fork the keyspace: rows written under the old encoding stay addressable
//! only by the old prefix, and readers would have to consult both forever. That
//! failure mode is not hypothetical here — see
//! `checkpoint-db-developer-20260809f-the-rows-were-not-missing-they-were-served-twice`,
//! where exactly this shape of change left 431 live rows served twice. A retired
//! keyspace is not worth a migration; caching the answer is.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};

/// Legacy-history prefix walks that actually reached storage.
///
/// Always compiled, not `#[cfg(test)]`: the integration probe
/// (`tests/purge_bulk_scaling_probe.rs`) reads it too, and a counter that only
/// exists under `cfg(test)` is invisible from `tests/`. One relaxed increment
/// on a path that already does a collection-wide walk costs nothing worth
/// measuring.
static SCANS: AtomicU64 = AtomicU64::new(0);

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

/// Count of walks that reached storage since process start.
///
/// This is the number the fix is about: it should rise once per molecule and
/// then stop, rather than once per field per purge.
pub fn scans() -> u64 {
    SCANS.load(Ordering::Relaxed)
}

/// Called by the scan itself, immediately before it reaches storage.
pub fn record_scan() {
    SCANS.fetch_add(1, Ordering::Relaxed);
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
    let Some(start) = event_key
        .find("history\0")
        .or_else(|| event_key.find("history:"))
    else {
        return;
    };
    // `history:{mol}:` / `history\0{mol}:` — up to and including the separator
    // after the uuid. Both tags are 8 bytes.
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
