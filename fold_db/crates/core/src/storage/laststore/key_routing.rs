//! Main-key → physical collection routing tables and dual-read candidate lists.

use super::*;

/// `pub(crate)` so `sync::policy` can assert against the routing table itself
/// rather than restating it: `atom_locators` is on `COMPACT_ALLOWLIST` only
/// while `aloc:` is the single prefix routed into it.
pub(crate) const MAIN_KEY_PREFIX_COLLECTIONS: &[(&str, &str)] = &[
    ("atom:", "atoms"),
    // Canonical target, so `main_collections_for_key` dedups to a single
    // candidate: a locator lookup is one point read in one collection, never a
    // fallback chain.
    ("aloc:", "atom_locators"),
    // Keep the longer v2 prefix first. This gives each format its own physical
    // byte gauge while both remain one logical `main` key family.
    ("aref:v2:", "atom_ref_edges_v2"),
    // Rebuildable v1 atom liveness edges. The collection is local-only and
    // excluded from cloud backup; restore rebuilds it from source rows.
    ("aref:", "atom_ref_edges"),
    // Rebuildable active molecule and blob liveness sets. Canonical catalog,
    // protein, and atom rows recreate these local delete-safety indexes.
    ("mref:v1:", "molecule_ref_edges"),
    ("bref:v1:", "blob_ref_edges"),
    // Write target agrees with `classify_main_key`. Leftover `tips` rows
    // stay visible via the proteins-target dual-read arm in
    // `main_collections_for_key` / `main_collections_for_prefix` until
    // residue drain — do not list `tips` here or the two tables disagree.
    ("protein:", "proteins"),
    ("molprot:", "proteins"),
    ("fldprot:", "proteins"),
    ("pfq:", "proteins"),
    // 2026-07-31 primary soak: field_tips is absent and dual_read legacy_hits
    // for mk:/field_tips stayed at 0. New and live reads use tips only.
    // 2026-08-05: mh:/tv: dual-read of field_tip_headers / field_tip_versions
    // removed after tip-residue compact procedure + empty-residue exit
    // (see PRUNED_ZERO_HIT_*). Explicit drain-tip-residue still uses
    // TIP_RESIDUE_LEGACY_COLLECTIONS.
    ("mord:", "field_update_order_log"),
    ("moc:", "field_update_order_count"),
    // Immutable molecule bases, the selected-base pointer, and sparse
    // post-base delete markers are authoritative tip state.
    ("mgp:v1:", "tips"),
    ("mgr:v1:", "tips"),
    ("mgd:v1:", "tips"),
    // field_update_order_legacy is also absent on the primary; keep the
    // populated order-log/count collections above until their drain cards land.
    ("mhr:", "field_hashrange_page_index"),
    ("mhk:", "field_hashrange_hash_index"),
    ("mhi:", "field_hashrange_complete"),
    ("history:", "mutation_history"),
    // Cold residual only: product mutation never co-writes `ref:{M}` blobs.
    // Inventory/purge + sync migrate-on-receive still address this collection.
    ("ref:", "legacy_blob_refs"),
    ("schema_atoms:", "schema_atom_index"),
    ("idx:", "schema_atom_index"),
    // Mapping retained so operators can name the legacy collection in drain
    // sources; live dual-read no longer consults it (see retired list below).
    ("schemaidx:", "legacy_schema_secondary_index"),
    ("conflict:", "sync_conflicts"),
];

/// Index-plane legacy split collections that live dual-read no longer consults.
///
/// `mhr:` / `mhk:` / `mhi:`: field_hashrange_* splits retired after zero-hit soak.
/// `schemaidx:`: product listings use the local-only `schema_index` namespace;
/// primary dual_read legacy_hits for `legacy_schema_secondary_index` stayed at 0.
/// Explicit CoW drain (`drain-index-residue --source legacy-schema-secondary-index`)
/// still copies/deletes/drops the collection.
pub(super) const RETIRED_INDEX_LEGACY_SPLIT_PREFIXES: &[&str] =
    &["mhr:", "mhk:", "mhi:", "schemaidx:"];
/// Main-key prefixes whose legacy dual-read collections were empty / aside after
/// zero-hit soak + tip-residue compact exit. Live logical-main reads must
/// **not** re-open those collections: candidate lists are write-target only
/// (`tips`). Do not re-add these prefixes to [`MAIN_KEY_PREFIX_COLLECTIONS`].
/// Explicit CoW drain (`drain-tip-residue`) still addresses headers/versions.
///
/// LEGACY-SUNSET: tip-family-dual-read-pruned-prefixes | shape=PRUNED_ZERO_HIT guard lists keeping dual-read from reopening tip residue
///   | class=residue
///   | probe=`lastdb status` tip_residue arms 0; `git grep field_tip_headers` only drain/docs after primary empty
///   | trigger=residue-zero-and-drained
///   | owner=lastdb-retire-tip-family-dual-read-arms
/// Sunset note: `docs/security/tip-family-dual-read-sunset.md`. Live dual-read
/// already deleted (fold #1215 / #1259); lists remain until cold drain on
/// primary and residual counter/drain code removal.
pub(super) const PRUNED_ZERO_HIT_MAIN_PREFIXES: &[&str] = &["mk:", "mo:", "mh:", "tv:"];
/// Legacy collection names that dual-read must never reintroduce for
/// [`PRUNED_ZERO_HIT_MAIN_PREFIXES`] (cold historical dirs may still exist).
pub(super) const PRUNED_ZERO_HIT_LEGACY_COLLECTIONS: &[&str] = &[
    "field_tips",
    "field_update_order_legacy",
    "field_tip_headers",
    "field_tip_versions",
];
pub(super) fn is_retired_index_legacy_split_prefix(key: &str) -> bool {
    RETIRED_INDEX_LEGACY_SPLIT_PREFIXES
        .iter()
        .any(|known| colon_prefix_matches(key, known))
}

pub(super) fn is_pruned_zero_hit_main_prefix(bare_key: &str) -> bool {
    PRUNED_ZERO_HIT_MAIN_PREFIXES
        .iter()
        .any(|known| colon_prefix_matches(bare_key, known))
}

/// `rdel:v2:` is new in this draft. No v2 row can live in legacy `main`, so
/// an absent barrier needs one `tips` read. A share receiver stores it under
/// a validated `from:{sender_hash}:` scope.
pub(super) fn is_draft_v2_delete_barrier(key_after_org_scope: &str) -> bool {
    if key_after_org_scope.starts_with("rdel:v2:") {
        return true;
    }
    let Some((sender_hash, rest)) = key_after_org_scope
        .strip_prefix("from:")
        .and_then(|rest| rest.split_once(':'))
    else {
        return false;
    };
    sender_hash.len() == 32
        && sender_hash.bytes().all(|byte| byte.is_ascii_hexdigit())
        && rest.starts_with("rdel:v2:")
}

pub(super) fn push_unique<'a>(collections: &mut Vec<&'a str>, collection: &'a str) {
    if !collections.contains(&collection) {
        collections.push(collection);
    }
}

pub(super) fn main_collection_for_key(key: &[u8]) -> &'static str {
    classify_main_key(key).unwrap_or(TIPS_COLLECTION)
}

/// `key` without a leading 64-hex org storage prefix (`{sha256}:rest`).
pub(super) fn strip_org_storage_prefix(key: &str) -> &str {
    crate::kind_partition::split_org_storage_prefix(key).map_or(key, |(_, rest)| rest)
}

pub(super) fn main_collections_for_key(key: &[u8]) -> Vec<&'static str> {
    let target = main_collection_for_key(key);
    let mut collections = vec![target];
    let Ok(key) = std::str::from_utf8(key) else {
        push_unique(&mut collections, LOGICAL_MAIN_COLLECTION);
        return collections;
    };
    let bare = strip_org_storage_prefix(key);
    for (known, legacy_collection) in MAIN_KEY_PREFIX_COLLECTIONS {
        if colon_prefix_matches(bare, known) {
            // K18 indexes plane dual-read: indexes → tips → legacy split.
            if target == "indexes" {
                push_unique(&mut collections, TIPS_COLLECTION);
            }
            // Protein SOT is `proteins`; leftover keys still live in `tips`.
            if target == "proteins" {
                push_unique(&mut collections, TIPS_COLLECTION);
            }
            if !is_retired_index_legacy_split_prefix(bare) {
                push_unique(&mut collections, legacy_collection);
            }
            return collections;
        }
    }
    if is_pruned_zero_hit_main_prefix(bare) || is_draft_v2_delete_barrier(bare) {
        // Write-target only — never fall through to legacy `main` or a
        // pruned collection for these keys.
        debug_assert!(
            collections
                .iter()
                .all(|c| *c == TIPS_COLLECTION || *c == target),
            "pruned dual-read candidate list must stay write-target only: {collections:?}"
        );
        debug_assert!(
            !collections
                .iter()
                .any(|c| PRUNED_ZERO_HIT_LEGACY_COLLECTIONS.contains(c)
                    || *c == LOGICAL_MAIN_COLLECTION),
            "pruned dual-read must not reopen legacy collections: {collections:?}"
        );
        return collections;
    }
    push_unique(&mut collections, LOGICAL_MAIN_COLLECTION);
    collections
}

pub(super) fn main_collections_for_prefix(prefix: &[u8]) -> Vec<&'static str> {
    if prefix.is_empty() {
        let mut collections = Vec::with_capacity(
            MAIN_MIGRATION_COLLECTIONS.len() + LEGACY_MAIN_MIGRATION_COLLECTIONS.len() + 1,
        );
        collections.extend(MAIN_MIGRATION_COLLECTIONS.iter().copied());
        collections.extend(LEGACY_MAIN_MIGRATION_COLLECTIONS.iter().copied());
        collections.push(LOGICAL_MAIN_COLLECTION);
        return collections;
    }

    let Ok(prefix) = std::str::from_utf8(prefix) else {
        return vec![TIPS_COLLECTION, LOGICAL_MAIN_COLLECTION];
    };
    let bare = strip_org_storage_prefix(prefix);

    let mut collections = Vec::new();
    for (known, collection) in MAIN_KEY_PREFIX_COLLECTIONS {
        if colon_prefix_matches(bare, known) {
            let target = main_collection_for_key(prefix.as_bytes());
            push_unique(&mut collections, target);
            if target == "indexes" {
                push_unique(&mut collections, TIPS_COLLECTION);
            }
            if target == "proteins" {
                push_unique(&mut collections, TIPS_COLLECTION);
            }
            if !is_retired_index_legacy_split_prefix(bare) {
                push_unique(&mut collections, collection);
            }
            return collections;
        }
    }
    if is_pruned_zero_hit_main_prefix(bare) || is_draft_v2_delete_barrier(bare) {
        push_unique(&mut collections, main_collection_for_key(prefix.as_bytes()));
        return collections;
    }

    push_unique(&mut collections, TIPS_COLLECTION);
    collections.extend(
        MAIN_KEY_PREFIX_COLLECTIONS
            .iter()
            .filter_map(|(known, collection)| known.starts_with(bare).then_some(*collection)),
    );
    push_unique(&mut collections, LOGICAL_MAIN_COLLECTION);
    collections.sort_unstable();
    collections.dedup();
    collections
}

pub(super) fn main_collections_for_range(start: &[u8], end: &[u8]) -> Vec<&'static str> {
    if end.is_empty() {
        return main_collections_for_prefix(start);
    }
    let common_len = start
        .iter()
        .zip(end.iter())
        .take_while(|(left, right)| left == right)
        .count();
    main_collections_for_prefix(&start[..common_len])
}
