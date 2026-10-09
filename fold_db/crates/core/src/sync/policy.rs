//! Namespace policy shared by cloud snapshots and store-level capture.

/// Namespaces that store-level capture and cloud snapshot skip because their
/// content is derived / rebuildable, not because they are excluded from cloud
/// **backup**. This gates `capture_should_skip_namespace` /
/// `snapshot_should_skip_namespace` only — it says nothing about the LastStore
/// chunk-backup plane, which has its own independent exclusion list
/// (`BACKUP_EXCLUDED_EXACT` in `storage::laststore::backup_manifest`) coupled
/// instead to at-rest encryption exemption
/// (`encrypting_namespaced_store::LASTSTORE_PLAINTEXT_NAMESPACES`). Renamed
/// from `LOCAL_ONLY_NAMESPACES` (2026-08-03) because that name was read as "not
/// cloud-captured" and used to justify a since-refuted "cloud is not exposed"
/// claim — see `lastdb-couple-backup-inclusion-to-at-rest-exemption`. A
/// namespace here can still end up in cloud backup via the chunk plane; check
/// `backup_role_for_collection` separately.
pub(crate) const CAPTURE_SKIP_NAMESPACES: &[&str] = &[
    "lineage_forward",
    "lineage_reverse",
    // Owner-only attribution proof for an isolated copy. It is rebuilt from
    // catalog roots and canonical product rows; another node must not replay
    // this node's snapshot-specific proof.
    "attribution_ledger",
    // `native_index` removed 2026-08-05 — retired product collection; not a
    // first-class capture-skip catalog entry. Residual cold keys fall through
    // to default capture/snapshot policy.
    "schema_index",
    "idempotency",
    // History: post-WASM ghost `process_results` removed from capture-skip.
    "change_feed",
    // The keep-small meter snapshot plane (2026-09-21). Same contract as the
    // exact-key skip of its legacy `metadata` row below: a node-local gauge,
    // reconstructed by local writes, hydrated as fresh when missing. Another
    // node must never replay this node's budget gauge.
    "keep_small",
];

/// Rebuildable rows inside the logical `main` namespace that mutation-log
/// capture omits. Unlike [`CAPTURE_SKIP_NAMESPACES`], these are key classes,
/// not independently opened namespaces.
///
/// Keep this deliberately narrow. Each skipped prefix must have a **point-key**
/// reconstruction / apply helper (no store scan), or be named dead residue
/// that product writes do not emit. See [`CAPTURE_SKIP_RECONSTRUCT_CONTRACTS`].
///
/// - `aloc:` — rebuild from a partition-prefixed `atom:` key
///   (`replay_put` → `rebuild_atom_locator_if_needed`).
/// - `mk:` — rebuild from `LogOp::MutationIntent` via
///   `MutationManager::apply_replayed_mutations` → serving persist
///   (`write_mutations_batch_inner`). Product writes already capture the
///   intent (not the tip body) inside `capture_logical_commit`. Skipping
///   leftover `mk:` puts (compact put-loops, thin-tip migrate, drain) is
///   what stops those rewrites from full-value-cloning into `sync_pin_log`.
/// - `rdel:v2:` — normal Delete intent rebuilds the durable winner by exact
///   molecule key. A raw barrier put can precede the intent and hide a warm tip.
/// - `mh:` — same apply path writes the molecule header (`header_key`).
/// - `tv:` — same apply path writes archived tip versions
///   (`tip_version_key`) only when point-in-time history is enabled.
///   Mini thin-tip default emits none; leftover admin `tv:` puts must
///   still not clone into the pin-log.
/// - `mord:` / `moc:` — product writes do not emit them. A peer rebuilds the
///   `mk:` tip from MutationIntent through `apply_replayed_mutations`; that
///   apply does not rebuild the log. SampleN reads live `mk:` tips in
///   (range, hash) order. Photograph S can still carry old rows.
///   `replay_order_log_entry` still stores a historical `mord:` put. Named in
///   [`DEAD_ORDER_LOG_RESIDUE_CAPTURE_SKIP_PREFIXES`].
/// - Dead index residue (`mhr:` / `mhk:` / `mhi:` / `schema_atoms:` / `idx:` /
///   `schemaidx:`) — no reconstruct helper. Product writes do not emit them
///   (`HASH_RANGE_*_ENABLED = false`). Named in
///   [`DEAD_INDEX_RESIDUE_CAPTURE_SKIP_PREFIXES`].
///
/// Do **not** add `atom:` or protein prefixes here without their own written
/// contract. Atoms and proteins are primary values. Do **not** add order-log
/// planes to [`CAPTURE_SKIP_BY_KEY_PREFIX_PLANES`] in the same change: that
/// would mark them capture-free and trip the unattended self-compact bar.
pub(crate) const CAPTURE_SKIP_MAIN_KEY_PREFIXES: &[&str] = &[
    "aloc:",
    "aref:",
    "mref:v1:",
    "bref:v1:",
    "mk:",
    "rdel:v2:",
    // Local immutable bases and their selector. Mutation replay reconstructs
    // the same logical rows as direct `mk:` tips on another device.
    "mgp:v1:",
    "mgr:v1:",
    "mgd:v1:",
    "mh:",
    "tv:",
    "mord:",
    "moc:",
    "mhr:",
    "mhk:",
    "mhi:",
    "schema_atoms:",
    "idx:",
    "schemaidx:",
];

/// Exact logical rows whose values are node-local gauges, not replicated
/// state. Keep this separate from the prefix catalog: `metadata` also holds
/// durable correctness records (including the at-rest key proof) which must
/// remain captured.
///
/// `keep_small:meters` is reconstructed by normal local writes. A missing row
/// is explicitly handled as a fresh-home meter snapshot by
/// `AtomStore::hydrate_keep_small`, so replay must not import another node's
/// budget gauge. Since 2026-09-21 the live row lives in the `keep_small`
/// namespace (skipped whole, above); this exact key keeps the legacy
/// `metadata` copy out of capture on homes that still carry it.
pub(crate) const CAPTURE_SKIP_EXACT_KEYS: &[(&str, &str)] = &[("metadata", "keep_small:meters")];

/// Physical planes whose *entire* key population is capture-skipped by prefix.
///
/// [`CAPTURE_SKIP_NAMESPACES`] answers "is this namespace captured?" and
/// [`CAPTURE_SKIP_MAIN_KEY_PREFIXES`] answers "is this row captured?", but a
/// LastStore collection is neither: `atom_locators` is a physical home that
/// `MAIN_KEY_PREFIX_COLLECTIONS` routes one and only one main-key prefix into.
/// Such a plane is capture-free without being a capture-skip *namespace*, and
/// that is exactly the property
/// `compactable_planes_are_capture_skipped_or_a_named_exception` needs to admit
/// it to `COMPACT_ALLOWLIST`.
///
/// Each entry is `(collection, sole main-key prefix)`. The prefix half is not
/// decoration: the bar asserts it is still in
/// [`CAPTURE_SKIP_MAIN_KEY_PREFIXES`], so dropping `aloc:` from that list fails
/// the test instead of quietly turning locator compaction into a mutation-log
/// amplifier — the 2026-08-08 `tips` mistake, which cost 11.58 GiB of new
/// `sync_pin_log`.
pub(crate) const CAPTURE_SKIP_BY_KEY_PREFIX_PLANES: &[(&str, &str)] = &[
    ("atom_locators", "aloc:"),
    ("atom_ref_edges", "aref:"),
    ("atom_ref_edges_v2", "aref:"),
    ("molecule_ref_edges", "mref:v1:"),
    ("blob_ref_edges", "bref:v1:"),
    // Dead index residue. Capture-free; owner compact + residual self-compact.
    ("indexes", "mhr:"),
    ("indexes", "mhk:"),
    ("indexes", "mhi:"),
    ("indexes", "schema_atoms:"),
    ("indexes", "idx:"),
    ("indexes", "schemaidx:"),
];

/// Whether compacting `collection` can emit mutation-log records.
///
/// The self-compactors call this before rewriting a plane, so the safety
/// argument is checked where the rewrite happens rather than only in a test.
/// A plane is capture-free either because its namespace is skipped, or because
/// every key it can hold carries a skipped prefix
/// ([`CAPTURE_SKIP_BY_KEY_PREFIX_PLANES`]).
pub(crate) fn compaction_is_capture_free(collection: &str) -> bool {
    if capture_should_skip_namespace(collection) {
        return true;
    }
    CAPTURE_SKIP_BY_KEY_PREFIX_PLANES
        .iter()
        .any(|(plane, prefix)| {
            *plane == collection && CAPTURE_SKIP_MAIN_KEY_PREFIXES.contains(prefix)
        })
}

/// Captured planes whose physical compactor is capture-neutral because the
/// production capture wrapper suppresses logical capture around the rewrite.
///
/// Membership is explicit because these collections may contain ordinary
/// captured keys. Unattended callers re-check this set at the rewrite site;
/// the policy regression separately pins `with_capture_suppressed` in the
/// production wrapper.
pub(crate) const CAPTURE_NEUTRAL_BY_SUPPRESSION: &[&str] =
    &["tips", "atoms", "field_update_order_log"];

pub(crate) fn compaction_is_capture_neutral_by_suppression(collection: &str) -> bool {
    CAPTURE_NEUTRAL_BY_SUPPRESSION.contains(&collection)
}

/// Namespaces owned by sync/capture bookkeeping rather than user data.
pub(crate) const SYNC_INTERNAL_NAMESPACES: &[&str] = &[
    "sync_outbox",
    "sync_capture",
    // The crash-safe dirty-key intent queue written by `capture::write_path`
    // before every captured local write and deleted immediately after. It was
    // missing here until 2026-08-17 even though every sibling `sync_*` queue
    // was listed. The omission was latent rather than harmful — the engine
    // stages markers through the *encrypting* store, below the capture
    // wrapper, so nothing was double-captured — but it also blocked the
    // `compactable_planes_are_capture_skipped_or_a_named_exception` bar, and
    // that plane needs compaction: see `COMPACT_ALLOWLIST`.
    "sync_capture_reexport",
    "sync_cursors",
    "sync_pin_log",
    "sync_thumb_cache",
    "sync_replay_quarantine",
    "__sled__default",
    "__at_rest_strict_markers",
];

pub(crate) fn is_capture_skip(name: &str) -> bool {
    CAPTURE_SKIP_NAMESPACES.contains(&name)
}

pub(crate) fn is_sync_internal(name: &str) -> bool {
    SYNC_INTERNAL_NAMESPACES.contains(&name)
}

/// Catalog namespaces whose leftover physical capture is expected and must be
/// reported separately from product-path fallback.
pub(crate) fn is_catalog_namespace(name: &str) -> bool {
    crate::storage::LASTSTORE_PLAINTEXT_NAMESPACES.contains(&name)
}

/// Leftover writes that must travel as applyable `LogOp::Put` bodies.
///
/// `db_catalog` rows are small JSON membership records. A member Mini that
/// holds only the org E2E key restores them from the org log. Other leftover
/// catalog planes (`schemas`) stay `PhysicalDigest` because those bodies are
/// large and snapshot-backed; replay of a digest skips apply.
pub(crate) fn leftover_capture_is_applyable(namespace: &str) -> bool {
    namespace == "db_catalog"
}

pub(crate) fn capture_should_skip_namespace(name: &str) -> bool {
    is_capture_skip(name) || is_sync_internal(name)
}

/// Whether one logical KV row should be omitted from mutation-log capture.
///
/// Org rows carry a 64-hex storage prefix before the ordinary main key. Strip
/// that scope only for classification; the replay side restores the locator
/// under the same scope.
///
/// Skip applies on logical `main` **and** on the physical plane that prefix
/// is routed into (`tips` for `mk:`, `atom_locators` for `aloc:`). Compact
/// and leftover admin rewrites open those planes directly; a main-only skip
/// would still full-value-capture those puts (the 2026-08-08 amplifier).
pub(crate) fn capture_should_skip_key(namespace: &str, key: &[u8]) -> bool {
    let Ok(key) = std::str::from_utf8(key) else {
        return false;
    };
    // The mutation intent already carries the signed author-clock tuple.
    // Replay observes that tuple and rebuilds the local high-water state.
    // Capturing this device-local row would add a duplicate physical record.
    if namespace == "metadata" && key.starts_with("mutation_author_clock:") {
        return true;
    }
    if CAPTURE_SKIP_EXACT_KEYS.contains(&(namespace, key)) {
        return true;
    }
    let base_key =
        crate::sync::org_sync::strip_storage_prefix(key).map_or(key, |(_, base_key)| base_key);
    let Some(prefix) = CAPTURE_SKIP_MAIN_KEY_PREFIXES
        .iter()
        .copied()
        .find(|prefix| crate::kind_partition::colon_prefix_matches(base_key, prefix))
    else {
        return false;
    };
    namespace == "main" || namespace_is_physical_home_of_skipped_prefix(namespace, prefix)
}

/// Physical LastStore collection that holds `prefix` (not a capture-skip
/// namespace). Distinct from [`CAPTURE_SKIP_BY_KEY_PREFIX_PLANES`], which
/// requires the plane's *entire* population to be that one prefix.
fn namespace_is_physical_home_of_skipped_prefix(namespace: &str, prefix: &str) -> bool {
    match prefix {
        "mk:" | "mgp:v1:" | "mgr:v1:" | "mgd:v1:" | "mh:" | "tv:" => namespace == "tips",
        "aloc:" => namespace == "atom_locators",
        "aref:" => namespace == "atom_ref_edges" || namespace == "atom_ref_edges_v2",
        "mref:v1:" => namespace == "molecule_ref_edges",
        "bref:v1:" => namespace == "blob_ref_edges",
        // Live home is the order-log plane; leftover dual-read rows also sit
        // on `tips` and must not re-enter capture when that plane is rewritten.
        "mord:" => namespace == "field_update_order_log" || namespace == "tips",
        "moc:" => namespace == "field_update_order_count" || namespace == "tips",
        "mhr:" | "mhk:" | "mhi:" | "schema_atoms:" | "idx:" | "schemaidx:" => {
            namespace == "indexes"
        }
        _ => false,
    }
}

pub(crate) fn snapshot_should_skip_namespace(name: &str) -> bool {
    capture_should_skip_namespace(name)
}
