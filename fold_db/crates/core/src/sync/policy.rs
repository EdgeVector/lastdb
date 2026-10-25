//! Current namespace policy for mutation capture and complete backups.

/// Node-local or derived namespaces omitted from logical capture and snapshots.
/// Reference directories are deliberately absent: current backups retain them.
pub const CAPTURE_SKIP_NAMESPACES: &[&str] = &[
    "lineage_forward",
    "lineage_reverse",
    "attribution_ledger",
    "schema_index",
    "idempotency",
    "change_feed",
    "keep_small",
];

/// Current main-key classes rebuilt by logical mutation replay, or kept only on
/// the node that wrote them. These skips do not apply to complete snapshot
/// photographs.
///
/// Do **not** add `atom:` or protein prefixes here without their own written
/// contract. Atoms and proteins are primary values. The one exception is the
/// atom counter record (`atom:{uuid}\0c`), which `capture_should_skip_key`
/// skips by its tag, because the planner rebuilds it on every node.
pub const CAPTURE_SKIP_MAIN_KEY_PREFIXES: &[&str] = &[
    "mref:v1:", "bref:v3:", "mk:", "rdel:v2:", "mgp:v1:", "mgr:v1:", "mgd:v1:", "mh:", "crq:",
    "crb:", "crs:",
];

/// Physical collections whose current keys are omitted from mutation capture.
/// Full backups retain every current reference directory and completeness key.
pub const CAPTURE_SKIP_BY_KEY_PREFIX_PLANES: &[(&str, &str)] =
    &[("molecule_ref_edges", "mref:v1:")];

pub fn compaction_is_capture_free(collection: &str) -> bool {
    capture_should_skip_namespace(collection)
        || CAPTURE_SKIP_BY_KEY_PREFIX_PLANES
            .iter()
            .any(|(plane, prefix)| {
                *plane == collection && CAPTURE_SKIP_MAIN_KEY_PREFIXES.contains(prefix)
            })
}

/// These current collections suppress logical capture during physical rewrite.
pub const CAPTURE_NEUTRAL_BY_SUPPRESSION: &[&str] = &["tips", "atoms"];

pub fn compaction_is_capture_neutral_by_suppression(collection: &str) -> bool {
    CAPTURE_NEUTRAL_BY_SUPPRESSION.contains(&collection)
}

pub const SYNC_INTERNAL_NAMESPACES: &[&str] = &[
    "sync_outbox",
    "sync_capture",
    "sync_capture_reexport",
    "sync_cursors",
    "sync_foreign_share_bindings",
    "sync_foreign_share_history",
    "sync_foreign_share_frontiers",
    "sync_pin_log",
    "sync_thumb_cache",
    "sync_replay_quarantine",
    "__sled__default",
    "__at_rest_strict_markers",
];

pub fn is_capture_skip(name: &str) -> bool {
    CAPTURE_SKIP_NAMESPACES.contains(&name)
}

pub fn is_sync_internal(name: &str) -> bool {
    SYNC_INTERNAL_NAMESPACES.contains(&name)
}

/// Membership and schema authority need complete bodies after the snapshot.
pub fn leftover_capture_is_applyable(namespace: &str, key: &[u8]) -> bool {
    namespace == "db_catalog"
        || crate::db_operations::schema_store::SchemaStore::is_replay_authority_key(namespace, key)
}

pub fn capture_should_skip_namespace(name: &str) -> bool {
    is_capture_skip(name) || is_sync_internal(name)
}

pub fn capture_should_skip_key(namespace: &str, key: &[u8]) -> bool {
    if namespace == "main"
        && key == crate::storage::disk_pack::restore_safety::PENDING_KEY.as_bytes()
    {
        return true;
    }
    let Ok(key) = std::str::from_utf8(key) else {
        return false;
    };
    // Replay observes the author-clock tuple from its mutation intent.
    if namespace == "metadata" && key.starts_with("mutation_author_clock:") {
        return true;
    }
    // An atom counter (`atom:{uuid}\0c`) is node-local state. A peer must never
    // import another node's count. Complete mutation replay uses the local
    // counter writer; physical lifecycle replay is refused.
    if crate::atom::atom_key_codec::is_counter_key(key) {
        return namespace == "main" || namespace == "atoms";
    }
    let base = crate::kind_partition::split_org_storage_prefix(key).map_or(key, |(_, base)| base);
    let Some(prefix) = CAPTURE_SKIP_MAIN_KEY_PREFIXES
        .iter()
        .copied()
        .find(|prefix| crate::kind_partition::colon_prefix_matches(base, prefix))
    else {
        return false;
    };
    namespace == "main" || namespace_is_physical_home_of_skipped_prefix(namespace, prefix)
}

fn namespace_is_physical_home_of_skipped_prefix(namespace: &str, prefix: &str) -> bool {
    match prefix {
        "mk:" | "rdel:v2:" | "mgp:v1:" | "mgr:v1:" | "mgd:v1:" | "mh:" | "crq:" | "crb:"
        | "crs:" => namespace == "tips",
        "mref:v1:" => namespace == "molecule_ref_edges",
        "bref:v3:" => namespace == "atoms",
        _ => false,
    }
}

pub fn snapshot_should_skip_namespace(name: &str) -> bool {
    capture_should_skip_namespace(name)
}
