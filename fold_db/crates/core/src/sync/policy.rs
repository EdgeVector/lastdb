//! Current namespace policy for mutation capture and complete backups.

/// Node-local or derived namespaces omitted from logical capture and snapshots.
/// Reference directories are deliberately absent: current backups retain them.
pub(crate) const CAPTURE_SKIP_NAMESPACES: &[&str] = &[
    "lineage_forward",
    "lineage_reverse",
    "attribution_ledger",
    "schema_index",
    "idempotency",
    "change_feed",
    "keep_small",
];

/// Current main-key classes rebuilt by logical mutation replay, or local GC
/// bookkeeping. These skips do not apply to complete snapshot photographs.
pub(crate) const CAPTURE_SKIP_MAIN_KEY_PREFIXES: &[&str] = &[
    "aloc:",
    "aref:v2:",
    "aref:pending:v1:",
    "aref:catalog-transitions:v1",
    "aref:gc:v1:c:",
    "aref:gc:v1:q:",
    "mref:v1:",
    "bref:v2:",
    "mk:",
    "rdel:v2:",
    "mgp:v1:",
    "mgr:v1:",
    "mgd:v1:",
    "mh:",
    "gcatoms-meta:",
    "gcatoms-probe-ref:",
    "gcatoms-probe-tv-skip:",
    "peer-apply:",
];

pub(crate) const NODE_LOCAL_GC_SCRATCH_PREFIXES: &[&str] = &[
    "gcatoms-meta:",
    "gcatoms-probe-ref:",
    "gcatoms-probe-tv-skip:",
    "peer-apply:",
];

/// The restoring node writes its own peer history and GC checkpoints.
pub(crate) fn is_node_local_gc_scratch_key(key: &[u8]) -> bool {
    let Ok(key) = std::str::from_utf8(key) else {
        return false;
    };
    let base = crate::sync::org_sync::strip_storage_prefix(key).map_or(key, |(_, base)| base);
    NODE_LOCAL_GC_SCRATCH_PREFIXES
        .iter()
        .any(|prefix| crate::kind_partition::colon_prefix_matches(base, prefix))
}

/// Physical collections whose current keys are omitted from mutation capture.
/// Full backups retain every current reference directory and completeness key.
pub(crate) const CAPTURE_SKIP_BY_KEY_PREFIX_PLANES: &[(&str, &str)] = &[
    ("atom_locators", "aloc:"),
    ("atom_ref_edges_v2", "aref:v2:"),
    ("atom_ref_edges", "aref:pending:v1:"),
    ("atom_ref_edges", "aref:catalog-transitions:v1"),
    ("atom_ref_edges", "aref:gc:v1:c:"),
    ("atom_ref_edges", "aref:gc:v1:q:"),
    ("molecule_ref_edges", "mref:v1:"),
    ("blob_ref_edges", "bref:v2:"),
];

pub(crate) fn compaction_is_capture_free(collection: &str) -> bool {
    capture_should_skip_namespace(collection)
        || CAPTURE_SKIP_BY_KEY_PREFIX_PLANES
            .iter()
            .any(|(plane, prefix)| {
                *plane == collection && CAPTURE_SKIP_MAIN_KEY_PREFIXES.contains(prefix)
            })
}

/// These current collections suppress logical capture during physical rewrite.
pub(crate) const CAPTURE_NEUTRAL_BY_SUPPRESSION: &[&str] = &["tips", "atoms"];

pub(crate) fn compaction_is_capture_neutral_by_suppression(collection: &str) -> bool {
    CAPTURE_NEUTRAL_BY_SUPPRESSION.contains(&collection)
}

pub(crate) const SYNC_INTERNAL_NAMESPACES: &[&str] = &[
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

pub(crate) fn is_capture_skip(name: &str) -> bool {
    CAPTURE_SKIP_NAMESPACES.contains(&name)
}

pub(crate) fn is_sync_internal(name: &str) -> bool {
    SYNC_INTERNAL_NAMESPACES.contains(&name)
}

pub(crate) fn is_catalog_namespace(name: &str) -> bool {
    crate::storage::LASTSTORE_PLAINTEXT_NAMESPACES.contains(&name)
}

/// Membership and schema authority need complete bodies after the snapshot.
pub(crate) fn leftover_capture_is_applyable(namespace: &str, key: &[u8]) -> bool {
    namespace == "db_catalog"
        || crate::db_operations::schema_store::SchemaStore::is_replay_authority_key(namespace, key)
}

pub(crate) fn capture_should_skip_namespace(name: &str) -> bool {
    is_capture_skip(name) || is_sync_internal(name)
}

pub(crate) fn capture_should_skip_key(namespace: &str, key: &[u8]) -> bool {
    let Ok(key) = std::str::from_utf8(key) else {
        return false;
    };
    // Replay observes the author-clock tuple from its mutation intent.
    if namespace == "metadata" && key.starts_with("mutation_author_clock:") {
        return true;
    }
    let base = crate::sync::org_sync::strip_storage_prefix(key).map_or(key, |(_, base)| base);
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
        "mk:"
        | "rdel:v2:"
        | "mgp:v1:"
        | "mgr:v1:"
        | "mgd:v1:"
        | "mh:"
        | "gcatoms-meta:"
        | "gcatoms-probe-ref:"
        | "gcatoms-probe-tv-skip:"
        | "peer-apply:" => namespace == "tips",
        "aloc:" => namespace == "atom_locators",
        "aref:v2:" => namespace == "atom_ref_edges_v2",
        "aref:pending:v1:" | "aref:catalog-transitions:v1" | "aref:gc:v1:c:" | "aref:gc:v1:q:" => {
            namespace == "atom_ref_edges"
        }
        "mref:v1:" => namespace == "molecule_ref_edges",
        "bref:v2:" => namespace == "blob_ref_edges",
        _ => false,
    }
}

pub(crate) fn snapshot_should_skip_namespace(name: &str) -> bool {
    capture_should_skip_namespace(name)
}
