use super::*;

/// Local or rebuildable collections omitted from the legacy chunk manifest.
/// Native backups copy the held file inventory. Backup inclusion is independent
/// of the CAS-only storage wrapper: direct atom, tip, and control records retain
/// their stored bytes, while encrypted atom content and CAS payloads stay sealed.
pub(super) const BACKUP_EXCLUDED_EXACT: &[&str] = &[
    // Sync machinery / ephemeral queues — not product state.
    "sync_capture",
    // Crash-safe dirty-key intent markers for in-flight local writes. Added
    // 2026-08-17: every `sync_*` ephemeral queue beside it was already
    // excluded, but this one was being uploaded — 3.14 GiB of segment bytes on
    // Tom's primary whose live set was zero. The markers name keys in *this*
    // node's serving store at one instant, and the drain re-reads current
    // truth, so a restored device gains nothing from them and would drain
    // intents belonging to a machine it is not.
    "sync_capture_reexport",
    "sync_outbox",
    "sync_conflicts",
    "sync_cursors",
    "sync_file_blob_known",
    "share_delivery_outbox",
    "org_sync_targets",
    // Current reference directories and proofs remain in physical backups.
    // Owner-only copy-migration proof. A restored node rebuilds it against its
    // own catalog snapshot; it has no cross-node meaning.
    "attribution_ledger",
    // The keep-small meter snapshot (its own plane since 2026-09-21). A
    // node-local gauge that normal writes reconstruct;
    // hydrate treats a missing row as a fresh home. Nothing a restored device
    // gains, and every put is a whole-map rewrite, so keep it out of the
    // photograph like the attribution ledger.
    "keep_small",
    // Local app-consent cache; re-enrolled on device.
    "app_identity:consent_requests",
    // Marker namespace, not user data.
    "__at_rest_strict_markers",
    // `native_index` removed 2026-08-05 — retired product collection is no
    // longer a first-class backup-exclusion catalog entry. Residual cold-home
    // keys (if any) follow the default mutable backup path.
    // Remaining entries are at-rest plaintext and excluded from the uploader.
    // Kept excluded by the Trinity-only pass (see doc comment above): their
    // exclusion is now a restore-completeness choice, not a secrecy one. None
    // of them is source of truth — `schema_states` and `schema_superseded_by`
    // were, and left this list on 2026-08-04.
    "schema_index",
    "public_keys",
    "idempotency",
];

/// Prefixes excluded from backup (legacy asides, temp renames).
pub(super) const BACKUP_EXCLUDED_PREFIXES: &[&str] = &["sync_outbox.aside", "__at_rest"];

pub(super) fn backup_role_for_collection(collection: &str) -> Option<BackupManifestRole> {
    if collection == ATOMS_COLLECTION {
        return Some(BackupManifestRole::Atom);
    }
    if is_backup_excluded_collection(collection) {
        return None;
    }
    // Remaining collections are mutable product state. This role controls
    // retention and publication, not value encryption.
    Some(BackupManifestRole::Mutable)
}

pub(super) fn is_backup_excluded_collection(collection: &str) -> bool {
    if BACKUP_EXCLUDED_EXACT.contains(&collection) {
        return true;
    }
    BACKUP_EXCLUDED_PREFIXES
        .iter()
        .any(|prefix| collection.starts_with(prefix))
}
