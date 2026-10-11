use super::*;

/// Collections that must never enter cloud backup (local-only / rebuildable /
/// aside archives / at-rest-exempt). Everything else on disk with sealed segs
/// is backed up so second-device restore is a usable copy of the primary
/// (schemas, atoms, cas_blobs/file blobs, range indexes, etc.).
///
/// **Coupled to at-rest encryption policy (won't-undo, 2026-08-03):** the
/// chunk-backup plane below has no encryption of its own — it ships on-disk
/// `.seg` bytes verbatim (live-path file-backed PUT), so a namespace
/// exempt from at-rest encryption
/// (`encrypting_namespaced_store::LASTSTORE_PLAINTEXT_NAMESPACES`) that is
/// *not* excluded here goes to the object store in cleartext.
///
/// The first form of this coupling required *exclusion*, on the premise that
/// cleartext catalogs in the object store are a confidentiality defect. The
/// Trinity-only encryption standard (Tom, 2026-08-03 — brain
/// `preference-lastdb-encryption-standard-trinity-only`) retires that premise:
/// published schema definitions are not a secret, and "treating cleartext
/// published schema JSON on disk or in cloud backup as a confidentiality
/// incident" is explicitly out of standard. Excluding `schemas` would also make
/// a restored device an unusable copy of the primary — it is a source-of-truth
/// collection (`mini_cutover::plane_roles::SOT_NAMED_COLLECTIONS`).
///
/// So the coupling now demands a **declaration** rather than exclusion: every
/// at-rest-plaintext namespace must be either listed here or named in
/// `BACKUP_CLEARTEXT_CATALOG_NAMESPACES` below. That keeps the guard load-bearing —
/// adding a namespace to the plaintext allowlist still fails the build until
/// someone states which of the two it is — while allowing the catalogs the
/// standard says may travel in the clear. Enforced by
/// `backup_role_matches_at_rest_encryption_exemption` below.
///
/// **Coupled to source-of-truth role (2026-08-04):** the confidentiality
/// coupling above says nothing about durability, and that gap shipped —
/// `schema_states` and `schema_superseded_by` are SOT
/// (`mini_cutover::plane_roles::SOT_COLLECTIONS`) and were excluded here purely
/// as fallout of the retired secrecy premise, so a restored device silently
/// lost every non-default schema state. Both are now backed up, and
/// `sot_collections_are_backed_up_or_declared_exception` below fails the build
/// if a future SOT collection is excluded without a stated reason.
///
/// The remaining entries that are only here because of the original exclusion
/// coupling (`schema_index`, `public_keys`, `idempotency`) are deliberately left
/// excluded: none is source of truth, so a restored device rebuilds or
/// re-earns them. Their exclusion is a restore-completeness choice, not a
/// secrecy one. (History: post-WASM ghosts `views` / `view_states` /
/// `transform_field_overrides` / `process_results` dropped from this list.)
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
    // Rebuildable atom reverse-reference index. Restore reconstructs it from
    // authoritative tips and tip-version rows.
    "atom_ref_edges",
    "atom_ref_edges_v2",
    "molecule_ref_edges",
    "blob_ref_edges",
    // Owner-only copy-migration proof. A restored node rebuilds it against its
    // own catalog snapshot; it has no cross-node meaning.
    "attribution_ledger",
    // The keep-small meter snapshot (its own plane since 2026-09-21). A
    // node-local gauge that normal writes and the liveness bootstrap rebuild;
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
    // All remaining finalized/CAS-eligible collections (schemas, cas_blobs,
    // field indexes, tips, proteins, log, blobs, …) are mutable product
    // state. "Finalized" here is lifecycle, not crypto — see
    // `concepts-lastdb-cloud-upload-seal-is-lifecycle-not-crypto`; whether the
    // bytes are actually encrypted is a separate question answered by
    // `LASTSTORE_PLAINTEXT_NAMESPACES` and enforced against this exclusion
    // list by `backup_role_matches_at_rest_encryption_exemption` below.
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
