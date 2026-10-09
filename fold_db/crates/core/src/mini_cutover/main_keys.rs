//! Split the sled `main` catch-all tree into real Last Store collections.
//!
//! Prefix table is migration-era compatibility data kept for older LastStore
//! homes; historical cutover reports live under `docs/history/mini-cutover/`.
//! `lastdb_local_maintain breakdown-main` still uses this classification
//! against real data.

use crate::kind_partition::{colon_prefix_matches, split_org_storage_prefix};

/// Target Last Store collections a `main` key can land in.
///
/// `atom_locators` holds the `aloc:{uuid}` → partition index. It is its own
/// collection on purpose: `atoms` is the collection whose cold group loads the
/// partition-prefix work exists to bound, so its per-atom index does not belong
/// in it. LastStore creates a collection lazily on first write and reads an
/// absent one as empty, so a home that has never written a locator is unaffected.
///
/// `proteins` is the SOT home for protein membership + fold queue
/// (`protein:` / `molprot:` / `fldprot:` / `pfq:`). `MAIN_KEY_PREFIX_COLLECTIONS`
/// lists the same write target. Legacy homes may still hold those keys under
/// `tips`; dual-read is proteins → tips (proteins-target arm in laststore).
/// `indexes` is the single rebuildable-helper plane (K18).
pub const MAIN_MIGRATION_COLLECTIONS: &[&str] =
    &["atoms", "tips", "atom_locators", "proteins", "indexes"];

/// Migration-era split collections that older LastStore homes may still carry
/// **and** that live logical-main dual-read may still consult on empty-prefix
/// scans. This list is the product-open set only — empty-prefix fan-out and
/// dual-read inventory must not reopen dual-read-deleted residue.
///
/// Intentionally **not** listed (still classifiable via `plane_roles` for cold
/// dirs / drain inventory, but product never opens them on live logical-main):
/// - tip residue: `field_tips`, `field_update_order_legacy`,
///   `field_tip_headers`, `field_tip_versions` (drain-tip-residue)
/// - index splits retired after zero-hit soak: `field_hashrange_*`,
///   `legacy_schema_secondary_index` (drain-index-residue)
///
/// Keep history-adjacent order-log (`field_update_order_log` /
/// `field_update_order_count`) and other still-live dual-read homes.
pub const LEGACY_MAIN_MIGRATION_COLLECTIONS: &[&str] = &[
    "field_update_order_log",
    "field_update_order_count",
    "mutation_history",
    "legacy_blob_refs",
    "schema_atom_index",
    "sync_conflicts",
];
/// Classify one `main` key (raw bytes) into its destination collection.
/// Returns `None` for an unknown prefix — callers must fail loudly.
pub fn classify_main_key(key: &[u8]) -> Option<&'static str> {
    let s = std::str::from_utf8(key).ok()?;
    let bare = split_org_storage_prefix(s).map_or(s, |(_, rest)| rest);
    if colon_prefix_matches(bare, "atom:") {
        Some("atoms")
    } else if colon_prefix_matches(bare, "aloc:") {
        Some("atom_locators")
    } else if bare.starts_with("aref:v2:") {
        Some("atom_ref_edges_v2")
    } else if colon_prefix_matches(bare, "aref:") {
        Some("atom_ref_edges")
    } else if bare.starts_with("mref:v1:") {
        Some("molecule_ref_edges")
    } else if bare.starts_with("bref:v1:") {
        Some("blob_ref_edges")
    } else if bare.starts_with("protein:")
        || bare.starts_with("molprot:")
        || bare.starts_with("fldprot:")
        || bare.starts_with("pfq:")
    {
        // Ideal plane: first-class proteins collection (docs/lastdb-ideal-storage-shape.md).
        Some("proteins")
    } else if bare.starts_with("mhr:")
        || bare.starts_with("mhk:")
        || bare.starts_with("mhi:")
        || bare.starts_with("schema_atoms:")
        || bare.starts_with("idx:")
        || colon_prefix_matches(bare, "schemaidx:")
    {
        Some("indexes")
    } else if bare.starts_with("mk:")
        || bare.starts_with("rdel:v1:")
        || bare.starts_with("rdel:v2:")
        || bare.starts_with("mgp:v1:")
        || bare.starts_with("mgr:v1:")
        || bare.starts_with("mgd:v1:")
        || bare.starts_with("mh:")
        || colon_prefix_matches(bare, "tv:")
        || colon_prefix_matches(bare, "mord:")
        || colon_prefix_matches(bare, "moc:")
        || bare.starts_with("mo:")
        || colon_prefix_matches(bare, "history:")
        || bare.starts_with("ref:")
        || colon_prefix_matches(bare, "conflict:")
        || colon_prefix_matches(bare, "dellog:")
        || colon_prefix_matches(bare, "gcatoms-probe-ref:")
    {
        Some("tips")
    } else {
        None
    }
}
