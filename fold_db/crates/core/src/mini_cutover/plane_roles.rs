//! Operator plane roles for LastStore collections (ideal storage shape).
//!
//! Maps each on-disk collection name under `data/data/` to a logical plane so
//! `lastdb status` can attribute disk without inventing a second LastStore root
//! (K13 Phase-1: same data root; cold is a role tag, not a path rewrite).
//!
//! Design: `docs/lastdb-ideal-storage-shape.md` §2 / PR-2 / PR-12.
//! Aside dumps (e.g. `sync_outbox.aside-legacy-*`) are **Tom-gated reclaim** —
//! never auto-deleted by this classification or status path.

use serde::{Deserialize, Serialize};

/// Logical storage plane for operator attribution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CollectionPlaneRole {
    /// Source-of-truth product ladder (schemas, tips, proteins, atoms, CAS).
    Sot,
    /// Rebuildable derived helpers (page index, schema index, single `indexes`).
    Indexes,
    /// Append-only / not rebuildable-from-tips without a proof (order log, tip versions).
    HistoryAdjacent,
    /// Migration-era tip dual-plane residue (field_tips* dual-read only).
    TipResidue,
    /// Locality helper (`atom_locators`).
    Locality,
    /// Sync/meta cold (Phase-1 same root; warm-priority exclude candidate).
    ColdSync,
    /// Node-local ops / identity / markers.
    Ops,
    /// Quarantined aside dumps — Tom-gated delete after backup receipt.
    Aside,
    /// Unrecognized collection name.
    Unknown,
}

impl CollectionPlaneRole {
    /// Stable operator label for status lines.
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::Sot => "SOT",
            Self::Indexes => "indexes",
            Self::HistoryAdjacent => "history-adjacent",
            Self::TipResidue => "tip-residue",
            Self::Locality => "locality",
            Self::ColdSync => "cold/sync",
            Self::Ops => "ops",
            Self::Aside => "aside",
            Self::Unknown => "unknown",
        }
    }

    /// Sort key for status tables (SOT first, aside last).
    #[must_use]
    pub fn sort_key(self) -> u8 {
        match self {
            Self::Sot => 0,
            Self::Indexes => 1,
            Self::HistoryAdjacent => 2,
            Self::TipResidue => 3,
            Self::Locality => 4,
            Self::ColdSync => 5,
            Self::Ops => 6,
            Self::Aside => 7,
            Self::Unknown => 8,
        }
    }
}

/// Every collection that is source of truth — the authoritative set.
///
/// This is the list to check against when the question is "is this collection
/// SOT?", and the only definition [`classify_collection_plane`] consults.
/// [`SOT_NAMED_COLLECTIONS`] is a smaller *display* list and must not be used
/// for that question — see its doc comment for the trap that caused.
///
/// A collection here is a collection a restored device needs. Adding one
/// obliges a cloud-backup disposition:
/// `storage::laststore::backup_manifest::sot_collections_are_backed_up_or_declared_exception`
/// fails the build until the new entry is either in the backup set or named in
/// that module's stated exception list.
pub const SOT_COLLECTIONS: &[&str] = &[
    "atoms",
    "tips",
    "proteins",
    "schemas",
    "db_catalog",
    "molecule_keys",
    "schema_states",
    "schema_superseded_by",
    "cas_blobs",
];

/// Classify one LastStore collection directory name into a plane role.
///
/// Pure and deterministic. Aside-style names (`*.aside-*`, `*.aside`) map to
/// [`CollectionPlaneRole::Aside`] — reclaim is operator-gated, never automatic.
#[must_use]
pub fn classify_collection_plane(name: &str) -> CollectionPlaneRole {
    let bare = name.trim();
    if bare.is_empty() {
        return CollectionPlaneRole::Unknown;
    }

    // Aside dumps first (may look like sync_* with a suffix).
    if bare.contains(".aside") || bare.ends_with(".aside") {
        return CollectionPlaneRole::Aside;
    }

    // ── SOT ──────────────────────────────────────────────────────────────
    // Derived from the const above so the SOT set has exactly one definition;
    // a reader or a guard can enumerate it without re-reading this match.
    if SOT_COLLECTIONS.contains(&bare) {
        return CollectionPlaneRole::Sot;
    }

    match bare {
        // ── Rebuildable indexes (K18 single home + legacy split names) ───
        "indexes"
        | "field_hashrange_page_index"
        | "field_hashrange_hash_index"
        | "field_hashrange_complete"
        | "schema_index"
        | "legacy_schema_secondary_index"
        | "schema_atom_index"
        // The live atom reverse-edge plane. `design-lastdb-compact-atom-reverse-edge-v2`
        // requirement 7 states it: "Keep the derived plane capture-free and
        // rebuildable." It is a derived helper, and on the primary it is the
        // largest one — 936.7 MiB of 3.36M active edges, projected to 1 GiB.
        // Unclassified it fell to `Unknown`, and `residue_named` used to render
        // every `Unknown` collection under the operator line `residue:`, so the
        // live delete-safety index was presented as reclaimable migration
        // leftovers. Naming it here is what stops that.
        | "atom_ref_edges_v2"
        | "molecule_ref_edges"
        | "blob_ref_edges"
        | "attribution_ledger"
        // The keep-small meter snapshot: one whole-map gauge, rewritten on a
        // debounce tick, rebuilt by the liveness bootstrap. Moved out of
        // `metadata` on 2026-09-21 after its single hash group there reached
        // 39 GB of superseded copies and looped the primary. Same disposition
        // as `attribution_ledger`: node-local, rebuildable, never residue.
        | "keep_small" => CollectionPlaneRole::Indexes,
        // `native_index` is a retired product collection (in-process native
        // embeddings removed). Residual cold-home keys, if any, classify as
        // Unknown — not as a live Indexes plane.
        //
        // `atom_ref_edges` — the drained v1 reverse-edge plane — is deliberately
        // NOT named here. It is genuine migration residue, but the only residue
        // role this map has is `TipResidue`, whose label reads `tip-residue`,
        // and this plane has nothing to do with tips. It classifies `Unknown`,
        // which is now reported as unattributed rather than asserted to be
        // residue. Give it a role when there is a migration-residue role to give.

        // ── History / sync-adjacent (not tips-only rebuildable) ──────────
        "field_update_order_log"
        | "field_update_order_count"
        | "field_update_order_legacy"
        | "field_tip_versions"
        | "mutation_history" => CollectionPlaneRole::HistoryAdjacent,

        // ── Tip dual-plane residue ───────────────────────────────────────
        "field_tips" | "field_tip_headers" => CollectionPlaneRole::TipResidue,

        // ── Locality ─────────────────────────────────────────────────────
        "atom_locators" => CollectionPlaneRole::Locality,

        // ── Cold / sync (K13 Phase-1 same root) ──────────────────────────
        "sync_capture"
        | "sync_cursors"
        | "sync_conflicts"
        | "sync_file_blob_known"
        | "change_feed"
        | "share_delivery_outbox"
        | "legacy_blob_refs" => CollectionPlaneRole::ColdSync,

        // ── Ops / identity ───────────────────────────────────────────────
        "metadata"
        | "node_config"
        | "node_identity"
        | "public_keys"
        | "idempotency"
        | "org_sync_targets"
        | "__at_rest_strict_markers"
        | "app_identity_consent_requests"
        | "app_identity:consent_requests"
        | "main" => CollectionPlaneRole::Ops,

        _ => {
            // Prefixed cold renames (cold_sync_capture, …) if ever applied.
            if bare.starts_with("cold_") || bare.starts_with("sync_") {
                CollectionPlaneRole::ColdSync
            } else {
                CollectionPlaneRole::Unknown
            }
        }
    }
}

/// One collection's plane attribution for status / inventory.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CollectionPlaneEntry {
    pub name: String,
    pub role: CollectionPlaneRole,
    pub bytes: u64,
}

/// Aggregated plane breakdown for operator display.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlaneBreakdown {
    pub entries: Vec<CollectionPlaneEntry>,
    pub by_role_bytes: Vec<(CollectionPlaneRole, u64)>,
    /// Bytes the filesystem has actually allocated — what `du` reports and what
    /// disk-capacity decisions must use.
    pub total_bytes: u64,
    /// Sum of apparent file lengths. Lower than `total_bytes` whenever the
    /// store holds allocated extents past the end of its records; see
    /// [`allocated_bytes`].
    pub total_apparent_bytes: u64,
}

/// Machine-readable ideal-storage plane map for status / milestone proofs.
///
/// Byte totals come from offline collection-dir walks (no LastStore open, no
/// keyspace scan). Full key counts require `lastdb db inventory` (live scan) —
/// this report intentionally leaves `keys` unset so normal status stays cheap.
///
/// Design: `docs/lastdb-ideal-storage-shape.md` (disk-matches-map milestone).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlaneMapReport {
    /// Collection rows sorted like [`PlaneBreakdown`] (role, then size).
    pub collections: Vec<PlaneMapCollection>,
    /// Bytes rolled up by plane role (only roles with mass).
    pub by_role: Vec<PlaneMapRoleTotal>,
    /// Named SOT ladder homes operators watch first (tips, proteins, atoms, …).
    pub sot_named: Vec<PlaneMapCollection>,
    /// Collapsed / migration residue homes still holding mass — collections the
    /// map has actually CLASSIFIED as residue ([`CollectionPlaneRole::TipResidue`]).
    ///
    /// This deliberately excludes [`CollectionPlaneRole::Unknown`], which it used
    /// to absorb. An unrecognized collection is unattributed, not residue, and
    /// merging the two made the map's default fail dangerous: every plane added
    /// to the product without a classifier arm was reported to operators under
    /// the one heading that invites reclaim. Unattributed mass is carried by
    /// [`Self::unknown_active_collections`] and reported as such.
    pub residue_named: Vec<PlaneMapCollection>,
    /// Legacy split planes still present with mass, including derived index
    /// splits such as `field_hashrange_*`.
    #[serde(default)]
    pub legacy_split_named: Vec<PlaneMapCollection>,
    /// Unknown active collections with mass. Empty means every non-empty
    /// collection is accounted for in the ideal map.
    ///
    /// Non-empty means the map is incomplete, and it says nothing about whether
    /// the mass is live or dead. Report it as unattributed. Do not present it
    /// beside residue, and never size a reclaim from it.
    #[serde(default)]
    pub unknown_active_collections: Vec<PlaneMapCollection>,
    /// History-adjacent homes (order-log family) — retained, not rebuild-from-tips.
    pub history_adjacent_named: Vec<PlaneMapCollection>,
    pub total_bytes: u64,
    pub collection_count: usize,
    /// Always null on this path — key enumeration is inventory-only (scan).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub keys: Option<u64>,
    /// How to get key counts without re-deriving status semantics.
    pub keys_source: String,
}

/// One collection in the plane map.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlaneMapCollection {
    pub name: String,
    pub role: CollectionPlaneRole,
    pub bytes: u64,
    /// Dual-read legacy hits served by this collection (process lifetime), if known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dual_read_legacy_hits: Option<u64>,
}

/// Role rollup row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlaneMapRoleTotal {
    pub role: CollectionPlaneRole,
    pub label: String,
    pub bytes: u64,
    pub collections: Vec<String>,
}

/// Dual-read legacy hits aggregated by plane role (from attributed collections).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DualReadPlaneHit {
    pub role: CollectionPlaneRole,
    pub label: String,
    pub legacy_hits: u64,
}

/// Ideal-map SOT collection names called out explicitly for operators.
///
/// **This is a display subset of [`SOT_COLLECTIONS`], not the SOT set.** It
/// exists to keep `lastdb status` readable — the five homes that carry mass and
/// that an operator watches — and it deliberately omits the small catalog-state
/// SOT collections `schema_states` and `schema_superseded_by` (single-digit MiB
/// against a multi-GiB ladder), which would be noise on a status line.
///
/// Do not reason about SOT membership from this list. Asking it whether a
/// collection is source of truth is how the 2026-08-04 cloud-backup gap got its
/// justification: the doc arguing that `schemas` must stay backed up cited this
/// list, which structurally cannot return yes for either of the two SOT
/// collections that were in fact excluded from backup. Use [`SOT_COLLECTIONS`]
/// or [`classify_collection_plane`]. `sot_named_is_a_subset_of_sot_collections`
/// pins the containment so the display list cannot drift out of the SOT set.
pub const SOT_NAMED_COLLECTIONS: &[&str] = &[
    "tips",
    "proteins",
    "atoms",
    "schemas",
    "db_catalog",
    "molecule_keys",
    "cas_blobs",
];

impl PlaneBreakdown {
    /// Build the machine-readable plane map (bytes only; keys stay inventory-only).
    #[must_use]
    pub fn to_plane_map_report(&self) -> PlaneMapReport {
        let collections: Vec<PlaneMapCollection> = self
            .entries
            .iter()
            .map(|e| PlaneMapCollection {
                name: e.name.clone(),
                role: e.role,
                bytes: e.bytes,
                dual_read_legacy_hits: None,
            })
            .collect();

        let by_role: Vec<PlaneMapRoleTotal> = self
            .by_role_bytes
            .iter()
            .filter(|(_, b)| *b > 0)
            .map(|(role, bytes)| {
                let names: Vec<String> = self
                    .entries
                    .iter()
                    .filter(|e| e.role == *role && e.bytes > 0)
                    .map(|e| e.name.clone())
                    .collect();
                PlaneMapRoleTotal {
                    role: *role,
                    label: role.label().to_string(),
                    bytes: *bytes,
                    collections: names,
                }
            })
            .collect();

        let sot_named: Vec<PlaneMapCollection> = collections
            .iter()
            .filter(|c| SOT_NAMED_COLLECTIONS.contains(&c.name.as_str()))
            .cloned()
            .collect();

        let residue_named: Vec<PlaneMapCollection> = collections
            .iter()
            .filter(|c| c.role == CollectionPlaneRole::TipResidue && c.bytes > 0)
            .cloned()
            .collect();

        let legacy_split_named: Vec<PlaneMapCollection> = collections
            .iter()
            .filter(|c| c.bytes > 0 && is_legacy_split_collection(&c.name, c.role))
            .cloned()
            .collect();

        let unknown_active_collections: Vec<PlaneMapCollection> = collections
            .iter()
            .filter(|c| c.role == CollectionPlaneRole::Unknown && c.bytes > 0)
            .cloned()
            .collect();

        let history_adjacent_named: Vec<PlaneMapCollection> = collections
            .iter()
            .filter(|c| c.role == CollectionPlaneRole::HistoryAdjacent && c.bytes > 0)
            .cloned()
            .collect();

        PlaneMapReport {
            collection_count: collections.len(),
            collections,
            by_role,
            sot_named,
            residue_named,
            legacy_split_named,
            unknown_active_collections,
            history_adjacent_named,
            total_bytes: self.total_bytes,
            keys: None,
            keys_source: "lastdb db inventory (full live scan; not part of status)".to_string(),
        }
    }

    /// Attach dual-read legacy hits (by collection name) onto the plane map.
    #[must_use]
    pub fn to_plane_map_report_with_dual_read(
        &self,
        legacy_hits_by_collection: &[(String, u64)],
    ) -> PlaneMapReport {
        let mut report = self.to_plane_map_report();
        let hits: std::collections::HashMap<&str, u64> = legacy_hits_by_collection
            .iter()
            .map(|(n, h)| (n.as_str(), *h))
            .collect();
        for c in &mut report.collections {
            if let Some(h) = hits.get(c.name.as_str()) {
                c.dual_read_legacy_hits = Some(*h);
            }
        }
        for c in &mut report.sot_named {
            if let Some(h) = hits.get(c.name.as_str()) {
                c.dual_read_legacy_hits = Some(*h);
            }
        }
        for c in &mut report.residue_named {
            if let Some(h) = hits.get(c.name.as_str()) {
                c.dual_read_legacy_hits = Some(*h);
            }
        }
        for c in &mut report.legacy_split_named {
            if let Some(h) = hits.get(c.name.as_str()) {
                c.dual_read_legacy_hits = Some(*h);
            }
        }
        for c in &mut report.unknown_active_collections {
            if let Some(h) = hits.get(c.name.as_str()) {
                c.dual_read_legacy_hits = Some(*h);
            }
        }
        for c in &mut report.history_adjacent_named {
            if let Some(h) = hits.get(c.name.as_str()) {
                c.dual_read_legacy_hits = Some(*h);
            }
        }
        report
    }
}

fn is_legacy_split_collection(name: &str, role: CollectionPlaneRole) -> bool {
    role == CollectionPlaneRole::TipResidue
        || name.starts_with("field_hashrange_")
        || name.starts_with("field_update_order_")
        || name == "field_tip_versions"
        || name == "schema_index"
        || name == "legacy_schema_secondary_index"
        || name == "schema_atom_index"
}

/// Aggregate dual-read legacy hits by plane role using collection attribution.
#[must_use]
pub fn dual_read_hits_by_plane(
    legacy_hits_by_collection: &[(String, u64)],
) -> Vec<DualReadPlaneHit> {
    let mut totals: std::collections::BTreeMap<u8, (CollectionPlaneRole, u64)> =
        std::collections::BTreeMap::new();
    for (name, hits) in legacy_hits_by_collection {
        if *hits == 0 {
            continue;
        }
        let role = classify_collection_plane(name);
        let slot = totals.entry(role.sort_key()).or_insert((role, 0));
        slot.1 = slot.1.saturating_add(*hits);
    }
    totals
        .into_values()
        .map(|(role, legacy_hits)| DualReadPlaneHit {
            role,
            label: role.label().to_string(),
            legacy_hits,
        })
        .collect()
}

impl PlaneBreakdown {
    /// Build from `(collection_name, size_bytes)` pairs (e.g. `du` of data/data/*).
    #[must_use]
    pub fn from_collection_sizes(sizes: impl IntoIterator<Item = (String, u64)>) -> Self {
        let mut entries: Vec<CollectionPlaneEntry> = sizes
            .into_iter()
            .map(|(name, bytes)| {
                let role = classify_collection_plane(&name);
                CollectionPlaneEntry { name, role, bytes }
            })
            .collect();
        entries.sort_by(|a, b| {
            a.role
                .sort_key()
                .cmp(&b.role.sort_key())
                .then_with(|| b.bytes.cmp(&a.bytes))
                .then_with(|| a.name.cmp(&b.name))
        });

        let mut role_totals: std::collections::BTreeMap<u8, (CollectionPlaneRole, u64)> =
            std::collections::BTreeMap::new();
        let mut total_bytes = 0u64;
        for e in &entries {
            total_bytes = total_bytes.saturating_add(e.bytes);
            let slot = role_totals.entry(e.role.sort_key()).or_insert((e.role, 0));
            slot.1 = slot.1.saturating_add(e.bytes);
        }
        let by_role_bytes: Vec<(CollectionPlaneRole, u64)> = role_totals.into_values().collect();

        Self {
            entries,
            by_role_bytes,
            total_bytes,
            total_apparent_bytes: total_bytes,
        }
    }

    /// Same as [`Self::from_collection_sizes`], but carrying the apparent-length
    /// total alongside the allocated total so the gap stays visible.
    #[must_use]
    pub fn from_collection_sizes_with_apparent(
        sizes: impl IntoIterator<Item = (String, u64)>,
        total_apparent_bytes: u64,
    ) -> Self {
        Self {
            total_apparent_bytes,
            ..Self::from_collection_sizes(sizes)
        }
    }
}

/// Walk `store_root/data` (LastStore collections root) and build a plane breakdown.
///
/// `store_root` is the LastStore home root (Mini: `$HOME/data`), whose child
/// `data/` holds collection directories. Missing dirs return an empty breakdown.
pub fn plane_breakdown_for_store_root(store_root: &std::path::Path) -> PlaneBreakdown {
    let collections_root = store_root.join("data");
    let Ok(rd) = std::fs::read_dir(&collections_root) else {
        return PlaneBreakdown::default();
    };
    let mut sizes = Vec::new();
    let mut total_apparent = 0u64;
    for entry in rd.filter_map(Result::ok) {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().into_owned();
        let DirSize {
            allocated,
            apparent,
        } = dir_size_bytes(&path).unwrap_or_default();
        total_apparent = total_apparent.saturating_add(apparent);
        sizes.push((name, allocated));
    }
    PlaneBreakdown::from_collection_sizes_with_apparent(sizes, total_apparent)
}

/// Bytes the filesystem has actually handed to a file, versus the length of the
/// records inside it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DirSize {
    pub allocated: u64,
    pub apparent: u64,
}

/// Bytes the filesystem has actually allocated for `meta`.
///
/// [`std::fs::Metadata::len`] is the file's *apparent* length. It is the wrong
/// number for a capacity gauge: the store holds its hot segment logs open for
/// append, and the filesystem grants those files extents past their written end
/// which it does not release while the descriptor stays open. That space is
/// charged to the volume, so `du` counts it and `len()` cannot see it.
///
/// Measured on the primary 2026-08-03: 7.94 GiB apparent against 9.63 GiB
/// allocated — a 1.69 GiB (17.5%) gap, concentrated in `tips` (+861 MiB) and
/// `atoms` (+683 MiB) at up to 1 MiB per open segment. The write-once `.idx`
/// sidecars beside them, and the `indexes` segments the daemon does not hold
/// open, sit under 1%. Brain:
/// `lastdb-status-underreports-disk-footprint-by-apparent-size-20260803`.
#[must_use]
pub fn allocated_bytes(meta: &std::fs::Metadata) -> u64 {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt as _;
        // st_blocks is defined in 512-byte units regardless of the fs block size.
        meta.blocks().saturating_mul(512)
    }
    #[cfg(not(unix))]
    {
        meta.len()
    }
}

pub(crate) fn dir_size_bytes(path: &std::path::Path) -> std::io::Result<DirSize> {
    let meta = std::fs::symlink_metadata(path)?;
    if meta.file_type().is_symlink() {
        return Ok(DirSize::default());
    }
    if meta.is_file() {
        return Ok(DirSize {
            allocated: allocated_bytes(&meta),
            apparent: meta.len(),
        });
    }
    let mut total = DirSize::default();
    if meta.is_dir() {
        for entry in std::fs::read_dir(path)? {
            let entry = entry?;
            let child = dir_size_bytes(&entry.path()).unwrap_or_default();
            total.allocated = total.allocated.saturating_add(child.allocated);
            total.apparent = total.apparent.saturating_add(child.apparent);
        }
    }
    Ok(total)
}

/// Human-readable status lines for plane breakdown (offline-safe).
#[must_use]
pub fn plane_status_lines(breakdown: &PlaneBreakdown) -> Vec<String> {
    if breakdown.entries.is_empty() {
        return Vec::new();
    }
    let mut lines = vec![format!(
        "Planes: {} collections, {}",
        breakdown.entries.len(),
        format_bytes(breakdown.total_bytes)
    )];
    // Report the allocated total, but never bury the gap: a store whose files
    // hold materially more disk than their records are worth is a capacity fact
    // the operator has to be able to see.
    if let Some(overhang) = allocation_overhang(breakdown) {
        lines.push(format!(
            "  on-disk: {} allocated vs {} in records \
             (+{}, {:.1}% held past record length)",
            format_bytes(breakdown.total_bytes),
            format_bytes(breakdown.total_apparent_bytes),
            format_bytes(overhang.bytes),
            overhang.percent,
        ));
    }
    for (role, bytes) in &breakdown.by_role_bytes {
        if *bytes == 0 {
            continue;
        }
        let names: Vec<&str> = breakdown
            .entries
            .iter()
            .filter(|e| e.role == *role && e.bytes > 0)
            .map(|e| e.name.as_str())
            .collect();
        let sample = if names.len() <= 4 {
            names.join(", ")
        } else {
            format!(
                "{}, … (+{})",
                names[..3].join(", "),
                names.len().saturating_sub(3)
            )
        };
        lines.push(format!(
            "  {}: {}  [{}]",
            role.label(),
            format_bytes(*bytes),
            sample
        ));
    }
    // Explicit aside reclaim note when aside mass is present.
    if breakdown
        .by_role_bytes
        .iter()
        .any(|(r, b)| *r == CollectionPlaneRole::Aside && *b > 0)
    {
        lines.push(
            "  aside reclaim: Tom-gated after backup receipt — never auto-delete \
             (see fold_db/docs/aside-reclaim-procedure.md)"
                .to_string(),
        );
    }
    lines
}

/// Allocated space held past the end of the store's records.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AllocationOverhang {
    pub bytes: u64,
    pub percent: f64,
}

/// The allocated-vs-apparent gap, when it is large enough to be worth reporting.
///
/// Every filesystem rounds to block boundaries, so a small gap is noise. This
/// reports only a gap that is both ≥1% of the store and ≥1 MiB, which is the
/// band where the cause is retained extents rather than rounding.
#[must_use]
pub fn allocation_overhang(breakdown: &PlaneBreakdown) -> Option<AllocationOverhang> {
    const MIN_BYTES: u64 = 1024 * 1024;
    const MIN_PERCENT: f64 = 1.0;

    let bytes = breakdown
        .total_bytes
        .checked_sub(breakdown.total_apparent_bytes)?;
    if bytes < MIN_BYTES || breakdown.total_bytes == 0 {
        return None;
    }
    let percent = 100.0 * (bytes as f64) / (breakdown.total_bytes as f64);
    if percent < MIN_PERCENT {
        return None;
    }
    Some(AllocationOverhang { bytes, percent })
}

fn format_bytes(n: u64) -> String {
    const KIB: f64 = 1024.0;
    const MIB: f64 = 1024.0 * 1024.0;
    const GIB: f64 = 1024.0 * 1024.0 * 1024.0;
    let f = n as f64;
    if f >= GIB {
        format!("{:.2} GiB", f / GIB)
    } else if f >= MIB {
        format!("{:.1} MiB", f / MIB)
    } else if f >= KIB {
        format!("{:.1} KiB", f / KIB)
    } else {
        format!("{n} B")
    }
}
