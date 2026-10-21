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
            Self::ColdSync => 2,
            Self::Ops => 3,
            Self::Aside => 4,
            Self::Unknown => 5,
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
        // Current local reference, schema and storage-meter helpers.
        "schema_index" | "attribution_ledger" | "keep_small" => CollectionPlaneRole::Indexes,

        // ── Cold / sync (K13 Phase-1 same root) ──────────────────────────
        "sync_capture"
        | "sync_cursors"
        | "sync_conflicts"
        | "sync_file_blob_known"
        | "change_feed"
        | "share_delivery_outbox" => CollectionPlaneRole::ColdSync,

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
    /// Unknown active collections with mass. Empty means every non-empty
    /// collection is accounted for in the ideal map.
    ///
    /// Non-empty means the map is incomplete, and it says nothing about whether
    /// the mass is live or dead. Report it as unattributed. Do not present it
    /// beside residue, and never size a reclaim from it.
    #[serde(default)]
    pub unknown_active_collections: Vec<PlaneMapCollection>,
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
}

/// Role rollup row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlaneMapRoleTotal {
    pub role: CollectionPlaneRole,
    pub label: String,
    pub bytes: u64,
    pub collections: Vec<String>,
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

        let unknown_active_collections: Vec<PlaneMapCollection> = collections
            .iter()
            .filter(|c| c.role == CollectionPlaneRole::Unknown && c.bytes > 0)
            .cloned()
            .collect();

        PlaneMapReport {
            collection_count: collections.len(),
            collections,
            by_role,
            sot_named,
            unknown_active_collections,
            total_bytes: self.total_bytes,
            keys: None,
            keys_source: "lastdb db inventory (full live scan; not part of status)".to_string(),
        }
    }
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

mod disk;
pub use disk::*;
