//! Option/report types for residue drains, reclaim, and owner collection compaction.

/// One invocation of the tip-residue drain.
#[derive(Debug, Clone)]
pub struct TipResidueDrainOptions {
    pub legacy_collection: String,
    pub after: Option<String>,
    pub limit: usize,
    pub execute: bool,
    pub drop_empty_collection: bool,
}

/// Report from one paged tip-residue drain invocation.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TipResidueDrainReport {
    pub legacy_collection: String,
    pub dry_run: bool,
    pub keys_scanned: u64,
    pub copied_to_tips: u64,
    pub tips_already_won: u64,
    pub deleted_from_legacy: u64,
    pub skipped: u64,
    pub after: Option<String>,
    pub done: bool,
    pub collection_dropped: bool,
}

/// Which plane family a generic residue drain walks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlaneResidueFamily {
    /// `mk:` / `mh:` / `tv:` → collection `tips`.
    Tip,
    /// `protein:` / `molprot:` / `fldprot:` / `pfq:` → collection `proteins`.
    Protein,
    /// Rebuildable index prefixes → collection `indexes` (never order-log).
    Index,
    /// `conflict:` → collection `tips` (from legacy `sync_conflicts`).
    Conflict,
    /// `mord:` / `moc:` / `mo:` → collection `tips` (from the legacy
    /// `field_update_order_log` / `field_update_order_count` collections).
    /// History-adjacent SoT: copy-then-delete, never rebuild.
    OrderLog,
}

/// Legacy collections the [`PlaneResidueFamily::OrderLog`] drain may read.
pub const ORDER_LOG_LEGACY_COLLECTIONS: &[&str] =
    &["field_update_order_log", "field_update_order_count"];

/// Legacy collection the [`PlaneResidueFamily::Conflict`] drain reads.
pub const SYNC_CONFLICTS_COLLECTION: &str = "sync_conflicts";

/// One invocation of a plane residue drain (protein / index / multi-source).
#[derive(Debug, Clone)]
pub struct PlaneResidueDrainOptions {
    pub family: PlaneResidueFamily,
    /// Collection to scan (legacy split or `tips` when keys still live there).
    pub source_collection: String,
    /// Canonical plane home (`tips` / `proteins` / `indexes`).
    pub target_collection: String,
    /// Optional id prefix for the paged walk (e.g. `protein:`, `mhr:`).
    ///
    /// When draining protein/index residue out of the large `tips` collection,
    /// callers must pass family prefixes — a full tips scan is not viable.
    pub key_prefix: Option<String>,
    pub after: Option<String>,
    pub limit: usize,
    pub execute: bool,
    pub drop_empty_source: bool,
}

/// Report from one paged plane-residue drain invocation.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PlaneResidueDrainReport {
    pub family: String,
    pub source_collection: String,
    pub target_collection: String,
    pub dry_run: bool,
    pub keys_scanned: u64,
    pub copied_to_target: u64,
    pub target_already_won: u64,
    pub deleted_from_source: u64,
    pub skipped: u64,
    pub after: Option<String>,
    pub done: bool,
    pub source_dropped: bool,
}

/// One page of a retired derived-index reclaim.
#[derive(Debug, Clone)]
pub struct IndexResidueReclaimOptions {
    /// Key prefix to walk — required, and must be on
    /// [`RECLAIMABLE_RETIRED_INDEX_PREFIXES`].
    pub prefix: String,
    /// Exclusive cursor: the previous page's last id.
    pub after: Option<String>,
    pub limit: usize,
    pub execute: bool,
}

/// Report from one page of [`LastStoreNamespacedStore::reclaim_retired_index_residue`].
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct IndexResidueReclaimReport {
    pub prefix: String,
    pub dry_run: bool,
    pub keys_scanned: u64,
    pub keys_deleted: u64,
    /// Stored key+value bytes of the deleted rows. Approximate as reclaimed
    /// disk: `LastStore::delete` is an append, so the segment bytes come back
    /// only after `compact --collection indexes`.
    pub bytes_freed_approx: u64,
    /// Rows whose bare key did not actually carry the prefix. Always zero in
    /// practice; non-zero means the scan bound and the key disagreed.
    pub skipped: u64,
    pub after: Option<String>,
    pub done: bool,
}

/// One page of a read-only per-prefix `indexes` measurement.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct IndexPlanePrefixInventory {
    pub prefix: String,
    pub keys: u64,
    pub bytes: u64,
    pub after: Option<String>,
    pub done: bool,
}

/// Collections safe to compact from the owner admin path.
///
/// Mutable planes whose superseded segment bytes are safe to reclaim.
///
/// `tips` joins the catalog collections now that hard-delete/purge removes the
/// corresponding live keys before this owner-only rewrite. `atoms` is allowed
/// only because execute records a durable per-sha retirement sidecar before the
/// rewrite and the next manifest cut emits the matching authenticated receipt.
///
/// `sync_pin_log` joined 2026-08-17. It was the largest single plane on Tom's
/// primary — 21.6 GiB of a 41.5 GiB store, 52% — over 1.65 GB of actual record
/// content, and it was the one big plane with no byte-return path at all.
/// `truncate_confirmed_pin_log_records` already deletes each record once cloud
/// confirms its frontier, but `LastStore::delete` is an **append**
/// (`encode_del` + `append`, `vendor/laststore/src/store.rs`): the superseded
/// record stays in the segment and a delete line is added. Measured live with
/// sync fully healthy and truncation firing, the plane still grew
/// 21,389 -> 21,621 MiB in 91 minutes (~3.7 GiB/day). Compaction is the only
/// verb that returns those bytes.
///
/// Admitting it needs none of what `atoms` needs:
/// - It is sync bookkeeping, not user data and not backup material, so there
///   is no keep-set tamper story and no deletion receipt to sign. Every row is
///   a replica of a mutation already applied to the local store.
/// - It is in `sync::policy::SYNC_INTERNAL_NAMESPACES`, so capture skips it.
///   Compacting it therefore cannot re-amplify into the mutation log the way
///   the 2026-08-08 `tips` compaction did. `compact_pin_log_plane_is_capture_free`
///   pins that pairing.
/// - Compaction only rewrites keys still live in the shard index, and a
///   confirmed-and-deleted row is not one of them.
///
/// `sync_capture_reexport` is admitted on the same reasoning, and it is the
/// purest case of it in the store: the plane holds a crash-safe *intent*
/// marker written immediately before each captured local write and deleted
/// immediately after it. Its live set is the number of writes in flight — a
/// handful — so a steady-state plane is entirely dead records plus their delete
/// lines. Two appends per captured write, kept forever, reached 3.14 GiB on
/// Tom's primary by 2026-08-17 behind a live set of zero, and nothing in the
/// product could return a byte of it: the plane was on neither this list nor
/// `SYNC_INTERNAL_NAMESPACES`.
///
/// `atom_locators` joined 2026-08-17. It clears the capture bar by *key
/// prefix* rather than by namespace, and it is the only plane in the store
/// that does: `atom_locators` holds exactly the `aloc:` rows
/// (`MAIN_KEY_PREFIX_COLLECTIONS`), and `aloc:` is the sole entry in
/// `sync::policy::CAPTURE_SKIP_MAIN_KEY_PREFIXES`. Capture therefore already
/// omits every row this plane can contain, so compacting it cannot re-enter
/// the mutation log — the property that makes `sync_pin_log` safe, reached by
/// the other route. `sync::policy::CAPTURE_SKIP_BY_KEY_PREFIX_PLANES` pins the
/// pairing so removing the prefix breaks the build rather than silently making
/// this compaction an amplifier.
///
/// It needed admitting because a locator row is rewritten whenever its atom is
/// re-addressed, `LastStore::delete`/overwrite is an append, and nothing
/// prunes the superseded records: measured on Tom's primary 2026-08-17 the
/// plane was 1.45 GiB — the fourth-largest collection in an 18.79 GiB store —
/// behind a live set of one small row per live atom (~1.55 M). Before this
/// entry `lastdb db compact --collection atom_locators` answered "not on the
/// compact allowlist", so the plane had no byte-return path of any kind.
///
/// Nothing is lost if a locator row is dropped: the same doc comment that
/// admits `aloc:` to the capture skip list states the reconstruction contract
/// — a partition-prefixed `atom:` key carries both the partition and the atom
/// UUID, so the locator is derivable from the atom plane.
///
/// `cas_blobs` joined 2026-10-08: a blob delete is an append, so the 568.8 MiB
/// plane on the primary could not return a byte. Its rows are captured and
/// backed up as `Mutable` chunks, so it compacts under cloud isolation like
/// `proteins`, and only by owner command (no self-compactor, on purpose).
pub const COMPACT_ALLOWLIST: &[&str] = &[
    "schemas",
    "schema_states",
    "schema_index",
    "tips",
    // Captured node-level SOT. Like tips, owner physical compact is
    // capture-neutral through the outer capture wrapper; ordinary metadata
    // puts remain captured because there is no rebuild contract.
    "metadata",
    "idempotency",
    "change_feed",
    "sync_pin_log",
    "sync_capture_reexport",
    // Keep-small meter snapshot: one live key, every put a whole-map rewrite,
    // so ~100% of the plane is dead between compactions. Capture-free.
    "keep_small",
    "atoms",
    "atom_locators",
    "atom_ref_edges",
    "atom_ref_edges_v2",
    "molecule_ref_edges",
    "blob_ref_edges",
    // Captured SOT (~364 MiB live). Compact only under cloud isolation —
    // protein: puts are not capture-skipped (no reconstruct contract).
    "proteins",
    // History-adjacent captured SOT. compact-order-log deletes are appends;
    // these two physical compacts return the bytes. Isolation required.
    "field_update_order_log",
    "field_update_order_count",
    // Dead index residue; every prefix is capture-skipped. Capture-free.
    "indexes",
    // Local file-blob rows. Captured SOT, backed up (Mutable). Isolation required.
    "cas_blobs",
];

/// Captured user-state planes on [`COMPACT_ALLOWLIST`] that still require
/// Cloud Sync isolation while they compact.
///
/// Execute rewrites every live row. If Cloud Sync is on, the owner compact
/// command pauses it for the rewrite and restores it afterwards so backup
/// upload and mutation-log capture do not race the segment swap. Capture-free
/// allowlist entries (`sync_pin_log`, `atom_locators`, …) do not need this.
/// The named captured exceptions `tips` and `metadata` also do not: their
/// owner physical-compact path is regression-proven capture-neutral through
/// `MutationLogCaptureNamespacedStore::compact_collection`.
///
/// Keep this in lockstep with `compaction_is_capture_free` — the policy test
/// `compact_isolation_matches_capture_free` fails if they drift.
pub fn compact_requires_cloud_isolation(collection: &str) -> bool {
    matches!(
        collection,
        "atoms"
            | "schemas"
            | "schema_states"
            | "proteins"
            | "field_update_order_log"
            | "field_update_order_count"
            | "cas_blobs"
    )
}

/// Owner compact of one allowlisted LastStore collection.
#[derive(Debug, Clone)]
pub struct CollectionCompactOptions {
    pub collection: String,
    /// When false, run LastStore compact (rewrite live keys, drop dead segs).
    /// When true (default), report sizes only.
    pub dry_run: bool,
    /// When true, after a successful atoms compact, also stamp committed
    /// successor-history SHAs into pending purged retirements. Default false
    /// so a Mini built from the helper PR does not fire.
    pub seed_committed_history: bool,
}

/// One owner drop of a dead hash group (`lastdb db reclaim-keep-small-legacy`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeadHashGroupDropOptions {
    /// Physical collection that owns the group.
    pub collection: String,
    /// The only id the group may hold. The store resolves the group from it
    /// and refuses if the group's sidecar records any other id.
    pub expected_only_id: String,
    /// Measure and verify only; the directory is untouched.
    pub dry_run: bool,
}

/// Report from [`NamespacedStore::drop_dead_hash_group`].
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DeadHashGroupDropReport {
    pub collection: String,
    pub expected_only_id: String,
    pub dry_run: bool,
    pub shard: u16,
    pub group: u32,
    /// `data/<collection>/<shard>/g/<group>` as it was before the drop.
    pub dir: String,
    /// `*.seg` files the group held.
    pub segments: u64,
    /// Bytes on disk under the group directory (returned to the OS on execute).
    pub on_disk_bytes: u64,
    /// Ids the sidecar recorded — at most the expected one.
    pub ids: Vec<String>,
    /// True only when the directory was removed.
    pub dropped: bool,
    /// True when no group directory existed at `dir`: an earlier drop already
    /// returned the bytes. The reclaim is idempotent, so this is a success
    /// with zero bytes, not a refusal (papercut
    /// `papercut-lastdb-reclaim-keep-small-legacy-409-when-group-already-absent-20260922`).
    /// Absent on rows written before this field shipped.
    #[serde(default)]
    pub already_absent: bool,
}

/// Report from [`LastStoreNamespacedStore::compact_collection_admin`].
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CollectionCompactReport {
    pub collection: String,
    pub dry_run: bool,
    pub live_keys: u64,
    pub bytes_before: u64,
    pub bytes_after: Option<u64>,
    /// The underlying LastStore COLLECTION POLICY
    /// (`LastStoreOptions::collection_policy`), which is NOT this verb's
    /// answer. `atoms` carries the policy — the ordinary store-level compact
    /// path refuses it — while this admin verb compacts it through
    /// `compact_atoms_with_retirement_provenance`. Read
    /// [`Self::compactable_here`] to learn whether the plane has a reclaim
    /// path; this field only says which store-level policy it sits under.
    ///
    /// The name is on the wire and in every rollup already written, so it
    /// stays. What changed is that a `true` here is now always accompanied by
    /// a [`Self::skipped_reason`] saying which of the two questions it
    /// answered.
    pub never_compact: bool,
    /// Whether THIS verb can compact the plane — the question an operator is
    /// actually asking when they read the report to decide where to reclaim.
    ///
    /// `false` only on a genuine refusal (off the allowlist, or a
    /// never-compact policy this verb does not exempt). Defaults to `false`
    /// when absent so a rollup row written before this field shipped does not
    /// deserialize into a claim nobody made.
    #[serde(default)]
    pub compactable_here: bool,
    pub executed: bool,
    pub skipped_reason: Option<String>,
    /// Record bytes live ids still address before the rewrite, from the
    /// store's residue counters. `None` when the backend cannot measure it.
    #[serde(default)]
    pub live_bytes: Option<u64>,
    /// Dead record bytes before the rewrite — superseded puts, deleted puts,
    /// delete markers. On a dry run this is the reason to execute; on an
    /// execute it is what the rewrite returned.
    #[serde(default)]
    pub dead_bytes: Option<u64>,
    /// On-disk bytes the residue probe could not classify.
    #[serde(default)]
    pub residue_unknown_bytes: Option<u64>,
}
