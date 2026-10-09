//! Dual-read residue counters for the logical `main` LastStore adapter.
//!
//! ## Why this exists
//!
//! Tip-family keys (`mk:` / `mh:` / `tv:`) already **write-one** into collection
//! `tips`. Live gets for those prefixes are **write-target only** after the
//! tip-residue dual-read delete (`mk` 2026-07-31; `mh`/`tv` 2026-08-05):
//! `field_tip_headers` / `field_tip_versions` are no longer consulted on the
//! logical-main path.
//!
//! The per-prefix and per-tip-collection counters that tracked those arms were
//! **deleted 2026-08-07** — first sunset through `sop-migration-code-sunset`.
//! They were structurally always-zero (the code no longer consults those
//! collections on any home, not just ours), and a CoW proof against a clone of
//! the real primary found both collections held **zero live rows**, so the
//! drain had nothing to copy and the delete stranded nothing. Evidence:
//! `docs/security/tip-family-dual-read-sunset.md`.
//!
//! `legacy_hits_by_collection` answers "what residue is still served" without a
//! hand-maintained bucket per collection, which is why the buckets were
//! removable at all.
//!
//! Other planes (proteins → tips, order-log splits, …) may still dual-read;
//! counters name that residue for operators.
//!
//! Counters are process-global (node lifetime). They reset on process restart —
//! that matches "is residue still being served under live traffic?" rather than
//! a durable inventory.
//!
//! ## What `legacy_hits` means (and what it must never mean)
//!
//! `legacy_hits` counts **residue that is expected to reach zero** — the number
//! a cutover reads to decide whether its fallback arm can be deleted. It is not
//! "every read that fell through to a non-target collection", because some
//! planes are consulted **by design and forever**:
//! [`CollectionPlaneRole::HistoryAdjacent`] is append-only mass that is not
//! rebuildable from tips, so a fallthrough there is architecture, not debt.
//!
//! Counting those as legacy is not a cosmetic error. Until 2026-08-06 this
//! counter attributed **790,172 hits — 34% of all gets** on the primary to
//! `field_update_order_log` / `field_update_order_count`, both
//! `HistoryAdjacent`. Any rule of the form "delete the fallback when
//! `legacy_hits` reaches zero" therefore never fired, and an operator glancing
//! at status reasonably concluded a third of reads were still served off a
//! legacy path. Meanwhile the genuine signal — the tip-family arms sitting at a
//! true zero across 2.3M gets, and so actually deletable — was invisible next
//! to that headline.
//!
//! By-design fallthroughs are counted in `by_design_hits` and attributed by
//! collection, so nothing is hidden; they are simply not called legacy.
//! `legacy_hits + by_design_hits == ` every fallthrough, which
//! [`DualReadMetricsSnapshot::fallthrough_hits`] reports.
//!
//! See `brain get papercut-dual-read-legacy-hits-counts-by-design-planes` and
//! `sop-migration-code-sunset` (this counter is a `class=residue` probe).

use crate::mini_cutover::plane_roles::{classify_collection_plane, CollectionPlaneRole};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};

/// Process-global dual-read counters. See module docs.
static DUAL_READ: DualReadMetrics = DualReadMetrics::new();

// The counters are process-global, so regular unit tests elsewhere in the
// crate can record a real dual-read while this module is asserting a reset
// window.  Keep those writers out of an exclusive metrics test window.

struct DualReadMetrics {
    /// Point gets that consulted the dual-read chain (logical main only).
    gets: AtomicU64,
    /// Resolved on the write-target collection (candidate index 0).
    target_hits: AtomicU64,
    /// Fell through to a non-target collection that is **residue** — a plane
    /// expected to reach zero. This is the number a cutover probe reads.
    legacy_hits: AtomicU64,
    /// Fell through to a non-target collection that is consulted **by design**
    /// and will never reach zero (see [`is_by_design_fallthrough`]). Reported
    /// so the traffic is visible, never counted as debt.
    by_design_hits: AtomicU64,
    /// Misses (no collection held the key).
    misses: AtomicU64,

    /// Legacy hits attributed to the collection that actually served them,
    /// indexed by [`ATTRIBUTED_COLLECTIONS`]. See that constant for why.
    by_collection: [AtomicU64; ATTRIBUTED_COLLECTIONS.len()],
    /// By-design fallthroughs attributed the same way, so excluding them from
    /// `legacy_hits` hides nothing — the traffic is still named.
    by_design_by_collection: [AtomicU64; ATTRIBUTED_COLLECTIONS.len()],
}

/// Is a fallthrough onto `collection` **by design** rather than residue?
///
/// True only for planes that are consulted forever by construction. Today that
/// is [`CollectionPlaneRole::HistoryAdjacent`] — append-only mass that is "not
/// rebuildable-from-tips without a proof", so no drain will ever empty it.
///
/// Two deliberate choices:
///
/// - **Only `HistoryAdjacent` is excluded.** Every other role — including
///   `Unknown` and an unclassifiable `None` collection — stays counted as
///   legacy. Under-excluding leaves a cutover looking not-yet-done, which
///   costs a re-measure; over-excluding hides live residue and authorizes
///   deleting a fallback that is still serving reads. Only one of those is
///   recoverable, so the predicate is deliberately narrow.
/// - **The drain list wins over the role.** `field_tip_versions` classifies
///   `HistoryAdjacent` but is named in [`TIP_RESIDUE_LEGACY_COLLECTIONS`], so
///   an explicit drain is expected to empty it. Drain-listed collections are
///   residue regardless of plane role.
#[must_use]
pub fn is_by_design_fallthrough(collection: &str) -> bool {
    if TIP_RESIDUE_LEGACY_COLLECTIONS.contains(&collection)
        || INDEX_RESIDUE_LEGACY_COLLECTIONS.contains(&collection)
    {
        return false;
    }
    matches!(
        classify_collection_plane(collection),
        CollectionPlaneRole::HistoryAdjacent
    )
}

/// Every collection a legacy fallthrough can resolve on, in report order.
///
/// The retired `field_tip*` counters answered "is the tip-family residue
/// drained yet", which was the only question this instrument was built for. It
/// left every other legacy hit in one undifferentiated bucket with no
/// breakdown, and dropped the serving collection on the floor entirely. An
/// operator watching a home sit at 59% legacy hits could therefore see *that*
/// residue was being served but had no way to learn *what* — which is exactly
/// the position the atom partition-prefix cutover was left in: unable to close,
/// because "the residual is entirely in the 'other' bucket" is not a finding.
///
/// Naming the serving collection turns that into a one-line answer.
pub const ATTRIBUTED_COLLECTIONS: &[&str] = &[
    "tips",
    "atoms",
    "atom_locators",
    "proteins",
    "indexes",
    "field_tip_headers",
    "field_tip_versions",
    "field_update_order_log",
    "field_update_order_count",
    "field_hashrange_page_index",
    "field_hashrange_hash_index",
    "field_hashrange_complete",
    "mutation_history",
    "legacy_blob_refs",
    "schema_atom_index",
    "legacy_schema_secondary_index",
    "sync_conflicts",
    "main",
];

impl DualReadMetrics {
    const fn new() -> Self {
        Self {
            gets: AtomicU64::new(0),
            target_hits: AtomicU64::new(0),
            legacy_hits: AtomicU64::new(0),
            by_design_hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            by_collection: [const { AtomicU64::new(0) }; ATTRIBUTED_COLLECTIONS.len()],
            by_design_by_collection: [const { AtomicU64::new(0) }; ATTRIBUTED_COLLECTIONS.len()],
        }
    }

    fn snapshot(&self) -> DualReadMetricsSnapshot {
        DualReadMetricsSnapshot {
            gets: self.gets.load(Ordering::Relaxed),
            target_hits: self.target_hits.load(Ordering::Relaxed),
            legacy_hits: self.legacy_hits.load(Ordering::Relaxed),
            by_design_hits: self.by_design_hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            legacy_hits_by_collection: attribution_vec(&self.by_collection),
            by_design_hits_by_collection: attribution_vec(&self.by_design_by_collection),
        }
    }

    fn reset(&self) {
        for c in self
            .by_collection
            .iter()
            .chain(&self.by_design_by_collection)
        {
            c.store(0, Ordering::Relaxed);
        }
        for c in [
            &self.gets,
            &self.target_hits,
            &self.legacy_hits,
            &self.by_design_hits,
            &self.misses,
        ] {
            c.store(0, Ordering::Relaxed);
        }
    }
}

/// Collapse one attribution array to non-zero `(collection, hits)` pairs in
/// [`ATTRIBUTED_COLLECTIONS`] order.
fn attribution_vec(counters: &[AtomicU64; ATTRIBUTED_COLLECTIONS.len()]) -> Vec<(String, u64)> {
    ATTRIBUTED_COLLECTIONS
        .iter()
        .zip(counters.iter())
        .filter_map(|(name, counter)| {
            let hits = counter.load(Ordering::Relaxed);
            (hits > 0).then(|| ((*name).to_string(), hits))
        })
        .collect()
}

/// Serializable dual-read counters for `/api/status` and unit tests.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DualReadMetricsSnapshot {
    pub gets: u64,
    pub target_hits: u64,
    /// Residue fallthroughs — the number a cutover probe reads. Excludes
    /// by-design planes; see the module docs.
    pub legacy_hits: u64,
    /// Fallthroughs onto planes read by design forever. Never debt.
    #[serde(default)]
    pub by_design_hits: u64,
    pub misses: u64,
    /// Legacy hits by the collection that served them, non-zero entries only,
    /// in [`ATTRIBUTED_COLLECTIONS`] order. This is what names the residue.
    #[serde(default)]
    pub legacy_hits_by_collection: Vec<(String, u64)>,
    /// By-design fallthroughs by serving collection, same order and shape.
    /// Excluded from `legacy_hits`, reported here so nothing is hidden.
    #[serde(default)]
    pub by_design_hits_by_collection: Vec<(String, u64)>,
}

impl DualReadMetricsSnapshot {
    /// The collection serving the most legacy hits, if any — the one line an
    /// operator needs to decide whether a cutover can close.
    #[must_use]
    pub fn top_legacy_collection(&self) -> Option<(&str, u64)> {
        self.legacy_hits_by_collection
            .iter()
            .max_by_key(|(_, hits)| *hits)
            .map(|(name, hits)| (name.as_str(), *hits))
    }

    /// The by-design plane serving the most reads, if any. Named so an
    /// operator can tell "34% of gets fall through" from "34% is debt".
    #[must_use]
    pub fn top_by_design_collection(&self) -> Option<(&str, u64)> {
        self.by_design_hits_by_collection
            .iter()
            .max_by_key(|(_, hits)| *hits)
            .map(|(name, hits)| (name.as_str(), *hits))
    }

    /// Every read that resolved off the write target, debt or not.
    ///
    /// `target_hits + fallthrough_hits() + misses == gets` for any window with
    /// no concurrent traffic — the invariant that proves the residue/by-design
    /// split loses nothing.
    #[must_use]
    pub fn fallthrough_hits(&self) -> u64 {
        self.legacy_hits.saturating_add(self.by_design_hits)
    }
}

/// Snapshot process-global dual-read counters.
#[must_use]
pub fn dual_read_metrics_snapshot() -> DualReadMetricsSnapshot {
    DUAL_READ.snapshot()
}

/// Reset process-global dual-read counters (tests).
pub fn dual_read_metrics_reset() {
    DUAL_READ.reset();
}

/// Record one dual-read resolution for a logical-main point get.
///
/// `candidate_index` is 0 for the write-target collection, >0 for any
/// fallthrough. `collection` is the collection that produced the hit (ignored
/// on miss — pass `None` for miss).
///
/// A fallthrough onto a by-design plane ([`is_by_design_fallthrough`]) is
/// attributed and counted in `by_design_hits`, then returns — it must not
/// reach `legacy_hits`, which means "residue still being served". An unnamed
/// collection (`None`) counts as residue: not knowing what served a read is
/// not evidence that it was by design.
///
/// Takes no key: the tip-family prefix buckets that needed one were deleted
/// with the tip-family sunset (2026-08-07), which also let the caller stop
/// cloning every key into a `Vec` on the logical-main get path.
pub(crate) fn record_dual_read_get(candidate_index: Option<usize>, collection: Option<&str>) {
    record_dual_read_get_inner(candidate_index, collection);
}

fn record_dual_read_get_inner(candidate_index: Option<usize>, collection: Option<&str>) {
    DUAL_READ.gets.fetch_add(1, Ordering::Relaxed);
    let Some(idx) = candidate_index else {
        DUAL_READ.misses.fetch_add(1, Ordering::Relaxed);
        return;
    };
    if idx == 0 {
        DUAL_READ.target_hits.fetch_add(1, Ordering::Relaxed);
        return;
    }

    if collection.is_some_and(is_by_design_fallthrough) {
        DUAL_READ.by_design_hits.fetch_add(1, Ordering::Relaxed);
        if let Some(idx) = collection.and_then(attributed_collection_index) {
            DUAL_READ.by_design_by_collection[idx].fetch_add(1, Ordering::Relaxed);
        }
        return;
    }

    DUAL_READ.legacy_hits.fetch_add(1, Ordering::Relaxed);

    // Attribute every legacy hit, not just the three tip-plane ones. A hit on a
    // collection missing from the table is silently unattributed — the table is
    // the report, so add the collection there rather than widening this arm.
    if let Some(idx) = collection.and_then(attributed_collection_index) {
        DUAL_READ.by_collection[idx].fetch_add(1, Ordering::Relaxed);
    }
}

fn attributed_collection_index(collection: &str) -> Option<usize> {
    ATTRIBUTED_COLLECTIONS
        .iter()
        .position(|name| *name == collection)
}

// ── Tip residue copy-verify (compact path skeleton) ─────────────────────────

/// Legacy tip-plane collections that explicit `drain-tip-residue` may still
/// copy-verify into write-target `tips`.
///
/// Live dual-read of these collections is **deleted** (2026-08-05). Cold homes
/// and operator reclaim still name them here. `field_tips` was removed from
/// live dual-read earlier (2026-07-31) and is not drain-listed (reclaim is a
/// separate aside path).
///
/// **Reclassified `class=format` 2026-08-07.** The node-local half of this
/// sunset is done: the always-zero counters were deleted, and a CoW proof
/// against a clone of the real primary found both collections holding **zero
/// live rows** (`drain-tip-residue` scanned 0, `--drop-empty-collection`
/// reclaimed 12.3 MB). But Mini ships publicly, so a home written by an older
/// Mini may still hold rows and is **unobservable from here** — this drain is
/// the only reclaim path for it. That makes the remaining tooling deletable
/// only against a declared upgrade floor, not by measurement.
///
/// LEGACY-SUNSET: tip-family-drain-tooling | shape=field_tip_headers+field_tip_versions cold collections + drain-tip-residue path
///   | class=format
///   | probe=none-possible — third-party homes are unobservable; do not wait for a counter
///   | trigger=floor>=<first release refusing homes below the tip-family write-one flip>
///   | owner=decisions-log 2026-08-06 upgrade-floor decision
/// Sunset note: `docs/security/tip-family-dual-read-sunset.md`.
pub const TIP_RESIDUE_LEGACY_COLLECTIONS: &[&str] = &["field_tip_headers", "field_tip_versions"];

/// Key prefixes whose write target is `tips` and whose legacy split collections
/// are [`TIP_RESIDUE_LEGACY_COLLECTIONS`] (drain-only; not live dual-read).
///
/// LEGACY-SUNSET: tip-family-drain-tooling | shape=mh:+tv: tip-residue key prefixes for drain-only reclaim
///   | class=format
///   | probe=same as TIP_RESIDUE_LEGACY_COLLECTIONS
///   | trigger=floor>=<first release refusing homes below the tip-family write-one flip>
///   | owner=decisions-log 2026-08-06 upgrade-floor decision
pub const TIP_RESIDUE_KEY_PREFIXES: &[&str] = &["mh:", "tv:"];

/// Decision for one key during tip-residue copy-verify.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TipResidueCopyAction {
    /// Legacy-only: copy value into tips (write target).
    CopyToTips,
    /// Both present: tips wins; optional delete of legacy (converging path).
    TipsWins,
    /// Already only on tips — nothing to do.
    AlreadyOnTips,
    /// Key is not tip-family (skip).
    SkipNotTipFamily,
}

/// Pure policy for tip residue compact (design PR-5).
///
/// - Write target is always `tips` (already shipped write-one).
/// - If both homes hold the key, **tips wins** (write target shadows legacy).
/// - legacy-only keys must be copied before the source collection is removed.
#[must_use]
pub fn classify_tip_residue_copy(
    key: &[u8],
    tips_has: bool,
    legacy_has: bool,
) -> TipResidueCopyAction {
    if !key_has_any_prefix(key, TIP_RESIDUE_KEY_PREFIXES) {
        return TipResidueCopyAction::SkipNotTipFamily;
    }
    match (tips_has, legacy_has) {
        (true, true) => TipResidueCopyAction::TipsWins,
        (false, true) => TipResidueCopyAction::CopyToTips,
        (true | false, false) => TipResidueCopyAction::AlreadyOnTips,
    }
}

/// Result of applying one page of copy-verify against an open store.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TipResidueCopyPageReport {
    pub legacy_collection: String,
    pub keys_scanned: u64,
    pub copied_to_tips: u64,
    pub tips_already_won: u64,
    pub skipped: u64,
}

/// Apply copy-verify policy for one legacy tip-residue collection.
///
/// For each key in `legacy_rows`:
/// - if tips lacks it → put into tips (copy)
/// - if tips has it → leave tips value (tips wins); do **not** overwrite
///
/// Does not delete legacy rows (converging writes / a later GC pass do that).
/// Safe to re-run; never invents dual-write of new traffic.
pub fn apply_tip_residue_copy_page(
    legacy_collection: &str,
    legacy_rows: &[(Vec<u8>, Vec<u8>)],
    tips_has: impl Fn(&[u8]) -> bool,
    mut put_tips: impl FnMut(&[u8], &[u8]),
) -> TipResidueCopyPageReport {
    let mut report = TipResidueCopyPageReport {
        legacy_collection: legacy_collection.to_string(),
        ..Default::default()
    };
    for (key, value) in legacy_rows {
        report.keys_scanned = report.keys_scanned.saturating_add(1);
        let on_tips = tips_has(key);
        match classify_tip_residue_copy(key, on_tips, true) {
            TipResidueCopyAction::CopyToTips => {
                put_tips(key, value);
                report.copied_to_tips = report.copied_to_tips.saturating_add(1);
            }
            TipResidueCopyAction::TipsWins => {
                report.tips_already_won = report.tips_already_won.saturating_add(1);
            }
            TipResidueCopyAction::AlreadyOnTips | TipResidueCopyAction::SkipNotTipFamily => {
                report.skipped = report.skipped.saturating_add(1);
            }
        }
    }
    report
}

// ── Protein / index plane residue (copy-verify into SOT homes) ───────────────

/// Protein-family key prefixes written to collection `proteins` (ideal map).
///
/// Legacy homes may still hold these under `tips` (unknown-prefix fallback).
/// Dual-read is proteins → tips; this drain copies tips-only rows into proteins.
pub const PROTEIN_FAMILY_KEY_PREFIXES: &[&str] = &["protein:", "molprot:", "fldprot:", "pfq:"];

/// Rebuildable index key prefixes written to collection `indexes` (K18).
///
/// Live logical-main reads are indexes → tips for retired index-plane splits
/// (`field_hashrange_*` and `legacy_schema_secondary_index` / `schemaidx:`);
/// those legacy collections remain explicit CoW drain sources only. Order-log
/// prefixes (`mord:` / `moc:` / `mo:`) are **not** included — they are
/// history-adjacent.
pub const INDEX_RESIDUE_KEY_PREFIXES: &[&str] = &[
    "mhr:",
    "mhk:",
    "mhi:",
    "schema_atoms:",
    "idx:",
    "schemaidx:",
];

/// Legacy split collections that explicit index-residue drains can copy into
/// the `indexes` write target.
pub const INDEX_RESIDUE_LEGACY_COLLECTIONS: &[&str] = &[
    "field_hashrange_page_index",
    "field_hashrange_hash_index",
    "field_hashrange_complete",
    "schema_atom_index",
    "legacy_schema_secondary_index",
];

/// Order-log / mutation-chain key prefixes — **history-adjacent**, not rebuildable.
///
/// Write target today remains `tips` for some of these keys; durable mass also
/// lives under the order-log collections below. Never treat as index residue.
pub const ORDER_LOG_KEY_PREFIXES: &[&str] = &["mord:", "moc:", "mo:"];

/// On-disk collections holding history-adjacent order-log mass (plane map role).
pub const ORDER_LOG_COLLECTIONS: &[&str] = &["field_update_order_log", "field_update_order_count"];

/// Decision for one key during protein/index plane residue copy-verify.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlaneResidueCopyAction {
    /// Legacy-only: copy value into the plane target collection.
    CopyToTarget,
    /// Both present: target wins; optional delete of legacy/source row.
    TargetWins,
    /// Already only on target — nothing to do.
    AlreadyOnTarget,
    /// Key is not in this plane's family (skip).
    SkipWrongFamily,
}

fn bare_key_str(key: &[u8]) -> Option<&str> {
    let s = std::str::from_utf8(key).ok()?;
    Some(super::strip_org_storage_prefix(s))
}

fn key_has_any_prefix(key: &[u8], prefixes: &[&str]) -> bool {
    let Some(bare) = bare_key_str(key) else {
        return false;
    };
    prefixes.iter().any(|p| bare.starts_with(p))
}

/// Pure policy: protein-family keys only; `proteins` is SOT when both present.
#[must_use]
pub fn classify_protein_residue_copy(
    key: &[u8],
    target_has: bool,
    source_has: bool,
) -> PlaneResidueCopyAction {
    if !key_has_any_prefix(key, PROTEIN_FAMILY_KEY_PREFIXES) {
        return PlaneResidueCopyAction::SkipWrongFamily;
    }
    match (target_has, source_has) {
        (true, true) => PlaneResidueCopyAction::TargetWins,
        (false, true) => PlaneResidueCopyAction::CopyToTarget,
        (true | false, false) => PlaneResidueCopyAction::AlreadyOnTarget,
    }
}

/// Pure policy: rebuildable index keys only; never order-log / tip families.
#[must_use]
pub fn classify_index_residue_copy(
    key: &[u8],
    target_has: bool,
    source_has: bool,
) -> PlaneResidueCopyAction {
    if key_has_any_prefix(key, ORDER_LOG_KEY_PREFIXES) {
        return PlaneResidueCopyAction::SkipWrongFamily;
    }
    if !key_has_any_prefix(key, INDEX_RESIDUE_KEY_PREFIXES) {
        return PlaneResidueCopyAction::SkipWrongFamily;
    }
    match (target_has, source_has) {
        (true, true) => PlaneResidueCopyAction::TargetWins,
        (false, true) => PlaneResidueCopyAction::CopyToTarget,
        (true | false, false) => PlaneResidueCopyAction::AlreadyOnTarget,
    }
}

/// Conflict-annotation keys. Canonical home is `tips`
/// (`classify_main_key`); rows written before the main-plane cutover still sit
/// in the legacy `sync_conflicts` collection, where every conflict-annotation
/// read reaches them only by falling through the candidate chain — measured at
/// 87% of all legacy dual-read hits on the live primary (2026-07-30).
pub const CONFLICT_KEY_PREFIXES: &[&str] = &["conflict:"];

/// Pure policy: conflict-annotation keys only; `tips` wins when both present.
///
/// This is a **copy**, never a rebuild — a conflict row is durable evidence
/// (which merge changed local state), so a row that exists nowhere else must
/// move, not be regenerated.
#[must_use]
pub fn classify_conflict_residue_copy(
    key: &[u8],
    target_has: bool,
    source_has: bool,
) -> PlaneResidueCopyAction {
    if !key_has_any_prefix(key, CONFLICT_KEY_PREFIXES) {
        return PlaneResidueCopyAction::SkipWrongFamily;
    }
    match (target_has, source_has) {
        (true, true) => PlaneResidueCopyAction::TargetWins,
        (false, true) => PlaneResidueCopyAction::CopyToTarget,
        (true | false, false) => PlaneResidueCopyAction::AlreadyOnTarget,
    }
}

/// Pure policy: order-log keys only (`mord:` / `moc:` / `mo:`); `tips` wins
/// when both present.
///
/// Order-log rows are history-adjacent source of truth — the exact family the
/// index drain refuses to touch ([`classify_index_residue_copy`]) because they
/// cannot be rebuilt. Moving them to their canonical `tips` home is therefore
/// copy-then-delete only, same crash ordering as every other plane drain.
#[must_use]
pub fn classify_order_log_residue_copy(
    key: &[u8],
    target_has: bool,
    source_has: bool,
) -> PlaneResidueCopyAction {
    if !key_has_any_prefix(key, ORDER_LOG_KEY_PREFIXES) {
        return PlaneResidueCopyAction::SkipWrongFamily;
    }
    match (target_has, source_has) {
        (true, true) => PlaneResidueCopyAction::TargetWins,
        (false, true) => PlaneResidueCopyAction::CopyToTarget,
        (true | false, false) => PlaneResidueCopyAction::AlreadyOnTarget,
    }
}
