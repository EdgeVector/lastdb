//! Locator-only tip probe options, strata, and the population report.

use super::*;

/// Default tip sample size for [`AtomStore::probe_locator_only_population`].
///
/// This is a *classification* budget. It buys
/// `DEFAULT_LOCATOR_ONLY_PROBE_MAX_TIPS / LOCATOR_ONLY_PROBE_TARGET_QUOTA`
/// independent positions in `mk:` — see [`locator_only_probe_window_count`],
/// which is the number that decides what this gauge can see.
///
/// It was 512 and unstratified until 2026-08-09. On Tom's primary that combination
/// reported **0‰** against a true population rate of **250‰**, because the walk
/// always classified the same first 512 keys — see
/// `papercut-lastdb-locator-only-probe-samples-the-head-of-the-keyspace-and-calls-it-a-rate`.
///
/// The 2026-08-09 fix replaced one head read with 17 head reads and kept the
/// budget buying **depth** at those 17 fixed positions. On 2026-08-18 that
/// reported `dangling = 0‰` against a full walk's **6.61‰** (14,295 tips), and
/// raising the budget 16x moved it 0 → 0, because the extra draws went deeper
/// at the same 17 places. Since 2026-09-07 the budget buys **positions** —
/// `locator_only_probe_window_count` — so raising it narrows what can hide.
/// See `papercut-lastdb-stratified-probe-is-17-head-reads-and-reports-zero-dangling-against-14295`.
pub const DEFAULT_LOCATOR_ONLY_PROBE_MAX_TIPS: usize = 4096;

/// Tips classified per window.
///
/// The budget is `positions * quota`, so this trades detection against cost:
/// a smaller quota buys more positions for the same number of classifications,
/// at one range page and one batched existence probe per position.
///
/// 8 is chosen so the default 4096-tip budget buys 512 positions, whose 95%
/// detection floor is 6‰ — just under the 6.61‰ the primary actually carried
/// when this gauge was reading zero. Below ~4 the per-window round trips start
/// to dominate a batch that exists to amortise them.
pub const LOCATOR_ONLY_PROBE_TARGET_QUOTA: usize = 8;

/// Number of contiguous sub-ranges the locator-only probe splits `mk:` into at
/// the default budget. Retained as the documented shape of a default probe;
/// the live count is [`locator_only_probe_window_count`] of `max_tips`.
pub const LOCATOR_ONLY_PROBE_STRATA: usize =
    DEFAULT_LOCATOR_ONLY_PROBE_MAX_TIPS / LOCATOR_ONLY_PROBE_TARGET_QUOTA;

/// How many disjoint `mk:` windows a `max_tips` budget is spread over.
///
/// **This is the number that decides what the gauge can see.** The classes this
/// probe looks for cluster by molecule id — molecule-id order is also migration
/// order — so a damaged class is a contiguous band, and a sample misses a band
/// of relative width `w` with probability `(1 - w)^positions`. Positions, not
/// draws, is the exponent; 4096 draws at 17 places see less than 512 draws at
/// 512 places.
pub(crate) fn locator_only_probe_window_count(max_tips: usize) -> usize {
    max_tips
        .div_ceil(LOCATOR_ONLY_PROBE_TARGET_QUOTA)
        .clamp(1, max_tips.max(1))
}

/// The narrowest contiguous band, in per mille of `mk:`, that `positions`
/// evenly-spaced draws detect with 95% confidence: `1 - 0.05^(1/positions)`.
///
/// Reported next to the rates so a `0` is readable as "nothing wider than this
/// floor is here" rather than as "clean". At 17 positions the floor is 162‰ —
/// the shipped gauge could not have seen a class occupying a sixth of the
/// keyspace, and printed `0` while 14,295 dangling tips sat at 6.61‰.
pub(crate) fn locator_only_detection_floor_per_mille(positions: usize) -> Option<u64> {
    if positions == 0 {
        return None;
    }
    let floor = 1.0 - 0.05_f64.powf(1.0 / positions as f64);
    Some((floor * 1000.0).ceil() as u64)
}

pub(super) fn locator_only_probe_strata(windows: usize) -> Vec<(String, Option<String>)> {
    /// Hex chars per boundary. 16^6 distinct cut points.
    const DEPTH: usize = 6;
    const GRID: u64 = 1 << (4 * DEPTH as u64);
    let windows = windows.clamp(1, GRID as usize);
    let width = DEPTH;
    let mut out = Vec::with_capacity(windows);
    let mut start = "mk:".to_string();
    for i in 1..windows as u128 {
        let cut = (i * GRID as u128 / windows as u128) as u64;
        if cut >= GRID {
            break;
        }
        let boundary = format!("mk:{cut:0width$x}");
        if boundary == start {
            // Two cuts landed on the same grid point (windows > grid points in
            // this stretch); skip rather than emit an empty window.
            continue;
        }
        out.push((start, Some(boundary.clone())));
        start = boundary;
    }
    // Final window runs to the end of the `mk:` prefix.
    out.push((start, None));
    out
}

/// Options for the cheap locator-only population probe.
#[derive(Debug, Clone)]
pub struct LocatorOnlyProbeOptions {
    /// Cap how many tips this call classifies. Defaults to
    /// [`DEFAULT_LOCATOR_ONLY_PROBE_MAX_TIPS`].
    pub max_tips: Option<usize>,
    pub tip_page: Option<usize>,
    pub storage_prefix: Option<String>,
}

/// Sampled population of tips whose atom body is reachable **only** via the
/// `aloc:` locator row (not tip-derived prefix, not flat).
///
/// This is the `skipped_mis_derived` class from `repair-dangling-tips`, exposed
/// as a first-class gauge so operators do not need a 57s full-keyspace dry-run
/// solely to learn the counter. When `completed` is true the sample is a full
/// walk; when false, treat rates as estimates and raise `max_tips` to refine.
///
/// The sample is **stratified** over [`locator_only_probe_window_count`]
/// contiguous windows that partition `mk:`, not the first `max_tips` keys in
/// order. That distinction is the whole value of the gauge: the unstratified
/// version shipped reporting 0‰ on a store whose true rate was 250‰, because the
/// locator-only class clusters by molecule id and none of it lived in the first
/// 512 keys.
///
/// Stratification alone is not enough, and the 17-window version proved it:
/// windows whose quota binds are still head reads of their own window. Read
/// [`Self::detection_floor_per_mille`] before reading any rate here — it is what
/// separates "no damage" from "no damage this sample could have seen".
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LocatorOnlyPopulationReport {
    /// Tips classified this call.
    pub tips_sampled: u64,
    /// Cap applied (`max_tips`).
    pub max_tips: u64,
    /// How many contiguous `mk:` windows the sample was drawn from, and how many
    /// of those the walk exhausted.
    ///
    /// `strata_exhausted == strata` is what `completed` means; a caller that
    /// wants to know *how* partial a partial read was reads these.
    #[serde(default)]
    pub strata: u64,
    #[serde(default)]
    pub strata_exhausted: u64,
    /// Tips whose body sits only under the locator-named partition
    /// ([`UnresolvedTipClass::MisDerivedPartition`]).
    pub locator_only: u64,
    /// Tips that resolved via tip-derived prefix or flat (healthy hot-path shape).
    pub body_at_derived_or_flat: u64,
    /// Tips whose atom body is unreachable by **every** reader route — the
    /// `repairable_tips` class of `repair-dangling-tips`, the one that makes a
    /// query return HTTP 200 with fewer rows than its index claims.
    ///
    /// Carved out of [`Self::other_unresolved`] on 2026-08-17, which is why it
    /// is `#[serde(default)]`: a report written by an older daemon carries this
    /// class inside `other_unresolved` and reports `0` here. Read
    /// `dangling + other_unresolved` when comparing across daemon versions.
    ///
    /// It earns its own field because it is the only class in that residual
    /// that means *damage*: `skipped_changed` is a benign race with a
    /// concurrent write, and `skipped_unparseable_key` is a key-shape
    /// complaint. Summing them hid a population that was measured growing at
    /// ~290 refs/hour on the primary — see
    /// `papercut-lastdb-dangling-tips-regenerate-within-hours-of-a-successful-repair`.
    #[serde(default)]
    pub dangling: u64,
    /// The unresolved classes that are NOT locator-only and NOT [`Self::dangling`]
    /// — the residual: changed-under-us, molecule missing, atom not in molecule,
    /// unparseable key, unrepairable.
    ///
    /// Kept under its original name as a residual rather than renamed, so a
    /// dashboard reading it keeps working; the carve-out only makes it smaller.
    pub other_unresolved: u64,
    /// True when the walk finished the `mk:` keyspace without hitting `max_tips`.
    pub completed: bool,
    /// Tips per range page used.
    pub tip_page: u64,
    /// `locator_only * 1000 / tips_sampled` when `tips_sampled > 0`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub locator_only_per_mille: Option<u64>,
    /// `dangling * 1000 / tips_sampled` when `tips_sampled > 0`.
    ///
    /// This is the trend signal the read-integrity gauge cannot give: that one
    /// counts rows a query happened to touch, so it rises with read traffic and
    /// sits still when nothing reads, in both cases independently of whether
    /// damage is growing. This rate is bounded by the store, not by traffic.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dangling_per_mille: Option<u64>,
    /// Unix seconds when this probe finished (for status cache age).
    #[serde(default)]
    pub probed_at_unix: u64,
    /// [`Self::dangling`] from the previous comparable probe in this process,
    /// and when that probe finished.
    ///
    /// Present only when the earlier sample used the same budget — see
    /// [`Self::with_recurrence_against`]. A level tells an operator how much is
    /// broken now; only a pair of levels tells them whether a repair held.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prev_dangling: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prev_probed_at_unix: Option<u64>,
    /// The narrowest contiguous band of `mk:`, in per mille, this sample could
    /// have detected with 95% confidence — see
    /// [`locator_only_detection_floor_per_mille`].
    ///
    /// This is the field that makes a reported `0` readable. A rate of zero
    /// means "no band wider than this floor is present", never "clean", and
    /// without the floor next to it an operator cannot tell those apart. The
    /// shipped 17-window probe had a floor of 162‰ and printed `dangling = 0`
    /// on a store carrying 6.61‰ — a true statement that read as an all-clear.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detection_floor_per_mille: Option<u64>,
    /// New dangling tips per hour since [`Self::prev_probed_at_unix`], or `0`
    /// when the population held steady or fell.
    ///
    /// This is the recurrence signal. `dangling_per_mille` says how much damage
    /// the store carries; this says how fast the store is making more of it,
    /// which is the question a completed repair raises and no single sample can
    /// answer. Measured at ~127 tips/hour on the primary the first time anyone
    /// diffed two probes by hand — see
    /// `papercut-lastdb-dangling-tips-regenerate-within-hours-of-a-successful-repair`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dangling_recurrence_per_hour: Option<u64>,
}

impl LocatorOnlyPopulationReport {
    /// Fill the recurrence fields from the previous probe of this process.
    ///
    /// Refuses to compare samples drawn on different budgets. `dangling` is a
    /// count over a bounded stratified sample, so a probe with a larger
    /// `max_tips` finds more of the same damage; subtracting one from the other
    /// would report store growth that is really budget growth. Requiring equal
    /// `max_tips`, equal `strata`, and the same `completed` verdict keeps the
    /// delta a statement about the store. When they differ the previous level
    /// is still reported and the rate is withheld — an absent rate is honest,
    /// a wrong one is not.
    pub(crate) fn with_recurrence_against(mut self, prev: Option<&Self>) -> Self {
        let Some(prev) = prev else { return self };
        self.prev_dangling = Some(prev.dangling);
        self.prev_probed_at_unix = Some(prev.probed_at_unix);
        let comparable = prev.max_tips == self.max_tips
            && prev.strata == self.strata
            && prev.completed == self.completed;
        let elapsed = self.probed_at_unix.saturating_sub(prev.probed_at_unix);
        if comparable && elapsed > 0 {
            let growth = self.dangling.saturating_sub(prev.dangling);
            self.dangling_recurrence_per_hour = Some(growth.saturating_mul(3600) / elapsed);
        }
        self
    }

    /// Fold one stratum's dry-run into the running sample.
    ///
    /// Summing counters across strata is only sound because the windows are
    /// disjoint and each tip is classified exactly once — see
    /// [`locator_only_probe_strata`].
    pub(super) fn from_strata(
        strata: &[DanglingTipRepairReport],
        max_tips: usize,
        probed_at_unix: u64,
    ) -> Self {
        let mut out = Self {
            tips_sampled: 0,
            max_tips: max_tips as u64,
            strata: strata.len() as u64,
            strata_exhausted: 0,
            locator_only: 0,
            body_at_derived_or_flat: 0,
            dangling: 0,
            other_unresolved: 0,
            completed: false,
            tip_page: strata.first().map_or(0, |r| r.tip_page),
            locator_only_per_mille: None,
            dangling_per_mille: None,
            probed_at_unix,
            prev_dangling: None,
            prev_probed_at_unix: None,
            detection_floor_per_mille: locator_only_detection_floor_per_mille(strata.len()),
            dangling_recurrence_per_hour: None,
        };
        for repair in strata {
            out.tips_sampled = out.tips_sampled.saturating_add(repair.tips_scanned);
            out.locator_only = out.locator_only.saturating_add(repair.skipped_mis_derived);
            out.body_at_derived_or_flat = out
                .body_at_derived_or_flat
                .saturating_add(repair.skipped_body_restored);
            out.dangling = out.dangling.saturating_add(repair.repairable_tips);
            out.other_unresolved = out
                .other_unresolved
                .saturating_add(repair.skipped_changed)
                .saturating_add(repair.skipped_molecule_missing)
                .saturating_add(repair.skipped_atom_not_in_molecule)
                .saturating_add(repair.skipped_unparseable_key)
                .saturating_add(repair.skipped_unrepairable);
            if repair.completed {
                out.strata_exhausted += 1;
            }
        }
        out.completed = out.strata_exhausted == out.strata;
        out.locator_only_per_mille = out
            .locator_only
            .saturating_mul(1000)
            .checked_div(out.tips_sampled);
        out.dangling_per_mille = out
            .dangling
            .saturating_mul(1000)
            .checked_div(out.tips_sampled);
        out
    }
}

/// Process-lifetime cache of the most recent locator-only probe.
///
/// Status reads this without walking the store. Probes write it. Not durable
/// across restarts — a cold process reports "never probed" until the next call.
pub(super) static LAST_LOCATOR_ONLY_PROBE: std::sync::Mutex<Option<LocatorOnlyPopulationReport>> =
    std::sync::Mutex::new(None);

/// Last locator-only population probe for this process, if any.
pub fn last_locator_only_probe() -> Option<LocatorOnlyPopulationReport> {
    LAST_LOCATOR_ONLY_PROBE.lock().ok().and_then(|g| g.clone())
}

pub(super) fn remember_locator_only_probe(report: &LocatorOnlyPopulationReport) {
    if let Ok(mut guard) = LAST_LOCATOR_ONLY_PROBE.lock() {
        *guard = Some(report.clone());
    }
}
