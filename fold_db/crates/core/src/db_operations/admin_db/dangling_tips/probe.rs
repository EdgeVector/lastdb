use super::*;

impl AtomStore {
    /// Cheap (bounded) **stratified** sample of the locator-only tip population.
    ///
    /// Reuses the same classification as `repair-dangling-tips` dry-run but always
    /// read-only and defaults to a small `max_tips` so status/ops can surface the
    /// gauge without a full-store walk. The result is also cached process-wide
    /// via [`last_locator_only_probe`] for `/api/status`.
    ///
    /// The budget is spread over [`locator_only_probe_window_count`] disjoint
    /// windows that partition `mk:`, rather than spent on the first `max_tips`
    /// keys. Reading consecutive keys is not sampling: molecule-id order is also
    /// migration order, so a class introduced by one migration sits contiguously
    /// and a head read either misses all of it or is all of it. Each window's
    /// quota is `remaining_budget / remaining_windows`, so windows that run dry
    /// hand their unspent budget to the ones that follow instead of shrinking
    /// the sample.
    ///
    /// **The budget buys positions, not depth.** Between 2026-08-09 and
    /// 2026-09-07 it bought depth at 17 fixed positions, which is 17 head reads
    /// wearing a stratified sample's name: it reported `dangling = 0‰` against a
    /// full walk's 6.61‰, and a 16x budget rise moved it 0 → 0. Window count now
    /// scales with `max_tips`, so a bigger budget narrows what can hide instead
    /// of re-reading the same 17 places more deeply.
    ///
    /// Each window costs one range page plus one batched existence probe, so
    /// `tip_page` is sized to the quota: reading 1024 rows to classify 8 of them
    /// is the waste the old fixed page had to accept when there were only 17
    /// windows to amortise it over.
    pub async fn probe_locator_only_population(
        &self,
        options: LocatorOnlyProbeOptions,
    ) -> Result<LocatorOnlyPopulationReport, SchemaError> {
        let max_tips = options
            .max_tips
            .unwrap_or(DEFAULT_LOCATOR_ONLY_PROBE_MAX_TIPS)
            .max(1);
        let windows = locator_only_probe_strata(locator_only_probe_window_count(max_tips));
        let mut remaining = max_tips;
        let mut strata = Vec::with_capacity(windows.len());
        for (i, window) in windows.iter().enumerate() {
            let left = windows.len() - i;
            // Ceil-divide so the last window can never be handed a zero quota.
            let quota = remaining.div_ceil(left).max(1);
            // One row past the quota, so the page that fills the budget also
            // proves there was more to read — the walk then stops on this page
            // instead of spending a second round trip to learn the same thing.
            // An explicit `tip_page` still wins, for callers sizing the scan.
            let tip_page = options.tip_page.or(Some(quota.saturating_add(1)));
            let repair = self
                .repair_dangling_tips(DanglingTipRepairOptions {
                    dry_run: true,
                    max_ops: Some(quota),
                    tip_page,
                    audit_unresolved: None,
                    storage_prefix: options.storage_prefix.clone(),
                    key_window: Some(window.clone()),
                    scope: None,
                })
                .await?;
            remaining = remaining.saturating_sub(repair.tips_scanned as usize);
            strata.push(repair);
        }
        let probed_at_unix = crate::clock::unix_secs();
        // Read the cache before overwriting it: the previous level is what turns
        // this sample from "how much is broken" into "is it getting worse".
        let previous = last_locator_only_probe();
        let report = LocatorOnlyPopulationReport::from_strata(&strata, max_tips, probed_at_unix)
            .with_recurrence_against(previous.as_ref());
        remember_locator_only_probe(&report);
        Ok(report)
    }
}
