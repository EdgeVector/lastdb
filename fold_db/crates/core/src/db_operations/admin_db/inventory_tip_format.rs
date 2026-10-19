//! Tip-format scan and thin-tip migration.

use super::*;

impl AtomStore {
    /// Scan `mk:` tip values and count thin vs fat, bounded and resumable.
    ///
    /// **Why bounded.** `tips` is the largest plane on a real home. The
    /// whole-prefix form of this scan materialized every `mk:` row in one
    /// allocation before deciding any of them, so its transient footprint grew
    /// with the plane rather than with the answer. Measured on a copy-on-write
    /// clone of a real 7.6 GiB home (2026-08-18): 2,188,593 `mk:` rows,
    /// 1.22 GB of key+value bytes held at once. That home completes in ~47s,
    /// so this is a scaling hazard rather than an observed timeout — but the
    /// allocation is unbounded in the one dimension that keeps growing, and
    /// the control socket's read deadline is what it grows toward.
    /// Paging the ordered keyspace with a key cap and a wall-clock budget makes
    /// the question answerable one window at a time, the same discipline
    /// [`AtomStore::audit_key_tombstone_flags`] already uses.
    ///
    /// `max_keys` caps rows decided by this call; `after_key` resumes past a
    /// previous call's `next_after_key`. Neither is required: with both `None`
    /// the pass still yields at [`Self::TIP_PASS_TIME_BUDGET`] rather than
    /// running to the deadline, and says so in `more_remaining`.
    pub async fn scan_tip_format(
        &self,
        max_keys: Option<usize>,
        after_key: Option<&str>,
        storage_prefix: Option<&str>,
    ) -> Result<TipFormatStats, SchemaError> {
        use super::super::atom_store::PerKeyRecord;

        let pass_started = std::time::Instant::now();
        let prefix = build_storage_key(storage_prefix, "mk:");
        let end = FilterUtils::create_prefix_end(&prefix)?;
        let mut stats = TipFormatStats::default();
        let mut page_start = after_key.map_or_else(|| prefix.clone(), str::to_owned);
        let mut skip_head = after_key.map(str::to_owned);
        let mut range_exhausted = false;

        while !range_exhausted {
            let rows = self
                .raw()
                .inner()
                .scan_range_paged(page_start.as_bytes(), end.as_bytes(), Self::TIP_SCAN_PAGE)
                .await
                .map_err(|e| SchemaError::InvalidData(format!("scan mk tips: {e}")))?;
            if rows.len() < Self::TIP_SCAN_PAGE {
                range_exhausted = true;
            }
            if rows.is_empty() {
                break;
            }
            let head = skip_head.take();
            let mut stop_after_page = false;
            for (k, v) in rows {
                let key = String::from_utf8_lossy(&k).into_owned();
                page_start.clone_from(&key);
                if head.as_deref() == Some(key.as_str()) {
                    continue;
                }
                if max_keys.is_some_and(|max| stats.tips_scanned as usize >= max)
                    || (stats.tips_scanned > 0
                        && pass_started.elapsed() >= Self::TIP_PASS_TIME_BUDGET)
                {
                    stats.more_remaining = true;
                    stop_after_page = true;
                    break;
                }
                stats.tips_scanned += 1;
                stats.next_after_key = Some(key);
                let bytes = k.len() as u64 + v.len() as u64;
                match serde_json::from_slice::<PerKeyRecord>(&v) {
                    Ok(rec) if rec.entry.is_thin() => {
                        stats.tips_thin += 1;
                        stats.thin_bytes_approx += bytes;
                    }
                    Ok(_) => {
                        stats.tips_fat += 1;
                        stats.fat_bytes_approx += bytes;
                    }
                    Err(_) => {
                        stats.tips_unreadable += 1;
                    }
                }
            }
            skip_head = Some(page_start.clone());
            if stop_after_page {
                break;
            }
        }
        Ok(stats)
    }

    /// Raw `mk:` rows fetched per storage page. Bounds one pass's transient
    /// scan allocation independently of how large the `tips` plane is.
    const TIP_SCAN_PAGE: usize = 2048;
    /// Rewrites per batched put, drained inside the walk.
    /// Wall-clock budget for one pass, well under the 600s control-socket read
    /// deadline. A key cap alone cannot bound a pass whose per-row cost varies
    /// by home; the bound the caller actually needs is the one the socket
    /// enforces, so bound on that.
    const TIP_PASS_TIME_BUDGET: std::time::Duration = std::time::Duration::from_secs(240);

    /// Rewrite fat `mk:` tip values to thin in place (same keys), bounded and
    /// resumable.
    ///
    /// **Why bounded.** The whole-prefix form scanned every `mk:` row into one
    /// allocation and then queued every rewrite into a second one before
    /// writing anything — on a real home that is millions of rows twice over,
    /// and the write half only started after the whole plane had been read.
    /// Worse, the dry run built the rewrite queue it would never use, so
    /// *measuring* the lever cost as much memory as pulling it. Pages of
    /// [`Self::TIP_SCAN_PAGE`] with a key cap and a time budget bound the read,
    /// draining puts inside the walk bounds the write, and dry runs now
    /// allocate nothing.
    ///
    /// `max_keys` caps rows decided by this call; `after_key` resumes past a
    /// previous call's `next_after_key`. A pass that stops early says so in
    /// `more_remaining` — the caller follows the cursor to the end.
    ///
    /// **What it returns, measured — do not budget bytes against this.** On a
    /// copy-on-write clone of a real 7.6 GiB home (2026-08-18), the dry run
    /// decided 2,188,593 `mk:` rows and found 2,184,537 of them (**99.81%**)
    /// already thin. Rewriting the remaining 4,056 moves 1,219,905,942 bytes
    /// to 1,219,650,414 — **249.5 KiB, 0.02% of the `mk:` population**. Local
    /// writes have emitted thin for long enough that the legacy fat tail is
    /// spent. This verb is an instrument, not a reclaim lever: it answers
    /// "is any of this plane still legacy fat?" cheaply and repeatedly.
    /// The mass in `tips` is the `tv:` archived-chain half, which
    /// [`AtomStore::drain_tip_history_chains`] addresses.
    pub async fn migrate_thin_tips(
        &self,
        dry_run: bool,
        max_keys: Option<usize>,
        after_key: Option<&str>,
        storage_prefix: Option<&str>,
    ) -> Result<ThinTipMigrateReport, SchemaError> {
        use super::super::atom_store::PerKeyRecord;

        if !dry_run {
            return Err(SchemaError::InvalidData(
                "migrate-thin-tips execute was removed: only the atom counter planner writes tip records"
                    .into(),
            ));
        }
        let pass_started = std::time::Instant::now();
        let prefix = build_storage_key(storage_prefix, "mk:");
        let end = FilterUtils::create_prefix_end(&prefix)?;

        let mut report = ThinTipMigrateReport {
            dry_run,
            ..Default::default()
        };
        let mut page_start = after_key.map_or_else(|| prefix.clone(), str::to_owned);
        let mut skip_head = after_key.map(str::to_owned);
        let mut range_exhausted = false;

        while !range_exhausted {
            let rows = self
                .raw()
                .inner()
                .scan_range_paged(page_start.as_bytes(), end.as_bytes(), Self::TIP_SCAN_PAGE)
                .await
                .map_err(|e| SchemaError::InvalidData(format!("scan mk migrate: {e}")))?;
            if rows.len() < Self::TIP_SCAN_PAGE {
                range_exhausted = true;
            }
            if rows.is_empty() {
                break;
            }
            let head = skip_head.take();
            let mut stop_after_page = false;
            for (k, v) in rows {
                let key = String::from_utf8_lossy(&k).into_owned();
                page_start.clone_from(&key);
                if head.as_deref() == Some(key.as_str()) {
                    continue;
                }
                if max_keys.is_some_and(|max| report.tips_scanned as usize >= max)
                    || (report.tips_scanned > 0
                        && pass_started.elapsed() >= Self::TIP_PASS_TIME_BUDGET)
                {
                    report.more_remaining = true;
                    stop_after_page = true;
                    break;
                }
                report.tips_scanned += 1;
                report.bytes_before_approx += k.len() as u64 + v.len() as u64;
                report.next_after_key = Some(key.clone());

                let Ok(rec) = serde_json::from_slice::<PerKeyRecord>(&v) else {
                    report.tips_unreadable += 1;
                    report.bytes_after_approx += k.len() as u64 + v.len() as u64;
                    continue;
                };
                if rec.entry.is_thin() {
                    report.tips_already_thin += 1;
                    report.bytes_after_approx += k.len() as u64 + v.len() as u64;
                    continue;
                }
                let thin = rec.entry.to_thin();
                let val = serde_json::to_value(super::super::atom_store::PerKeyRecord {
                    entry: thin,
                    ..rec
                })
                .map_err(|e| SchemaError::InvalidData(format!("serialize thin tip: {e}")))?;
                report.bytes_after_approx +=
                    key.len() as u64 + serde_json::to_vec(&val).map_or(0, |b| b.len() as u64);
                report.tips_rewritten += 1;
            }
            skip_head = Some(page_start.clone());
            if stop_after_page {
                break;
            }
        }

        Ok(report)
    }
}
