use super::*;

impl AtomStore {
    /// One bounded page of the zero-live sweep — phase B of the bloat audit.
    ///
    /// Phase A walks `mk:` and decides molecules that still have live keys.
    /// This phase pages the `moc:` namespace instead and selects residue: a
    /// molecule with an order count or order log and **no** live `mk:` row.
    ///
    /// It does not need phase A's results, and deliberately does not take
    /// them. A molecule phase A decided necessarily has a live `mk:` row, so
    /// `molecule_has_any_live_key` — a one-row prefix probe — already excludes
    /// it. That probe is point-scoped and independent of how much of the `mk:`
    /// keyspace any call walked, which is what makes this phase safe to page
    /// at all. `compact_order_log_zero_live_one` re-probes under the molecule
    /// commit guard before deleting, so a writer that lands an `mk:` row
    /// between selection here and the delete makes the molecule ineligible
    /// rather than losing its order log.
    ///
    /// Three independent bounds stop a page, and whichever trips first wins:
    ///
    /// * `ZERO_LIVE_COMMIT_BATCH` candidates selected — this is the bound on
    ///   the **commit**. The caller deletes exactly what one page selected, so
    ///   no single request can delete more than this many molecules.
    /// * `max_keys` `moc:` rows examined — the bound on classification work,
    ///   which costs a live probe plus a storage measure per row.
    /// * `pass_budget` wall-clock.
    ///
    /// Returning `more_remaining` with a `moc:`-namespace cursor is what banks
    /// progress: the caller commits this page before asking for the next one.
    #[allow(clippy::too_many_arguments)]
    // lint:fn-size-ok verbatim move from admin_db.rs; splitting this function is separate work
    pub(in super::super) async fn sweep_zero_live_page(
        &self,
        mut report: OrderLogBloatAudit,
        cursor: &str,
        moc_prefix: &str,
        max_keys: Option<usize>,
        pass_started: std::time::Instant,
        pass_budget: std::time::Duration,
        storage_prefix: Option<&str>,
    ) -> Result<OrderLogBloatAudit, SchemaError> {
        const SCAN_PAGE: usize = 2048;
        /// Molecules one call may hand to the delete phase. Sized so a page's
        /// deletes stay well inside any client deadline; the cursor makes the
        /// number of passes, not the number of bytes, the thing that grows.
        const ZERO_LIVE_COMMIT_BATCH: usize = 20_000;

        // Entered with nothing left to spend — phase A used the whole pass.
        // Hand the caller the cursor it came in on so the next call, which
        // gets a fresh budget, resumes exactly here instead of crawling one
        // row per round trip.
        if max_keys == Some(0) || pass_started.elapsed() >= pass_budget {
            report.more_remaining = true;
            report.next_after_key = Some(cursor.to_owned());
            Self::sort_order_log_bloat(&mut report);
            return Ok(report);
        }

        let (moc_start, moc_end) = kind_partition::colon_plane_bounds(moc_prefix);
        let mut page_start = if moc_molecule_from_key(cursor).is_some() {
            cursor.to_owned()
        } else {
            moc_start
        };
        // The cursor names the last row the previous call consumed (or the
        // bare plane start, which matches no real moc count key), so skip it once.
        let mut skip_head = Some(cursor.to_owned());
        let mut examined: usize = 0;
        let mut range_exhausted = false;

        while !range_exhausted {
            let rows = self
                .raw()
                .inner()
                .scan_range_paged(page_start.as_bytes(), moc_end.as_bytes(), SCAN_PAGE)
                .await
                .map_err(|e| {
                    SchemaError::InvalidData(format!("scan moc order counts (zero-live): {e}"))
                })?;
            if rows.len() < SCAN_PAGE {
                range_exhausted = true;
            }
            if rows.is_empty() {
                break;
            }

            let head = skip_head.take();
            for (k, v) in rows {
                let key = String::from_utf8_lossy(&k).into_owned();
                page_start = key.clone();
                if head.as_deref() == Some(key.as_str()) {
                    continue;
                }
                let Some(molecule) = moc_molecule_from_key(&key) else {
                    continue;
                };
                let Ok(order_count) = serde_json::from_slice::<u64>(&v) else {
                    report.order_counts_unreadable += 1;
                    continue;
                };
                report.order_counts_read += 1;
                examined += 1;

                if !self
                    .molecule_has_any_live_key(&molecule, storage_prefix)
                    .await?
                {
                    let moc_bytes = k.len() as u64 + v.len() as u64;
                    let (order_log_entries, order_log_bytes) = self
                        .measure_order_log_storage(&molecule, storage_prefix)
                        .await?;
                    if order_count > 0 || order_log_entries > 0 {
                        report.molecules_zero_live += 1;
                        report.stale_entries += order_log_entries;
                        report.order_log_bytes += order_log_bytes;
                        report.order_count_bytes += moc_bytes;
                        report.zero_live_bytes += order_log_bytes + moc_bytes;
                        report.zero_live_molecules.push(OrderLogBloatRow {
                            molecule,
                            schema: None,
                            order_count,
                            live_keys: 0,
                            live_unique_keys: 0,
                            stale_entries: order_log_entries,
                            order_log_entries,
                            order_log_bytes,
                            order_count_bytes: moc_bytes,
                            zero_live: true,
                        });
                    }
                }

                if report.zero_live_molecules.len() >= ZERO_LIVE_COMMIT_BATCH
                    || max_keys.is_some_and(|max| examined >= max)
                    || pass_started.elapsed() >= pass_budget
                {
                    report.more_remaining = true;
                    report.next_after_key = Some(page_start.clone());
                    Self::sort_order_log_bloat(&mut report);
                    return Ok(report);
                }
            }
            skip_head = Some(page_start.clone());
        }

        report.more_remaining = false;
        report.next_after_key = None;
        Self::sort_order_log_bloat(&mut report);
        Ok(report)
    }

    /// True when `mk:{M}:` holds at least one live storage row. Point-scoped.
    pub(in super::super) async fn molecule_has_any_live_key(
        &self,
        molecule: &str,
        storage_prefix: Option<&str>,
    ) -> Result<bool, SchemaError> {
        let mol_prefix = build_storage_key(
            storage_prefix,
            &molecule_key_codec::molecule_record_prefix(molecule),
        );
        let mol_end = FilterUtils::create_prefix_end(&mol_prefix);
        let rows = self
            .raw()
            .inner()
            .scan_range_paged(mol_prefix.as_bytes(), mol_end.as_bytes(), 1)
            .await
            .map_err(|e| {
                SchemaError::InvalidData(format!("probe live keys for {molecule}: {e}"))
            })?;
        Ok(!rows.is_empty())
    }
}
