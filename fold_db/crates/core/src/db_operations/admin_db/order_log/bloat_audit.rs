use super::*;

impl AtomStore {
    /// Audit every `moc:{M}` order-log count against its molecule's live logical
    /// `mk:` slots and measure exact stored bytes of excess residue. Read-only.
    ///
    /// Companion to [`Self::audit_order_log_counts`]: that detector finds logs
    /// that are *too short*; this one finds logs that are *too long* (or that
    /// outlive every key of their molecule). Bounding and resume match the
    /// shortfall audit: soft `max_keys` at molecule boundaries, wall-clock
    /// budget, `next_after_key` cursor.
    ///
    /// The audit runs in two bounded phases, and `next_after_key` says which
    /// one the next call resumes because the cursor carries its namespace:
    ///
    /// * **Phase A** (`mk:` cursor) walks live keys and decides bloated
    ///   molecules at their boundaries.
    /// * **Phase B** (`moc:` cursor, [`Self::sweep_zero_live_page`]) pages the
    ///   order-count namespace and selects zero-live residue.
    ///
    /// Every phase-A page ends by handing off to phase B rather than
    /// classifying zero-live residue itself. Deferring that classification to
    /// the terminal page is what made this verb unrunnable on a real store:
    /// the walk was bounded and the resulting delete batch was not.
    // lint:fn-size-ok verbatim move from admin_db.rs; splitting this function is separate work
    pub async fn audit_order_log_bloat(
        &self,
        max_keys: Option<usize>,
        after_key: Option<&str>,
        storage_prefix: Option<&str>,
    ) -> Result<OrderLogBloatAudit, SchemaError> {
        const SCAN_PAGE: usize = 2048;
        const PASS_TIME_BUDGET: std::time::Duration = std::time::Duration::from_secs(240);

        let pass_started = std::time::Instant::now();
        let mut report = OrderLogBloatAudit::default();

        let (moc_colon, moc_start, moc_end) = moc_plane(storage_prefix);

        // Phase B resume. A cursor in the moc namespace (`moc\0` writes or
        // leftover `moc:` rows) means the `mk:` walk already finished on an
        // earlier call and this one continues the bounded zero-live sweep.
        // Return before the order-count snapshot below: phase B reads each
        // moc row from its own paged scan, so a resumed pass never rebuilds
        // a whole-store map it does not use.
        if let Some(cursor) = after_key.filter(|key| cursor_is_moc_plane(key)) {
            return self
                .sweep_zero_live_page(
                    report,
                    cursor,
                    &moc_colon,
                    max_keys,
                    pass_started,
                    PASS_TIME_BUDGET,
                    storage_prefix,
                )
                .await;
        }

        // 1. Snapshot every order count (and the count record's exact bytes).
        // Walk `moc\0` and leftover `moc:` together — writes are kind-as-partition.
        let mut order_counts: HashMap<String, (u64, u64)> = HashMap::new();
        for (k, v) in self
            .raw()
            .inner()
            .scan_range(moc_start.as_bytes(), moc_end.as_bytes())
            .await
            .map_err(|e| SchemaError::InvalidData(format!("scan moc order counts (bloat): {e}")))?
        {
            let key = String::from_utf8_lossy(&k).into_owned();
            let Some(molecule) = moc_molecule_from_key(&key) else {
                continue;
            };
            let Ok(count) = serde_json::from_slice::<u64>(&v) else {
                report.order_counts_unreadable += 1;
                continue;
            };
            report.order_counts_read += 1;
            let moc_bytes = k.len() as u64 + v.len() as u64;
            order_counts.insert(molecule, (count, moc_bytes));
        }

        // 2. Walk live keys, decide each molecule at its boundary.
        let mk_prefix = build_storage_key(storage_prefix, molecule_key_codec::MK_PREFIX);
        let mk_end = FilterUtils::create_prefix_end(&mk_prefix);
        let mut page_start = after_key.map_or_else(|| mk_prefix.clone(), str::to_owned);
        let mut skip_head = after_key.map(str::to_owned);
        let mut range_exhausted = false;

        let mut open: Option<OpenMoleculeWalk> = None;
        let mut last_key_of_open: Option<String> = None;
        let mut stop_at_boundary = false;

        while !range_exhausted {
            let rows = self
                .raw()
                .inner()
                .scan_range_paged(page_start.as_bytes(), mk_end.as_bytes(), SCAN_PAGE)
                .await
                .map_err(|e| SchemaError::InvalidData(format!("range scan mk order bloat: {e}")))?;
            if rows.len() < SCAN_PAGE {
                range_exhausted = true;
            }
            if rows.is_empty() {
                break;
            }

            let head = skip_head.take();
            for (k, _) in rows {
                let key = String::from_utf8_lossy(&k).into_owned();
                page_start = key.clone();
                if head.as_deref() == Some(key.as_str()) {
                    continue;
                }
                let Some(molecule) = key
                    .strip_prefix(mk_prefix.as_str())
                    .and_then(|rest| rest.split_once(':'))
                    .map(|(molecule, _)| molecule.to_string())
                else {
                    continue;
                };
                let logical = Self::order_audit_logical_id(&molecule, &key, mk_prefix.as_str());

                match &mut open {
                    Some(cur) if cur.molecule == molecule => {
                        cur.raw_keys += 1;
                        cur.unique.insert(logical);
                        last_key_of_open = Some(key);
                    }
                    _ => {
                        if let Some(done) = open.take() {
                            self.decide_order_log_bloat_molecule(
                                &mut report,
                                &order_counts,
                                &done.molecule,
                                done.raw_keys,
                                done.unique.len() as u64,
                                storage_prefix,
                            )
                            .await?;
                            report.next_after_key = last_key_of_open.take();
                        }
                        if stop_at_boundary {
                            report.more_remaining = true;
                            Self::sort_order_log_bloat(&mut report);
                            return Ok(report);
                        }
                        let mut unique = HashSet::new();
                        unique.insert(logical);
                        open = Some(OpenMoleculeWalk {
                            molecule,
                            raw_keys: 1,
                            unique,
                        });
                        last_key_of_open = Some(key);
                    }
                }
                report.keys_scanned += 1;

                if !stop_at_boundary
                    && (max_keys.is_some_and(|max| report.keys_scanned as usize >= max)
                        || pass_started.elapsed() >= PASS_TIME_BUDGET)
                {
                    stop_at_boundary = true;
                }
            }
            skip_head = Some(page_start.clone());
        }

        if let Some(done) = open.take() {
            self.decide_order_log_bloat_molecule(
                &mut report,
                &order_counts,
                &done.molecule,
                done.raw_keys,
                done.unique.len() as u64,
                storage_prefix,
            )
            .await?;
        }
        // The `mk:` walk is finished. Zero-live classification used to happen
        // right here, over the whole-store `order_counts` map, and
        // `compact_order_log_zero_live` would then delete every molecule it
        // named in this same request. `max_keys` bounded the walk and nothing
        // bounded that commit, so on a real store the terminal pass held one
        // synchronous request open for ~1.07 M deletes and never returned —
        // three attempts, ~2h35m of node time, zero bytes reclaimed.
        //
        // Continue into the bounded sweep instead, with whatever budget this
        // pass has left. A small store still finishes in one call; a large one
        // stops at the first bound and returns a `moc:`-namespace cursor,
        // which routes the next call straight back into phase B.
        let spent = usize::try_from(report.keys_scanned).unwrap_or(usize::MAX);
        let remaining = max_keys.map(|max| max.saturating_sub(spent));
        self.sweep_zero_live_page(
            report,
            &moc_start,
            &moc_colon,
            remaining,
            pass_started,
            PASS_TIME_BUDGET,
            storage_prefix,
        )
        .await
    }
}
