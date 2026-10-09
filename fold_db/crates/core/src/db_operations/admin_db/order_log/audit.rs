use super::*;

impl AtomStore {
    /// Audit every `moc:{M}` order-log count against its molecule's live `mk:`
    /// record count. Read-only; see [`OrderLogAudit`] for the invariant and why
    /// the `mk:` count is the only witness a truncated log leaves behind.
    ///
    /// **Cost.** One `moc:` prefix scan (a small key class — 170 KiB on the
    /// 2026-08 primary) plus a paged walk of the `mk:` keyspace. Unlike
    /// [`Self::audit_key_tombstone_flags`] this never fetches an atom body: the
    /// molecule uuid comes out of the key, so a row costs a `split_once`. That
    /// is what makes a whole-store pass affordable.
    ///
    /// **Bounding.** `max_keys` is a *soft* cap: the walk stops at the first
    /// molecule boundary at or after it. A hard cap could land mid-molecule on
    /// a molecule with more keys than the cap and never finish it, so no pass
    /// would ever decide it and the cursor would not advance. Overshooting one
    /// molecule is the cheaper failure.
    // lint:fn-size-ok verbatim move from admin_db.rs; splitting this function is separate work
    pub async fn audit_order_log_counts(
        &self,
        max_keys: Option<usize>,
        after_key: Option<&str>,
        storage_prefix: Option<&str>,
    ) -> Result<OrderLogAudit, SchemaError> {
        /// Raw `mk:` rows per storage page. Bounds one pass's transient scan
        /// allocation independently of store size.
        const SCAN_PAGE: usize = 2048;
        /// Wall-clock budget for one pass, well under the 600s control-socket
        /// read deadline. Also enforced only at molecule boundaries.
        const PASS_TIME_BUDGET: std::time::Duration = std::time::Duration::from_secs(240);

        let pass_started = std::time::Instant::now();
        let mut report = OrderLogAudit::default();

        // 1. The order counts. Small class, read whole — the audit needs random
        // access to it by molecule, and paging a map it will fully materialize
        // anyway buys nothing.
        let (_moc_colon, moc_start, moc_end) = moc_plane(storage_prefix);
        let mut order_counts: HashMap<String, u64> = HashMap::new();
        for (k, v) in self
            .raw()
            .inner()
            .scan_range(moc_start.as_bytes(), moc_end.as_bytes())
            .await
            .map_err(|e| SchemaError::InvalidData(format!("scan moc order counts: {e}")))?
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
            order_counts.insert(molecule, count);
        }

        // 2. The key counts, in key order so molecules complete in order.
        let mk_prefix = build_storage_key(storage_prefix, molecule_key_codec::MK_PREFIX);
        let mk_end = FilterUtils::create_prefix_end(&mk_prefix);
        let mut page_start = after_key.map_or_else(|| mk_prefix.clone(), str::to_owned);
        let mut skip_head = after_key.map(str::to_owned);
        let mut range_exhausted = false;

        // The molecule currently being walked, and the last key seen for it.
        // Nothing is decided until the walk leaves the molecule, so a pass that
        // is cut short cannot report a half-counted molecule as healthy.
        //
        // `unique` tracks decoded (hash, range) identities so H1 (missing
        // append: unique > moc) can be told from H2 (dual encoding: raw > unique
        // with unique ≈ moc). Undecodable suffixes fall back to the full storage
        // key so they never collapse into each other.
        let mut open: Option<OpenMoleculeWalk> = None;
        let mut last_key_of_open: Option<String> = None;
        let mut decided_molecules: HashSet<String> = HashSet::new();
        let mut stop_at_boundary = false;

        while !range_exhausted {
            let rows = self
                .raw()
                .inner()
                .scan_range_paged(page_start.as_bytes(), mk_end.as_bytes(), SCAN_PAGE)
                .await
                .map_err(|e| SchemaError::InvalidData(format!("range scan mk order audit: {e}")))?;
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
                        // Left the previous molecule: it is now fully walked.
                        if let Some(done) = open.take() {
                            self.decide_order_log_molecule(
                                &mut report,
                                &order_counts,
                                &done.molecule,
                                done.raw_keys,
                                done.unique.len() as u64,
                                storage_prefix,
                            )
                            .await;
                            decided_molecules.insert(done.molecule);
                            report.next_after_key = last_key_of_open.take();
                        }
                        if stop_at_boundary {
                            // This row belongs to the NEXT molecule and is not
                            // counted: the cursor points before it, so the
                            // resumed pass walks it.
                            report.more_remaining = true;
                            return Ok(Self::finish_order_log_audit(
                                report,
                                &order_counts,
                                &decided_molecules,
                                false,
                            ));
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

                // Soft caps: arm the brake, and stop at the next boundary. A
                // HARD cap here decides the molecule the walk is standing
                // inside, on a key count that is still partial — and a partial
                // count under a full `moc:` reads as healthy. Verified by
                // mutation: with a hard cap, the truncated molecule in
                // `order_log_audit_paging_never_clears_a_partly_walked_molecule`
                // is not merely mis-scored, it disappears from the report.
                if !stop_at_boundary
                    && (max_keys.is_some_and(|max| report.keys_scanned as usize >= max)
                        || pass_started.elapsed() >= PASS_TIME_BUDGET)
                {
                    stop_at_boundary = true;
                }
            }
            skip_head = Some(page_start.clone());
        }

        // The keyspace ended, so the molecule still open is complete too.
        if let Some(done) = open.take() {
            self.decide_order_log_molecule(
                &mut report,
                &order_counts,
                &done.molecule,
                done.raw_keys,
                done.unique.len() as u64,
                storage_prefix,
            )
            .await;
            decided_molecules.insert(done.molecule);
        }
        report.next_after_key = None;
        let whole_keyspace = after_key.is_none();
        Ok(Self::finish_order_log_audit(
            report,
            &order_counts,
            &decided_molecules,
            whole_keyspace,
        ))
    }

    /// The verb writes nothing. SampleN reads `mk:` tips.
    pub fn repair_order_log_shortfall(
        &self,
        _dry_run: bool,
        _max_keys: Option<usize>,
        _after_key: Option<&str>,
        _storage_prefix: Option<&str>,
    ) -> Result<OrderLogRepairReport, SchemaError> {
        let _ = self;
        Err(SchemaError::InvalidData(
            "repair-order-log-shortfall writes nothing".into(),
        ))
    }

    /// Identity used for unique-key counting inside the order-log audit.
    ///
    /// Prefer the decoded `(hash, range)` pair so two storage encodings of the
    /// same logical key collapse. Fall back to the full storage key when the
    /// suffix is not a unified HashRange encoding (legacy or corrupt shapes).
    pub(in super::super) fn order_audit_logical_id(
        molecule: &str,
        full_key: &str,
        mk_prefix: &str,
    ) -> String {
        let Some(rest) = full_key.strip_prefix(mk_prefix) else {
            return full_key.to_string();
        };
        let Some(suffix) = rest
            .strip_prefix(molecule)
            .and_then(|s| s.strip_prefix(':'))
        else {
            return full_key.to_string();
        };
        match molecule_key_codec::decode_hash_range_suffix(suffix) {
            Some((hash, range)) => format!("\0{hash}\0{range}"),
            None => full_key.to_string(),
        }
    }

    /// Classify one fully walked molecule against its persisted `moc:` count.
    ///
    /// A molecule that looks short against the step-1 snapshot has its `moc:`
    /// re-read here, after its keys are counted, and is re-decided on the fresh
    /// value. This removes the audit's built-in positive bias without opening a
    /// false-negative hole: the re-read can only clear a molecule whose log has
    /// genuinely caught up with its keys, and a store that really is missing
    /// entries has nothing to catch up with. It cuts the other way too — a
    /// count that *fell* between the snapshot and now is a live reap, and the
    /// fresh value reports the larger, truer shortfall.
    ///
    /// `keys` is the raw storage-row count; `unique_keys` is the number of
    /// distinct decoded `(hash, range)` pairs. The raw shortfall can be pure
    /// encoding duality (H2) when `unique_keys <= order_count < keys`.
    /// `pub(crate)` for the test seam: passing a deliberately stale
    /// `order_counts` against a fresh store is the only deterministic way to
    /// reproduce the snapshot/walk race this re-read exists to absorb.
    pub(crate) async fn decide_order_log_molecule(
        &self,
        report: &mut OrderLogAudit,
        order_counts: &HashMap<String, u64>,
        molecule: &str,
        keys: u64,
        unique_keys: u64,
        storage_prefix: Option<&str>,
    ) {
        report.molecules_decided += 1;
        let Some(&snapshot_count) = order_counts.get(molecule) else {
            // Hash/Range molecule or a pre-split inline order — not damage.
            report.molecules_without_order_count += 1;
            return;
        };
        if snapshot_count >= keys {
            report.molecules_ok += 1;
            return;
        }
        // Short against the snapshot. Re-read before believing it.
        report.short_candidates_rechecked += 1;
        let order_count = match self.reread_order_count(molecule, storage_prefix).await {
            // Unreadable now, or the row is gone: keep the snapshot rather than
            // clearing a finding on a failed read. A detector must not treat
            // "could not check" as "healthy".
            Ok(Some(fresh)) => fresh,
            Ok(None) | Err(_) => snapshot_count,
        };
        if order_count >= keys {
            report.short_candidates_cleared_by_recheck += 1;
            report.molecules_ok += 1;
            return;
        }
        let shortfall = keys - order_count;
        let unique_keys = unique_keys.min(keys);
        let duplicate_storage_rows = keys.saturating_sub(unique_keys);
        let logical_shortfall = unique_keys.saturating_sub(order_count);
        report.molecules_short += 1;
        report.entries_missing += shortfall;
        report.logical_entries_missing += logical_shortfall;
        if duplicate_storage_rows > 0 {
            report.molecules_with_duplicate_storage_rows += 1;
        }
        report.short_molecules.push(OrderLogShortRow {
            molecule: molecule.to_string(),
            order_count,
            keys,
            unique_keys,
            duplicate_storage_rows,
            shortfall,
            logical_shortfall,
            // Resolved by the `FoldDB` caller, which holds the schema manager.
            schema: None,
        });
    }

    /// Point-read one molecule's current `moc:` count.
    ///
    /// `Ok(None)` means the row is absent or does not decode — deliberately not
    /// folded into `Ok(0)`, which would read as "the log is empty" and turn a
    /// failed check into a maximal finding.
    pub(in super::super) async fn reread_order_count(
        &self,
        molecule: &str,
        storage_prefix: Option<&str>,
    ) -> Result<Option<u64>, SchemaError> {
        let key = build_storage_key(
            storage_prefix,
            &molecule_key_codec::order_count_key(molecule),
        );
        let dense = self
            .raw()
            .get_item::<u64>(&key)
            .await
            .map_err(|e| SchemaError::InvalidData(format!("re-read moc {molecule}: {e}")))?;
        let Some(dense) = dense else {
            return Ok(None);
        };
        let sparse_prefix = build_storage_key(
            storage_prefix,
            &molecule_key_codec::sparse_order_log_prefix(molecule),
        );
        let sparse = self
            .raw()
            .list_keys_with_prefix(&sparse_prefix)
            .await
            .map_err(|e| {
                SchemaError::InvalidData(format!(
                    "count sparse order log {molecule} during audit: {e}"
                ))
            })?
            .len() as u64;
        Ok(Some(dense.saturating_add(sparse)))
    }

    /// Sort the findings worst-first and account for the `moc:` rows the `mk:`
    /// walk never reached.
    ///
    /// `whole_keyspace` is true only when this call both started at the
    /// beginning of the `mk:` range and reached its end. `decided` holds the
    /// molecules *this call* walked, so on any other pass the molecules a
    /// sibling pass decided would be indistinguishable from molecules with no
    /// keys at all — every one of them would be miscounted here. A resumed run
    /// reports `0` rather than a number that looks precise and is wrong.
    pub(in super::super) fn finish_order_log_audit(
        mut report: OrderLogAudit,
        order_counts: &HashMap<String, u64>,
        decided: &HashSet<String>,
        whole_keyspace: bool,
    ) -> OrderLogAudit {
        report
            .short_molecules
            .sort_by_key(|row| std::cmp::Reverse(row.shortfall));
        if whole_keyspace && !report.more_remaining {
            report.order_counts_without_keys = order_counts
                .keys()
                .filter(|molecule| !decided.contains(*molecule))
                .count() as u64;
        }
        report
    }
}
