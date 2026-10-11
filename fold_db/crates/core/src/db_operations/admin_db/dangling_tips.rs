//! Locator-only population probe and dangling-tip repair.

use super::*;

impl AtomStore {
    pub async fn repair_dangling_tips(
        &self,
        options: DanglingTipRepairOptions,
    ) -> Result<DanglingTipRepairReport, SchemaError> {
        let tip_page = options
            .tip_page
            .unwrap_or(ATOM_PARTITION_REKEY_TIP_PAGE)
            .max(ATOM_PARTITION_REKEY_MIN_TIP_PAGE);
        let storage_prefix = options.storage_prefix.as_deref();
        let scan_started_at = Utc::now().to_rfc3339();
        let windows = self.dangling_tip_walk_windows(&options, storage_prefix)?;
        let mut all_windows_exhausted = true;
        let mut report = new_dangling_tip_report(
            &options,
            tip_page,
            scan_started_at.clone(),
            windows.len() as u64,
        );

        let ledger = if options.dry_run {
            None
        } else {
            Some(
                self.begin_delete_ledger_row(
                    storage_prefix,
                    AtomDeleteLedgerEntry::repair_dangling_tips(
                        "repair-dangling-tips",
                        &scan_started_at,
                    ),
                )
                .await?,
            )
        };

        let mut walk = DanglingTipWalk {
            options: &options,
            storage_prefix,
            tip_page,
            ops: 0,
            saw_remaining: false,
            repair_groups: BTreeMap::new(),
        };
        for (scan_start, scan_end) in windows {
            let range_exhausted = self
                .walk_dangling_tip_window(scan_start, scan_end, &mut report, &mut walk)
                .await?;
            // An early stop skips the tail windows; it only happens with
            // `saw_remaining` set, so it can never read as complete.
            if walk.saw_remaining {
                break;
            }
            all_windows_exhausted &= range_exhausted;
        }
        let DanglingTipWalk {
            mut saw_remaining,
            repair_groups,
            ..
        } = walk;

        saw_remaining |= self
            .repair_walked_groups(repair_groups, storage_prefix, &mut report)
            .await;

        // A `break` out of the window loop skips the tail windows, and it only
        // happens with `saw_remaining` set, so an early stop can never read as
        // complete.
        report.completed = all_windows_exhausted && !saw_remaining;
        if let Some(handle) = ledger {
            self.commit_repair_ledger(handle, &report).await;
        }
        Ok(report)
    }

    /// Repair the groups the walk collected. Returns true when repeated
    /// failures stopped the run early, so the report cannot read as complete.
    async fn repair_walked_groups(
        &self,
        repair_groups: BTreeMap<String, Vec<RepairCandidate>>,
        storage_prefix: Option<&str>,
        report: &mut DanglingTipRepairReport,
    ) -> bool {
        let mut consecutive_failures = 0u64;
        // The repairable set is bounded by the damage, not by store size. Hold
        // that small set until the walk finishes so exact slots from one
        // molecule share one batch read.
        'groups: for (molecule_uuid, candidates) in repair_groups {
            let (attempts, molecule_loads) = self
                .repair_dangling_tip_group(&molecule_uuid, candidates, storage_prefix)
                .await;
            report.molecule_loads += molecule_loads;
            for attempt in attempts {
                match attempt {
                    Ok(attempt) => {
                        consecutive_failures = 0;
                        record_repair_attempt(report, attempt);
                    }
                    Err(e) => {
                        report.failed_repairs += 1;
                        consecutive_failures += 1;
                        tracing::warn!(
                            "repair-dangling-tips: tip repair failed ({} consecutive): {e}",
                            consecutive_failures
                        );
                        if consecutive_failures >= MAX_CONSECUTIVE_REPAIR_FAILURES {
                            report.aborted_on_repeated_failures = true;
                            return true;
                        }
                    }
                }
            }
        }
        false
    }

    async fn commit_repair_ledger(
        &self,
        handle: DeleteLedgerHandle,
        report: &DanglingTipRepairReport,
    ) {
        self.commit_delete_ledger_row(handle, |entry| {
            entry.tips_repaired = report.tips_repaired;
            entry.storage_keys_deleted = report.storage_keys_deleted;
            // Transient race residue only. Permanent structural skips land
            // in their own ledger columns so an operator reading a row can
            // tell re-running apart from residue that will never clear.
            entry.tips_skipped_changed = report.skipped_changed;
            entry.tips_skipped_molecule_missing = report.skipped_molecule_missing;
            entry.tips_skipped_atom_not_in_molecule = report.skipped_atom_not_in_molecule;
            entry.atoms_scanned = report.tips_scanned;
        })
        .await;
        let _ = self.flush().await;
    }

    /// The `[start, end)` ranges the dangling-tip walk covers.
    fn dangling_tip_walk_windows(
        &self,
        options: &DanglingTipRepairOptions,
        storage_prefix: Option<&str>,
    ) -> Result<Vec<(String, String)>, SchemaError> {
        use crate::schema::types::field::FilterUtils;

        let mk_prefix = build_storage_key(storage_prefix, "mk:");
        let mk_end = FilterUtils::create_prefix_end(&mk_prefix);
        // A `key_window` narrows the walk to one sub-range of `mk:`; a `scope`
        // narrows it to one prefix range per scoped molecule (and hash);
        // without either the one window IS `mk:`, so every path shares the
        // walk.
        Ok(
            match (options.scope.as_ref(), options.key_window.as_ref()) {
                (Some(_), Some(_)) => {
                    return Err(SchemaError::InvalidData(
                        "repair-dangling-tips: scope and key_window are exclusive".to_string(),
                    ))
                }
                (Some(scope), None) => self.dangling_tip_scope_windows(scope, storage_prefix)?,
                (None, Some((start, end))) => vec![(
                    build_storage_key(storage_prefix, start),
                    end.as_ref()
                        .map_or_else(|| mk_end, |e| build_storage_key(storage_prefix, e)),
                )],
                (None, None) => vec![(mk_prefix, mk_end)],
            },
        )
    }

    /// Page through one `mk:` window, classifying each page's tips. Returns
    /// whether the window's range was read to its end. Stops early (with
    /// `walk.saw_remaining` set) once the `max_ops` budget is spent.
    async fn walk_dangling_tip_window(
        &self,
        scan_start: String,
        scan_end: String,
        report: &mut DanglingTipRepairReport,
        walk: &mut DanglingTipWalk<'_>,
    ) -> Result<bool, SchemaError> {
        let mut page_start = scan_start;
        let mut skip_head: Option<String> = None;
        let mut range_exhausted = false;
        while !range_exhausted {
            let rows = self
                .raw()
                .inner()
                .scan_range_paged(page_start.as_bytes(), scan_end.as_bytes(), walk.tip_page)
                .await
                .map_err(|e| SchemaError::InvalidData(format!("repair scan mk: {e}")))?;
            if rows.len() < walk.tip_page {
                range_exhausted = true;
            }
            if rows.is_empty() {
                break;
            }

            let head = skip_head.take();
            let decoded = decode_dangling_tip_page(
                rows,
                head.as_deref(),
                &mut page_start,
                walk.storage_prefix,
            );
            skip_head = Some(page_start.clone());

            self.process_dangling_tip_page(decoded, report, walk)
                .await?;
            if walk.saw_remaining {
                return Ok(range_exhausted);
            }
        }
        Ok(range_exhausted)
    }

    /// Apply the `max_ops` budget to one decoded page, then classify each
    /// slot. Sets `walk.saw_remaining` when the budget cut the page short.
    async fn process_dangling_tip_page(
        &self,
        mut decoded: Vec<RekeySlot>,
        report: &mut DanglingTipRepairReport,
        walk: &mut DanglingTipWalk<'_>,
    ) -> Result<(), SchemaError> {
        if let Some(max) = walk.options.max_ops {
            let budget = max.saturating_sub(walk.ops);
            if decoded.len() > budget {
                decoded.truncate(budget);
                walk.saw_remaining = true;
            }
        }
        if decoded.is_empty() {
            return Ok(());
        }
        walk.ops += decoded.len();
        report.tips_scanned += decoded.len() as u64;

        let page_present = self.probe_page_bodies_present(&decoded).await?;
        for (slot_idx, slot) in decoded.into_iter().enumerate() {
            if page_present.get(slot_idx).copied().unwrap_or(false) {
                report.skipped_body_restored += 1;
                continue;
            }
            self.classify_dangling_tip_slot(slot, report, walk).await?;
        }
        Ok(())
    }

    /// Storage-scoped `[start, end)` prefix ranges for a
    /// [`DanglingTipRepairScope`]: one per molecule-uuid spelling (and storage
    /// hash candidate when a HashKey is given). Sorted and de-duplicated, so a
    /// molecule named twice is walked once.
    pub(super) fn dangling_tip_scope_windows(
        &self,
        scope: &DanglingTipRepairScope,
        storage_prefix: Option<&str>,
    ) -> Result<Vec<(String, String)>, SchemaError> {
        use crate::schema::types::field::FilterUtils;

        let codec = self.key_codec();
        let mut prefixes: Vec<String> = Vec::new();
        for molecule_uuid in &scope.molecule_uuids {
            let bare = match scope.hash_key.as_deref() {
                Some(hash) => codec
                    .api_hash_range_scan_prefixes_for_read(molecule_uuid, hash)
                    .map_err(|e| {
                        SchemaError::InvalidData(format!(
                            "repair-dangling-tips: hash key prefix for {molecule_uuid}: {e}"
                        ))
                    })?,
                None => codec.molecule_record_prefixes_for_read(molecule_uuid),
            };
            prefixes.extend(bare.iter().map(|p| build_storage_key(storage_prefix, p)));
        }
        prefixes.sort();
        prefixes.dedup();
        Ok(prefixes
            .into_iter()
            .map(|start| {
                let end = FilterUtils::create_prefix_end(&start);
                (start, end)
            })
            .collect())
    }
}

/// Mutable state shared by the dangling-tip walk's helpers.
struct DanglingTipWalk<'a> {
    options: &'a DanglingTipRepairOptions,
    storage_prefix: Option<&'a str>,
    tip_page: usize,
    /// Tips counted against `options.max_ops` so far.
    ops: usize,
    /// Set when the `max_ops` budget cut the walk short.
    saw_remaining: bool,
    /// Repairable slots grouped by molecule, held until the walk finishes.
    repair_groups: BTreeMap<String, Vec<RepairCandidate>>,
}

fn new_dangling_tip_report(
    options: &DanglingTipRepairOptions,
    tip_page: usize,
    scan_started_at: String,
    window_count: u64,
) -> DanglingTipRepairReport {
    DanglingTipRepairReport {
        dry_run: options.dry_run,
        scan_started_at,
        tip_page: tip_page as u64,
        tips_scanned: 0,
        repairable_tips: 0,
        tips_repaired: 0,
        skipped_changed: 0,
        skipped_molecule_missing: 0,
        skipped_atom_not_in_molecule: 0,
        skipped_body_restored: 0,
        skipped_mis_derived: 0,
        skipped_unparseable_key: 0,
        skipped_unrepairable: 0,
        refused_molecules: Vec::new(),
        rescued_by_live_probe: 0,
        failed_repairs: 0,
        aborted_on_repeated_failures: false,
        molecule_loads: 0,
        storage_keys_deleted: 0,
        completed: false,
        unresolved: Vec::new(),
        unresolved_truncated: false,
        scope: options
            .scope
            .as_ref()
            .map(|scope| DanglingTipRepairScopeReport {
                schemas: Vec::new(),
                molecule_uuids: scope.molecule_uuids.clone(),
                hash_key: scope.hash_key.clone(),
                key_ranges: window_count,
            }),
    }
}

mod classify;
use classify::decode_dangling_tip_page;
mod probe;
mod repair_group;
