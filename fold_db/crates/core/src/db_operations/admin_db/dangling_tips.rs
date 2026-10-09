// lint:file-size-ok verbatim move out of the 9.8k-line admin_db.rs; one admin theme per file, split further when next touched
//! Locator-only population probe and dangling-tip repair.

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

    // lint:fn-size-ok verbatim move from admin_db.rs; splitting this function is separate work
    pub async fn repair_dangling_tips(
        &self,
        options: DanglingTipRepairOptions,
    ) -> Result<DanglingTipRepairReport, SchemaError> {
        use crate::atom::{atom_key_codec, atom_locator_codec, AtomKeyEncoding, AtomPartition};
        use crate::schema::types::field::FilterUtils;

        let tip_page = options
            .tip_page
            .unwrap_or(ATOM_PARTITION_REKEY_TIP_PAGE)
            .max(ATOM_PARTITION_REKEY_MIN_TIP_PAGE);
        let storage_prefix = options.storage_prefix.as_deref();
        let scan_started_at = Utc::now().to_rfc3339();
        let mk_prefix = build_storage_key(storage_prefix, "mk:");
        let mk_end = FilterUtils::create_prefix_end(&mk_prefix);
        // A `key_window` narrows the walk to one sub-range of `mk:`; a `scope`
        // narrows it to one prefix range per scoped molecule (and hash);
        // without either the one window IS `mk:`, so every path shares the
        // walk below.
        let windows: Vec<(String, String)> =
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
                        .map_or_else(|| mk_end.clone(), |e| build_storage_key(storage_prefix, e)),
                )],
                (None, None) => vec![(mk_prefix.clone(), mk_end.clone())],
            };
        let window_count = windows.len() as u64;
        let mut all_windows_exhausted = true;
        let mut saw_remaining = false;
        let mut ops = 0usize;
        let mut consecutive_failures = 0u64;
        let mut repair_groups: BTreeMap<String, Vec<RepairCandidate>> = BTreeMap::new();
        let mut report = DanglingTipRepairReport {
            dry_run: options.dry_run,
            scan_started_at: scan_started_at.clone(),
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
        };

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

        'windows: for (scan_start, scan_end) in windows {
            let mut page_start = scan_start;
            let mut skip_head: Option<String> = None;
            let mut range_exhausted = false;
            while !range_exhausted {
                let rows = self
                    .raw()
                    .inner()
                    .scan_range_paged(page_start.as_bytes(), scan_end.as_bytes(), tip_page)
                    .await
                    .map_err(|e| SchemaError::InvalidData(format!("repair scan mk: {e}")))?;
                if rows.len() < tip_page {
                    range_exhausted = true;
                }
                if rows.is_empty() {
                    break;
                }

                let head = skip_head.take();
                let mut decoded: Vec<RekeySlot> = Vec::with_capacity(rows.len());
                for (k, v) in rows {
                    let tip_key = String::from_utf8_lossy(&k).into_owned();
                    page_start = tip_key.clone();
                    if head.as_deref() == Some(tip_key.as_str()) {
                        continue;
                    }
                    let Some(base_key) = strip_storage_prefix(storage_prefix, &tip_key) else {
                        continue;
                    };
                    let Some(partition) = AtomPartition::from_record_key(base_key) else {
                        continue;
                    };
                    let Ok(val) = serde_json::from_slice::<Value>(&v) else {
                        continue;
                    };
                    let Some(uuid) = val
                        .pointer("/entry/atom_uuid")
                        .and_then(|x| x.as_str())
                        .filter(|u| !u.is_empty())
                    else {
                        continue;
                    };
                    let uuid = uuid.to_string();
                    decoded.push(RekeySlot {
                        flat_key: build_storage_key(
                            storage_prefix,
                            &atom_key_codec::flat_key(&uuid),
                        ),
                        prefixed_key: build_storage_key(
                            storage_prefix,
                            &atom_key_codec::storage_key(
                                AtomKeyEncoding::PartitionPrefix,
                                Some(&partition),
                                &uuid,
                            ),
                        ),
                        locator_key: build_storage_key(
                            storage_prefix,
                            &atom_locator_codec::locator_key(&uuid),
                        ),
                        tip_key,
                        uuid,
                        partition,
                    });
                }
                skip_head = Some(page_start.clone());

                if let Some(max) = options.max_ops {
                    let budget = max.saturating_sub(ops);
                    if decoded.len() > budget {
                        decoded.truncate(budget);
                        saw_remaining = true;
                    }
                }
                if decoded.is_empty() {
                    if saw_remaining {
                        break 'windows;
                    }
                    continue;
                }
                ops += decoded.len();
                report.tips_scanned += decoded.len() as u64;

                // Decide the healthy majority with ONE probe for the page.
                //
                // Almost every tip a full walk sees has its body exactly where the
                // tip derives it, and `classify_repairable_dangling_tip` settles
                // that case on its first `exists` alone. Asking that question one
                // tip at a time made the walk's cheap-tier round trips scale with
                // the page — the cost `rekey_page_round_trips_do_not_scale_with_page_size`
                // exists to hold down on the migration walk over the same `mk:`
                // plane, which this walk did not inherit.
                //
                // Measured on the live primary (0.23.3-1663-gd5a4bba77, 16.5 GiB
                // store, 8.68 GiB of tips): a bounded dry run cost 49.7 s for 100
                // tips and 62.2 s for 1000 within one page, i.e. ~13.9 ms of
                // marginal classify per tip against hash-scattered partitions. At
                // the ~750K-1.9M tips that plane has held, sizing a repair is a
                // multi-hour job, and `probe_locator_only_population` — a cheap
                // bounded gauge, 4096 tips over 512 windows — inherits the same
                // per-tip cost. A full `mk:` dry run exceeded the CLI's own 600 s
                // admin deadline on that store on 2026-09-07, against 2m35s for the
                // same walk on 2026-08-18, so the bounded probe is now the only
                // affordable instrument and its spread has to carry the answer.
                //
                // The probe below asks the same question on the same tier for the
                // same keys, so a slot it calls present takes exactly the branch the
                // per-slot classifier took. Only the slots it calls absent — bounded
                // by the damage, not by the store — walk on to the locator reads and
                // the live re-confirmation, which are deliberately left per-slot.
                let page_present = {
                    let mut probe_keys = Vec::with_capacity(decoded.len() * 2);
                    for slot in &decoded {
                        probe_keys.push(slot.prefixed_key.clone());
                        probe_keys.push(slot.flat_key.clone());
                    }
                    let found = self
                        .rekey_exists(&probe_keys, "repair exists body (page)")
                        .await?;
                    decoded
                        .iter()
                        .enumerate()
                        .map(|(i, _)| {
                            found.get(i * 2).copied().unwrap_or(false)
                                || found.get(i * 2 + 1).copied().unwrap_or(false)
                        })
                        .collect::<Vec<bool>>()
                };

                for (slot_idx, slot) in decoded.into_iter().enumerate() {
                    if page_present.get(slot_idx).copied().unwrap_or(false) {
                        report.skipped_body_restored += 1;
                        continue;
                    }
                    let Some(class) = self
                        .classify_repairable_dangling_tip(
                            &slot,
                            storage_prefix,
                            ExistsAuthority::Cached,
                        )
                        .await?
                    else {
                        report.skipped_body_restored += 1;
                        continue;
                    };
                    if class == UnresolvedTipClass::MisDerivedPartition {
                        report.skipped_mis_derived += 1;
                        continue;
                    }
                    // The paged walk answered from whatever tier was warm. Before a
                    // tip is called repairable — reported as such in a dry run, and
                    // deleted in an execute — confirm it against the live group.
                    // The candidate set is small by construction, so the live loads
                    // this costs are bounded by the damage, not by the store.
                    let Some(class) = self
                        .classify_repairable_dangling_tip(
                            &slot,
                            storage_prefix,
                            ExistsAuthority::Live,
                        )
                        .await?
                    else {
                        report.skipped_body_restored += 1;
                        report.rescued_by_live_probe += 1;
                        continue;
                    };
                    if class == UnresolvedTipClass::MisDerivedPartition {
                        report.skipped_mis_derived += 1;
                        report.rescued_by_live_probe += 1;
                        continue;
                    }
                    report.repairable_tips += 1;
                    if let Some(cap) = options.audit_unresolved {
                        if report.unresolved.len() < cap {
                            report.unresolved.push(UnresolvedTip {
                                tip_key: slot.tip_key.clone(),
                                atom_uuid: slot.uuid.clone(),
                                derived_partition: slot.partition.as_str().to_string(),
                                molecule_uuid: slot
                                    .partition
                                    .molecule_uuid()
                                    .unwrap_or_default()
                                    .to_string(),
                                schema: None,
                                locator_partition: self
                                    .locator_partition_for_report(&slot.locator_key)
                                    .await?,
                                class,
                            });
                        } else {
                            report.unresolved_truncated = true;
                        }
                    }
                    if options.dry_run {
                        continue;
                    }
                    let Some(key) = decode_repair_key(storage_prefix, &slot.tip_key) else {
                        report.skipped_unparseable_key += 1;
                        continue;
                    };
                    repair_groups
                        .entry(key.molecule_uuid.clone())
                        .or_default()
                        .push(RepairCandidate { slot, key });
                }

                if saw_remaining {
                    break 'windows;
                }
            }
            all_windows_exhausted &= range_exhausted;
        }

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
                        record_repair_attempt(&mut report, attempt);
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
                            saw_remaining = true;
                            break 'groups;
                        }
                    }
                }
            }
        }

        // A `break 'windows` skips the tail windows, and it only happens with
        // `saw_remaining` set, so an early stop can never read as complete.
        report.completed = all_windows_exhausted && !saw_remaining;
        if let Some(handle) = ledger {
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
        Ok(report)
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

    // lint:fn-size-ok verbatim move from admin_db.rs; splitting this function is separate work
    pub(super) async fn repair_dangling_tip_group(
        &self,
        molecule_uuid: &str,
        candidates: Vec<RepairCandidate>,
        storage_prefix: Option<&str>,
    ) -> (Vec<Result<RepairAttempt, SchemaError>>, u64) {
        let mut attempts = Vec::with_capacity(candidates.len());
        let mut ready = Vec::with_capacity(candidates.len());
        // Durable writers acquire the molecule barrier, atom body locks, then
        // exact tip locks. Keep these through the final live check and removal.
        // A caller that needs durable and publication locks takes them in that
        // order. Otherwise a Put can replace a validated tip, or a body can
        // reappear, before removal.
        let _molecule_guard = self
            .lock_molecule_commit(molecule_uuid, storage_prefix)
            .await;
        let atom_uuids: Vec<String> = candidates
            .iter()
            .map(|candidate| candidate.slot.uuid.clone())
            .collect();
        let _atom_guards = self.lock_automatic_gc_atoms(&atom_uuids).await;
        let tip_keys: Vec<String> = candidates
            .iter()
            .map(|candidate| candidate.slot.tip_key.clone())
            .collect();
        let _tip_guards = self.lock_tip_commits(&tip_keys).await;
        let _publication_guards = self.lock_tip_publications(&tip_keys).await;
        for candidate in candidates {
            match self
                .revalidate_dangling_tip(&candidate.slot, storage_prefix)
                .await
            {
                Ok(None) => ready.push(candidate),
                Ok(Some(attempt)) => attempts.push(Ok(attempt)),
                Err(e) => attempts.push(Err(e)),
            }
        }
        if ready.is_empty() {
            return (attempts, 0);
        }

        let slots: Vec<(String, String)> = ready
            .iter()
            .map(|candidate| (candidate.key.hash.clone(), candidate.key.range.clone()))
            .collect();
        let mut molecule = match self
            .load_molecule_for_storage_slot_purge(molecule_uuid, storage_prefix, &slots)
            .await
        {
            Ok(Some(molecule)) => molecule,
            Ok(None) => {
                attempts.extend(
                    ready
                        .into_iter()
                        .map(|_| Ok(RepairAttempt::MoleculeMissing)),
                );
                return (attempts, 1);
            }
            Err(e) => {
                let message = format!("load repair molecule {molecule_uuid}: {e}");
                attempts.extend(
                    ready
                        .into_iter()
                        .map(|_| Err(SchemaError::InvalidData(message.clone()))),
                );
                return (attempts, 1);
            }
        };

        let mut removable = Vec::with_capacity(ready.len());
        for candidate in ready {
            let current_uuid = molecule
                .get_atom_uuid(&candidate.key.hash, &candidate.key.range)
                .map(String::as_str);
            if current_uuid != Some(candidate.slot.uuid.as_str()) {
                attempts.push(Ok(RepairAttempt::AtomNotInMolecule));
                continue;
            }
            let removed = molecule.remove_atom_uuid(&candidate.key.hash, &candidate.key.range);
            debug_assert_eq!(removed.as_deref(), Some(candidate.slot.uuid.as_str()));
            removable.push(candidate);
        }
        if removable.is_empty() {
            return (attempts, 1);
        }

        let removed_slots: Vec<(String, String)> = removable
            .iter()
            .map(|candidate| (candidate.key.hash.clone(), candidate.key.range.clone()))
            .collect();
        // This molecule holds only the requested slots. An empty partial load
        // cannot prove that no sibling remains in the live or base generation.
        // Leave the header for later cleanup if these were the final slots.
        let remove_result = self
            .remove_molecule_keys(molecule_uuid, &molecule, &removed_slots, storage_prefix)
            .await;
        if let Err(e) = remove_result {
            let message = format!(
                "remove {} repair slot(s) from molecule {molecule_uuid}: {e}",
                removable.len()
            );
            attempts.extend(
                removable
                    .into_iter()
                    .map(|_| Err(SchemaError::InvalidData(message.clone()))),
            );
            return (attempts, 1);
        }

        for candidate in removable {
            let mut deleted_keys = 1u64;
            let locator_exists = self.raw().exists_item(&candidate.slot.locator_key).await;
            match locator_exists {
                Ok(true) => {
                    if let Err(e) = self.raw().delete_item(&candidate.slot.locator_key).await {
                        attempts.push(Err(SchemaError::InvalidData(format!(
                            "repair delete locator: {e}"
                        ))));
                        continue;
                    }
                    deleted_keys += 1;
                }
                Ok(false) => {}
                Err(e) => {
                    attempts.push(Err(SchemaError::InvalidData(format!(
                        "repair locator exists: {e}"
                    ))));
                    continue;
                }
            }
            attempts.push(Ok(RepairAttempt::Repaired { deleted_keys }));
        }
        (attempts, 1)
    }

    pub(super) async fn revalidate_dangling_tip(
        &self,
        slot: &RekeySlot,
        storage_prefix: Option<&str>,
    ) -> Result<Option<RepairAttempt>, SchemaError> {
        // The last word before anything is deleted, so it must come from the
        // live group rather than whatever tier happens to be warm.
        match self
            .classify_repairable_dangling_tip(slot, storage_prefix, ExistsAuthority::Live)
            .await?
        {
            None => return Ok(Some(RepairAttempt::BodyRestored)),
            Some(UnresolvedTipClass::MisDerivedPartition) => {
                return Ok(Some(RepairAttempt::MisDerived));
            }
            Some(_) => {}
        }

        let current: Option<Value> = self
            .raw()
            .get_item(&slot.tip_key)
            .await
            .map_err(|e| SchemaError::InvalidData(format!("repair re-read tip: {e}")))?;
        let Some(current) = current else {
            return Ok(Some(RepairAttempt::Changed));
        };
        if current.pointer("/entry/atom_uuid").and_then(|x| x.as_str()) != Some(slot.uuid.as_str())
        {
            return Ok(Some(RepairAttempt::Changed));
        }
        Ok(None)
    }

    pub(super) async fn classify_repairable_dangling_tip(
        &self,
        slot: &RekeySlot,
        storage_prefix: Option<&str>,
        authority: ExistsAuthority,
    ) -> Result<Option<UnresolvedTipClass>, SchemaError> {
        use crate::atom::atom_locator_codec;

        let present = self
            .exists_with_authority(
                &[slot.prefixed_key.clone(), slot.flat_key.clone()],
                "repair exists body",
                authority,
            )
            .await?;
        if present.into_iter().any(|ok| ok) {
            return Ok(None);
        }
        let locator: Option<Value> = self
            .raw()
            .get_item(&slot.locator_key)
            .await
            .map_err(|e| SchemaError::InvalidData(format!("repair get locator: {e}")))?;
        let Some(raw) = locator else {
            return Ok(Some(UnresolvedTipClass::NoBodyAnywhere));
        };
        let Some(located) = atom_locator_codec::decode_value(&raw) else {
            return Ok(Some(UnresolvedTipClass::UndecodableLocator));
        };
        if located.as_str() == slot.partition.as_str() {
            return Ok(Some(UnresolvedTipClass::OrphanLocator));
        }
        let located_key = build_storage_key(
            storage_prefix,
            &crate::atom::atom_key_codec::storage_key(
                crate::atom::AtomKeyEncoding::PartitionPrefix,
                Some(&located),
                &slot.uuid,
            ),
        );
        let elsewhere = self
            .exists_with_authority(&[located_key], "repair exists located body", authority)
            .await?;
        if elsewhere.first().copied().unwrap_or(false) {
            Ok(Some(UnresolvedTipClass::MisDerivedPartition))
        } else {
            Ok(Some(UnresolvedTipClass::OrphanLocator))
        }
    }

    pub(super) async fn locator_partition_for_report(
        &self,
        locator_key: &str,
    ) -> Result<Option<String>, SchemaError> {
        let raw: Option<Value> = self
            .raw()
            .get_item(locator_key)
            .await
            .map_err(|e| SchemaError::InvalidData(format!("repair report locator: {e}")))?;
        Ok(raw
            .as_ref()
            .and_then(crate::atom::atom_locator_codec::decode_value)
            .map(|p| p.as_str().to_string()))
    }
}
