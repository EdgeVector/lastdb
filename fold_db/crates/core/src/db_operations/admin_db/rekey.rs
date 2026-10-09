// lint:file-size-ok verbatim move out of the 9.8k-line admin_db.rs; one admin theme per file, split further when next touched
//! Atom partition-prefix rekey and its checkpoint.

use super::*;

impl AtomStore {
    /// Resumable dual-readable rekey: copy flat atom bodies onto
    /// `atom:{partition}\0{uuid}` + locator, optionally drop flats after
    /// verify.
    ///
    /// Walks live `mk:` tips (the only durable source of uuid→partition), never
    /// scans atoms for placement. Orphans (no tip) stay flat by design.
    ///
    /// **Both addressings stay readable** for the duration when
    /// `remove_flat` is false: new prefixed keys are additive. Deletes of the
    /// old flat key happen only after the prefixed body is verified present,
    /// and only when `remove_flat` is true — never a silent in-place rewrite
    /// or list-orphan-delete-on-miss.
    ///
    /// Progress is checkpointed at [`ATOM_PARTITION_REKEY_CHECKPOINT_KEY`] after
    /// **every page**, so a killed pass — including one killed mid-call by an
    /// authorized restart — resumes within a page of where it stopped rather
    /// than losing the whole call's cursor progress. Independent of the
    /// store's current [`crate::atom::AtomKeyEncoding`]: the target key shape is
    /// always `PartitionPrefix` (migration writes the destination layout;
    /// `Flat` homes stay readable via the dual-write until the encoding flips).
    ///
    /// **Cost is `O(tips walked)`, not `O(tips in store)`.** The walk keyset-pages
    /// the `mk:` range starting at the cursor. It used to `scan_prefix` every tip,
    /// JSON-parse every value, sort the whole vector and then skip past the cursor
    /// in memory — so each resumed pass re-read the entire keyspace, the job was
    /// quadratic in tip count, and a single pass transiently allocated a vector
    /// proportional to the whole store on a node whose resident budget is the
    /// binding constraint.
    // lint:fn-size-ok verbatim move from admin_db.rs; splitting this function is separate work
    pub async fn rekey_atoms_to_partition_prefix(
        &self,
        options: AtomPartitionRekeyOptions,
    ) -> Result<AtomPartitionRekeyReport, SchemaError> {
        use crate::atom::{atom_key_codec, atom_locator_codec, AtomKeyEncoding, AtomPartition};
        use crate::schema::types::field::FilterUtils;

        // Bounds the transient allocation of a pass to the page, independent of
        // store size.
        //
        // **Floored at TWO, and one is not a valid page size.** The range start
        // is inclusive, so every page after the first re-reads the cursor row and
        // drops it as `skip_head`; a page of one row is therefore *only* the
        // cursor row, decodes to nothing, and leaves `page_start` where it was.
        // `rows.len() < tip_page` is the sole range-exhausted signal and a
        // one-row page always satisfies `1 == tip_page`, so the walk would spin
        // forever without advancing or completing — a hang inside an admin route,
        // not a slow pass. A resuming page must hold the cursor row *plus* at
        // least one new row. Synthesising a successor key would avoid the re-read
        // and this floor with it, but that means encoding an assumption about how
        // the backend orders key bounds, which this walk deliberately does not
        // make.
        let tip_page = options
            .tip_page
            .unwrap_or(ATOM_PARTITION_REKEY_TIP_PAGE)
            .max(ATOM_PARTITION_REKEY_MIN_TIP_PAGE);

        let storage_prefix = options.storage_prefix.as_deref();
        let checkpoint_key = build_storage_key(storage_prefix, ATOM_PARTITION_REKEY_CHECKPOINT_KEY);

        if options.progress_only {
            // Read the durable checkpoint and answer. No scan, no cursor write.
            let checkpoint = self
                .load_atom_partition_rekey_checkpoint(&checkpoint_key)
                .await?;
            return Ok(AtomPartitionRekeyReport {
                dry_run: true,
                remove_flat: options.remove_flat,
                tips_scanned: 0,
                slots_considered: 0,
                dual_written: 0,
                would_dual_write: 0,
                already_prefixed: 0,
                missing_body: 0,
                flat_removed: 0,
                flat_retained_unverified: 0,
                completed: checkpoint.completed,
                scan_reached_end: checkpoint.completed,
                tips_walked_total: checkpoint.tips_walked_total,
                tip_page: tip_page as u64,
                unresolved_mis_derived: 0,
                unresolved_orphan_locator: 0,
                unresolved_undecodable_locator: 0,
                unresolved_no_body: 0,
                unresolved: Vec::new(),
                unresolved_truncated: false,
                would_dual_write_tips: Vec::new(),
                would_dual_write_truncated: false,
                checkpoint,
            });
        }

        let mut checkpoint = if options.dry_run {
            // Dry-run does not trust / advance the durable cursor — report a
            // plan from the start so operators can re-run safely.
            AtomPartitionRekeyCheckpoint {
                version: 1,
                ..Default::default()
            }
        } else {
            self.load_atom_partition_rekey_checkpoint(&checkpoint_key)
                .await?
        };
        // A completed job that is re-invoked (e.g. second phase with
        // `remove_flat=true`, or a re-verify after encoding flip) must walk
        // tips again. Keeping the exclusive cursor at the last tip would skip
        // every row and make remove_flat a no-op.
        if checkpoint.completed {
            checkpoint.after_tip_key = None;
            checkpoint.completed = false;
        }

        let mk_prefix = build_storage_key(storage_prefix, "mk:");
        let mk_end = FilterUtils::create_prefix_end(&mk_prefix);

        // Keyset cursor. `after_tip_key` is exclusive, but the range start is
        // inclusive, so start *at* the cursor and drop that one row: the
        // alternative — synthesising a successor key — would have to assume how
        // the backend encodes key bounds, and this does not.
        let resume_after = checkpoint.after_tip_key.clone();
        let mut page_start = resume_after.clone().unwrap_or_else(|| mk_prefix.clone());

        let mut report = AtomPartitionRekeyReport {
            dry_run: options.dry_run,
            remove_flat: options.remove_flat,
            tips_scanned: 0,
            slots_considered: 0,
            dual_written: 0,
            would_dual_write: 0,
            already_prefixed: 0,
            missing_body: 0,
            flat_removed: 0,
            flat_retained_unverified: 0,
            completed: false,
            scan_reached_end: false,
            tips_walked_total: checkpoint.tips_walked_total,
            tip_page: tip_page as u64,
            unresolved_mis_derived: 0,
            unresolved_orphan_locator: 0,
            unresolved_undecodable_locator: 0,
            unresolved_no_body: 0,
            unresolved: Vec::new(),
            unresolved_truncated: false,
            would_dual_write_tips: Vec::new(),
            would_dual_write_truncated: false,
            checkpoint: checkpoint.clone(),
        };

        // Baseline of the durable counters this call accumulates onto. Held
        // separately so the checkpoint can be re-derived and persisted after
        // every page without double-counting.
        let baseline = checkpoint.clone();

        let mut ops = 0usize;
        let mut last_tip: Option<String> = checkpoint.after_tip_key.clone();
        let mut saw_remaining = false;
        let mut range_exhausted = false;
        // Head row to drop from the next page: the range start is inclusive, and
        // both the resume cursor and each page boundary are keys already walked.
        let mut skip_head = resume_after.clone();

        while !range_exhausted {
            let rows = self
                .raw()
                .inner()
                .scan_range_paged(page_start.as_bytes(), mk_end.as_bytes(), tip_page)
                .await
                .map_err(|e| SchemaError::InvalidData(format!("rekey scan mk: {e}")))?;
            if rows.len() < tip_page {
                range_exhausted = true;
            }
            if rows.is_empty() {
                break;
            }
            // Advance the page cursor before decoding: rows this pass cannot
            // interpret (unparseable tip, no atom_uuid) must still be walked
            // past, exactly as the unpaginated walk skipped them.
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
                    flat_key: build_storage_key(storage_prefix, &atom_key_codec::flat_key(&uuid)),
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

            // `max_ops` now bites at page granularity. A tip trimmed off the end
            // of the page is *not* walked, so the cursor never advances over it
            // and the next pass picks it up again — the same contract the
            // one-tip-at-a-time break had.
            if let Some(max) = options.max_ops {
                let budget = max.saturating_sub(ops);
                if decoded.len() > budget {
                    decoded.truncate(budget);
                    saw_remaining = true;
                }
            }

            if !decoded.is_empty() {
                ops += decoded.len();
                last_tip = Some(decoded[decoded.len() - 1].tip_key.clone());
                self.rekey_page(&decoded, &options, storage_prefix, &mut report)
                    .await?;
            }

            // A finished page is a durable resume point: persist the cursor so a
            // restart mid-call resumes at the next page rather than redoing this
            // call. The cursor only ever names a tip whose work is done.
            if !options.dry_run {
                checkpoint = self
                    .persist_rekey_checkpoint(&checkpoint_key, &baseline, &report, &last_tip, false)
                    .await?;
            }

            if saw_remaining {
                break;
            }
        }

        // The walk reached the end of the keyspace iff it was not cut short by
        // max_ops. That is a statement about the *scan*, and it is all a dry run
        // can ever establish.
        report.scan_reached_end = !saw_remaining;
        // The migration is complete only when an executing pass walked to the
        // end. A dry run writes nothing, so it cannot complete anything.
        report.completed = report.scan_reached_end && !options.dry_run;

        if options.dry_run {
            checkpoint.after_tip_key = last_tip.or(checkpoint.after_tip_key);
            checkpoint.tips_scanned = report.tips_scanned;
            checkpoint.tips_walked_total = baseline
                .tips_walked_total
                .saturating_add(report.tips_scanned);
            checkpoint.already_prefixed = baseline
                .already_prefixed
                .saturating_add(report.already_prefixed);
            checkpoint.missing_body = baseline.missing_body.saturating_add(report.missing_body);
            checkpoint.flat_removed = baseline.flat_removed.saturating_add(report.flat_removed);
            checkpoint.completed = false;
            checkpoint.version = 1;
        } else {
            checkpoint = self
                .persist_rekey_checkpoint(
                    &checkpoint_key,
                    &baseline,
                    &report,
                    &last_tip,
                    report.completed,
                )
                .await?;
            let _ = self.flush().await;
        }
        report.tips_walked_total = checkpoint.tips_walked_total;
        report.checkpoint = checkpoint;

        Ok(report)
    }

    /// Migrate one decoded page of tips in a bounded number of store round
    /// trips, rather than four per tip.
    ///
    /// The phase order is load-bearing, not cosmetic. **A flat key may only be
    /// deleted once the prefixed copy is verified present**, so the batched
    /// delete is the last phase of the page — after the batched write and the
    /// batched verify — instead of sitting next to the slot that decided it. The
    /// sequential walk got that ordering for free by doing one atom at a time;
    /// batching has to arrange it.
    ///
    /// Every slot in `page` is walked. The caller has already advanced its
    /// cursor to the page's last tip, so this must not skip a slot on any
    /// per-slot condition.
    // lint:fn-size-ok verbatim move from admin_db.rs; splitting this function is separate work
    pub(super) async fn rekey_page(
        &self,
        page: &[RekeySlot],
        options: &AtomPartitionRekeyOptions,
        storage_prefix: Option<&str>,
        report: &mut AtomPartitionRekeyReport,
    ) -> Result<(), SchemaError> {
        use crate::atom::{atom_locator_codec, AtomPartition};
        use std::collections::BTreeMap;

        report.tips_scanned += page.len() as u64;
        report.slots_considered += page.len() as u64;

        // (1) One existence probe for the page's prefixed bodies. Deliberately an
        // *existence* probe and not a batched get: an atom body runs to
        // `LASTDB_MAX_ATOM_CONTENT_BYTES`, so answering "is it already there?"
        // for a full page must not pull a page of bodies into memory.
        let prefixed_keys: Vec<String> = page.iter().map(|s| s.prefixed_key.clone()).collect();
        let mut prefixed_present = self
            .rekey_exists(&prefixed_keys, "rekey exists prefixed")
            .await?;

        // Two slots of one page can address the same body — a deduplicated atom
        // referenced from several tips that derive the same partition. The
        // sequential walk saw the second slot *after* the first had written it,
        // and counted it as already-prefixed; a page-wide probe taken before any
        // write sees both as absent. Fold the duplicates back so the write set
        // holds each body once and the counters match the one-at-a-time walk.
        let mut first_writer: HashSet<&str> = HashSet::with_capacity(page.len());
        for slot in 0..page.len() {
            if prefixed_present[slot] {
                continue;
            }
            if !first_writer.insert(page[slot].prefixed_key.as_str()) {
                prefixed_present[slot] = true;
            }
        }

        // Already-migrated slots need only their locator (and, under
        // remove_flat, their flat key) reconciled; the rest need a source body.
        let already: Vec<usize> = (0..page.len()).filter(|&i| prefixed_present[i]).collect();
        let candidates: Vec<usize> = (0..page.len()).filter(|&i| !prefixed_present[i]).collect();
        report.already_prefixed += already.len() as u64;

        // (2) One batched get of just the flat bodies that still need copying.
        let candidate_flat_keys: Vec<String> = candidates
            .iter()
            .map(|&i| page[i].flat_key.clone())
            .collect();
        let mut bodies: Vec<Option<Value>> = if candidate_flat_keys.is_empty() {
            Vec::new()
        } else {
            self.raw()
                .get_items::<Value>(&candidate_flat_keys)
                .await
                .map_err(|e| SchemaError::InvalidData(format!("rekey get flat: {e}")))?
        };

        let mut writable: Vec<(usize, Value)> = Vec::with_capacity(candidates.len());
        let mut unresolved: Vec<usize> = Vec::new();
        for (pos, &slot) in candidates.iter().enumerate() {
            match bodies[pos].take() {
                Some(body) => writable.push((slot, body)),
                None => unresolved.push(slot),
            }
        }
        report.missing_body += unresolved.len() as u64;

        if let Some(cap) = options.audit_unresolved {
            self.rekey_audit_page(page, &unresolved, storage_prefix, cap, report)
                .await?;
        }

        if options.dry_run {
            // A plan counts remaining work under its own name; `dual_written`
            // stays 0 so no reader can mistake this report for progress.
            report.would_dual_write += writable.len() as u64;
            if let Some(cap) = options.audit_unresolved {
                // Counters count every flat-only tip; only detail rows are
                // capped — same contract as the unresolved audit.
                for (slot, _) in &writable {
                    if report.would_dual_write_tips.len() < cap {
                        report.would_dual_write_tips.push(WouldDualWriteTip {
                            tip_key: page[*slot].tip_key.clone(),
                            atom_uuid: page[*slot].uuid.clone(),
                            derived_partition: page[*slot].partition.as_str().to_string(),
                        });
                    } else {
                        report.would_dual_write_truncated = true;
                    }
                }
            }
            if options.remove_flat {
                // What a real pass would delete: every body it would write (whose
                // flat key it just read, so it is certainly there), plus the
                // already-migrated slots that still carry one.
                let already_flat: Vec<String> =
                    already.iter().map(|&i| page[i].flat_key.clone()).collect();
                let present = self
                    .rekey_exists(&already_flat, "rekey exists flat")
                    .await?;
                report.flat_removed +=
                    writable.len() as u64 + present.iter().filter(|&&p| p).count() as u64;
            }
            return Ok(());
        }

        // (3) Locator reconciliation for the already-migrated slots: a crash
        // between an older experimental body write and its locator would leave
        // the body reachable only by a tip-derived guess, so the sequential walk
        // repaired it here. Keep that, batched.
        let already_locator_keys: Vec<String> = already
            .iter()
            .map(|&i| page[i].locator_key.clone())
            .collect();
        let already_locator_present = self
            .rekey_exists(&already_locator_keys, "rekey exists locator")
            .await?;

        // One write set for the page: bodies plus locators. The locator is keyed
        // by uuid alone, so several slots of one page can target it under
        // different partitions; dedupe to one entry per key so the batch does not
        // depend on the backend applying duplicates in order. Whichever partition
        // wins names one whose body this same batch writes (or already found), so
        // the hint stays valid either way.
        let mut writes: Vec<(String, Value)> = Vec::with_capacity(writable.len() * 2);
        let mut locators: BTreeMap<String, AtomPartition> = BTreeMap::new();
        for (pos, &slot) in already.iter().enumerate() {
            if !already_locator_present[pos] {
                locators.insert(page[slot].locator_key.clone(), page[slot].partition.clone());
            }
        }
        let mut written: Vec<usize> = Vec::with_capacity(writable.len());
        for (slot, body) in writable {
            writes.push((page[slot].prefixed_key.clone(), body));
            locators.insert(page[slot].locator_key.clone(), page[slot].partition.clone());
            written.push(slot);
        }
        report.dual_written += written.len() as u64;
        for (key, partition) in locators {
            writes.push((key, atom_locator_codec::encode_value(&partition)));
        }

        if !writes.is_empty() {
            // Body + locator in one batch — same transactional contract as
            // `batch_store_atoms_located`. Never rewrite the flat key in place.
            self.raw()
                .batch_put_items(writes)
                .await
                .map_err(|e| SchemaError::InvalidData(format!("rekey dual-write: {e}")))?;
        }

        // (4) Verify every body this page wrote, before anything can delete a
        // flat key. One probe for the page — and a probe rather than a re-read,
        // for the same reason phase (1) is: proving a page of bodies landed must
        // not cost a page of bodies in memory.
        //
        // This probe stays on the cheap tier, and the reason is worth writing
        // down because phase (5) does NOT.
        //
        // `written` is every slot whose flat body this page read successfully
        // and whose prefixed body `batch_put_items` just acknowledged. The check
        // here is defence in depth against a store that acknowledged a write it
        // did not take — and no read tier detects that store, because a probe
        // answered from a cache the write itself populated is exactly as wrong
        // as the acknowledgement was. Paying a live group load per key to
        // re-ask a question the write already answered would make the walk's
        // single-key round trips scale with the page size, which is the cost
        // `rekey_page_round_trips_do_not_scale_with_page_size` exists to hold
        // down: at ~12.4 ms per single-key op against hash-scattered
        // partitions, 1.46M tips is a five-hour job.
        //
        // Phase (5)'s `already` population has no such write behind it, which
        // is why it, and only it, pays for confirmation.
        if !written.is_empty() {
            let written_keys: Vec<String> = written
                .iter()
                .map(|&i| page[i].prefixed_key.clone())
                .collect();
            let verified = self.rekey_exists(&written_keys, "rekey verify").await?;
            if let Some(pos) = verified.iter().position(|ok| !ok) {
                let uuid = &page[written[pos]].uuid;
                return Err(SchemaError::InvalidData(format!(
                    "rekey verify failed for atom {uuid}: prefixed body missing after write"
                )));
            }
        }

        // (5) Only now may a flat key go: the bodies just verified, plus
        // already-migrated slots that still carry one. Deduped, because one uuid
        // can be both freshly written under one partition and already migrated
        // under another, and a key counted twice would overstate `flat_removed`.
        if options.remove_flat {
            let already_flat: Vec<String> =
                already.iter().map(|&i| page[i].flat_key.clone()).collect();
            let already_flat_present = self
                .rekey_exists_confirmed(&already_flat, "rekey exists flat")
                .await?;
            // Re-probe the prefixed body for the already-migrated slots.
            //
            // `already` is not all one thing. Most of it comes from the phase
            // (1) existence probe — those slots really do have a prefixed body.
            // But the dedup loop *also* pushes slots in on a promise: when two
            // slots of one page address the same prefixed key, the second is
            // marked present before anything is written, on the expectation
            // that the first writer creates it. When that write never happens
            // — the first writer's flat body read came back `None`, so it
            // landed in `unresolved` instead of `written` — the promise is
            // never kept, and nothing in phases (3) or (4) notices: the verify
            // only covers `written`.
            //
            // That leaves a slot in `already` with no prefixed body at all. If
            // a concurrent writer lands the flat body in the meantime (exactly
            // the window this walk runs in — a `partition: None` writer under
            // partition-prefix encoding is the population `would_dual_write`
            // exists to count), the phase (5) flat probe now says present, and
            // the flat body is deleted with no prefixed copy anywhere. That is
            // the only copy, and the tip that names it stays live.
            //
            // So the delete re-validates its own precondition instead of
            // trusting phase (1): reclaim a flat key only when the prefixed
            // body is confirmed present at delete time. The freshly `written`
            // slots need no re-probe — phase (4) verified them after the write,
            // which is the same guarantee one step earlier.
            let already_prefixed: Vec<String> = already
                .iter()
                .map(|&i| page[i].prefixed_key.clone())
                .collect();
            // ...and confirm the presents on the live group. This answer is
            // the delete's precondition — `already_prefixed_present[pos]` true
            // is exactly what moves a flat key into `doomed` — so a cached
            // "present" that the group index does not back would destroy the
            // only copy. Narrowing costs one live load per key about to be
            // reclaimed, and can only ever reclaim fewer.
            let already_prefixed_present = self
                .rekey_exists_confirmed(&already_prefixed, "rekey reverify prefixed")
                .await?;
            let written_flat: Vec<String> = written
                .iter()
                .map(|&slot| page[slot].flat_key.clone())
                .collect();
            let (doomed, retained) = flat_keys_to_reclaim(
                &written_flat,
                &already_flat,
                &already_flat_present,
                &already_prefixed_present,
            );
            report.flat_retained_unverified += retained;
            if !doomed.is_empty() {
                report.flat_removed += doomed.len() as u64;
                self.raw()
                    .batch_delete_keys(doomed.into_iter().collect())
                    .await
                    .map_err(|e| SchemaError::InvalidData(format!("rekey delete flat: {e}")))?;
            }
        }

        Ok(())
    }

    /// Classify a page's tips that resolved to no atom body. Reads only — a
    /// mis-derived body is reported, not moved.
    ///
    /// Two batched reads for the whole page: the locators, then (only for the
    /// locators that disagree with the tip-derived partition) whether the body is
    /// simply somewhere else.
    // lint:fn-size-ok verbatim move from admin_db.rs; splitting this function is separate work
    pub(super) async fn rekey_audit_page(
        &self,
        page: &[RekeySlot],
        unresolved: &[usize],
        storage_prefix: Option<&str>,
        cap: usize,
        report: &mut AtomPartitionRekeyReport,
    ) -> Result<(), SchemaError> {
        use crate::atom::{atom_key_codec, atom_locator_codec, AtomKeyEncoding};

        if unresolved.is_empty() {
            return Ok(());
        }
        // The locator is the only address in the store that does not come from
        // the tip key, so it is the only way to tell a purged atom from one
        // written under a partition this pass never derives.
        let locator_keys: Vec<String> = unresolved
            .iter()
            .map(|&i| page[i].locator_key.clone())
            .collect();
        let locator_rows = self
            .raw()
            .get_items::<Value>(&locator_keys)
            .await
            .map_err(|e| SchemaError::InvalidData(format!("rekey audit get locator: {e}")))?;

        let mut elsewhere_pos: Vec<usize> = Vec::new();
        let mut elsewhere_keys: Vec<String> = Vec::new();
        for (pos, raw) in locator_rows.iter().enumerate() {
            let Some(located) = raw.as_ref().and_then(atom_locator_codec::decode_value) else {
                continue;
            };
            let slot = unresolved[pos];
            if located.as_str() == page[slot].partition.as_str() {
                continue;
            }
            elsewhere_pos.push(pos);
            elsewhere_keys.push(build_storage_key(
                storage_prefix,
                &atom_key_codec::storage_key(
                    AtomKeyEncoding::PartitionPrefix,
                    Some(&located),
                    &page[slot].uuid,
                ),
            ));
        }
        let elsewhere_present = self
            .rekey_exists(&elsewhere_keys, "rekey audit exists located body")
            .await?;
        let mut body_elsewhere = vec![false; unresolved.len()];
        for (pos, present) in elsewhere_pos.into_iter().zip(elsewhere_present) {
            body_elsewhere[pos] = present;
        }

        for (pos, &slot) in unresolved.iter().enumerate() {
            let located = locator_rows[pos]
                .as_ref()
                .and_then(atom_locator_codec::decode_value);
            let (class, locator_str) = match (&locator_rows[pos], located) {
                (None, _) => (UnresolvedTipClass::NoBodyAnywhere, None),
                (Some(_), None) => (UnresolvedTipClass::UndecodableLocator, None),
                (Some(_), Some(located)) => {
                    let located_str = located.as_str().to_string();
                    // A locator agreeing with the tip while the body is absent is
                    // an orphan — the page's prefixed probe already established
                    // absence, so no second read is needed for that case.
                    let class = if located.as_str() != page[slot].partition.as_str()
                        && body_elsewhere[pos]
                    {
                        UnresolvedTipClass::MisDerivedPartition
                    } else {
                        UnresolvedTipClass::OrphanLocator
                    };
                    (class, Some(located_str))
                }
            };

            match class {
                UnresolvedTipClass::MisDerivedPartition => report.unresolved_mis_derived += 1,
                UnresolvedTipClass::OrphanLocator => report.unresolved_orphan_locator += 1,
                UnresolvedTipClass::UndecodableLocator => {
                    report.unresolved_undecodable_locator += 1;
                }
                UnresolvedTipClass::NoBodyAnywhere => report.unresolved_no_body += 1,
            }

            // Counters classify every unresolved tip; only the detail rows are
            // capped.
            if report.unresolved.len() < cap {
                report.unresolved.push(UnresolvedTip {
                    tip_key: page[slot].tip_key.clone(),
                    atom_uuid: page[slot].uuid.clone(),
                    derived_partition: page[slot].partition.as_str().to_string(),
                    molecule_uuid: page[slot]
                        .partition
                        .molecule_uuid()
                        .unwrap_or_default()
                        .to_string(),
                    schema: None,
                    locator_partition: locator_str,
                    class,
                });
            } else {
                report.unresolved_truncated = true;
            }
        }
        Ok(())
    }

    /// Batched existence probe with the rekey's error framing, tolerant of the
    /// empty key list several phases legitimately produce.
    pub(super) async fn rekey_exists(
        &self,
        keys: &[String],
        ctx: &str,
    ) -> Result<Vec<bool>, SchemaError> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        self.raw()
            .exists_items(keys)
            .await
            .map_err(|e| SchemaError::InvalidData(format!("{ctx}: {e}")))
    }

    /// Existence probe that always resolves the **live** group.
    ///
    /// [`crate::storage::traits::KvStore::exists_many`] is allowed to answer a
    /// *cold* group from the in-memory key-index cache or the on-disk key
    /// sidecar instead of loading the group. Its answer is therefore a function
    /// of what happens to be resident, not only of what is durable — the right
    /// trade for a page-at-a-time scan of millions of tips, and the wrong one
    /// for the last check before a delete.
    ///
    /// The singular `exists` takes the live handle per key, so it answers from
    /// the group's real index. That costs one group load per key, which is why
    /// only the candidate set reaches this path: by then it is the handful of
    /// tips a whole pass judged repairable, not the millions it walked.
    pub(super) async fn rekey_exists_live(
        &self,
        keys: &[String],
        ctx: &str,
    ) -> Result<Vec<bool>, SchemaError> {
        let mut found = Vec::with_capacity(keys.len());
        for key in keys {
            found.push(
                self.raw()
                    .exists_item(key)
                    .await
                    .map_err(|e| SchemaError::InvalidData(format!("{ctx}: {e}")))?,
            );
        }
        Ok(found)
    }

    /// Existence probe whose *present* answers are confirmed on the live group.
    ///
    /// Cheap tier first, then one live probe over exactly the keys the cheap
    /// tier called present, ANDed back in by [`narrow_present_with_live`]. The
    /// result is therefore never more permissive than either tier alone.
    ///
    /// This is the shape a delete gate needs, at a cost a delete gate can
    /// afford. `rekey_exists_live` alone would spend one group load per key of
    /// every page the walk touches; this spends one only per key the cheap tier
    /// already nominated for deletion. Its callers restrict it further to the
    /// `already` population — slots this page did not write — so a first-pass
    /// migration, where every slot is written, pays nothing at all and
    /// `rekey_page_round_trips_do_not_scale_with_page_size` still holds.
    ///
    /// The remaining `false`s are left alone deliberately: for both callers a
    /// cheap false "absent" retains a key rather than reclaiming it, which is
    /// the conservative direction. See [`ExistsAuthority`].
    pub(super) async fn rekey_exists_confirmed(
        &self,
        keys: &[String],
        ctx: &str,
    ) -> Result<Vec<bool>, SchemaError> {
        let cached = self.rekey_exists(keys, ctx).await?;
        let live_positions: Vec<usize> = cached
            .iter()
            .enumerate()
            .filter(|(_, &present)| present)
            .map(|(pos, _)| pos)
            .collect();
        if live_positions.is_empty() {
            return Ok(cached);
        }
        let live_keys: Vec<String> = live_positions
            .iter()
            .map(|&pos| keys[pos].clone())
            .collect();
        let live = self.rekey_exists_live(&live_keys, ctx).await?;
        Ok(narrow_present_with_live(&cached, &live_positions, &live))
    }

    /// Route an existence probe to the tier `authority` asks for.
    pub(super) async fn exists_with_authority(
        &self,
        keys: &[String],
        ctx: &str,
        authority: ExistsAuthority,
    ) -> Result<Vec<bool>, SchemaError> {
        match authority {
            ExistsAuthority::Cached => self.rekey_exists(keys, ctx).await,
            ExistsAuthority::Live => self.rekey_exists_live(keys, ctx).await,
        }
    }

    /// Merge this call's counters onto the durable baseline and persist.
    ///
    /// Called after every page as well as at the end of a pass, so it must be
    /// idempotent in the number of times it runs: it re-derives the checkpoint
    /// from `baseline + report` rather than incrementing in place.
    pub(super) async fn persist_rekey_checkpoint(
        &self,
        checkpoint_key: &str,
        baseline: &AtomPartitionRekeyCheckpoint,
        report: &AtomPartitionRekeyReport,
        last_tip: &Option<String>,
        completed: bool,
    ) -> Result<AtomPartitionRekeyCheckpoint, SchemaError> {
        let checkpoint = AtomPartitionRekeyCheckpoint {
            version: 1,
            after_tip_key: last_tip.clone().or_else(|| baseline.after_tip_key.clone()),
            tips_scanned: report.tips_scanned,
            tips_walked_total: baseline
                .tips_walked_total
                .saturating_add(report.tips_scanned),
            dual_written: baseline.dual_written.saturating_add(report.dual_written),
            already_prefixed: baseline
                .already_prefixed
                .saturating_add(report.already_prefixed),
            missing_body: baseline.missing_body.saturating_add(report.missing_body),
            flat_removed: baseline.flat_removed.saturating_add(report.flat_removed),
            completed,
        };
        self.raw()
            .put_item(
                checkpoint_key,
                &serde_json::to_value(&checkpoint).map_err(|e| {
                    SchemaError::InvalidData(format!("rekey encode checkpoint: {e}"))
                })?,
            )
            .await
            .map_err(|e| SchemaError::InvalidData(format!("rekey put checkpoint: {e}")))?;
        Ok(checkpoint)
    }

    pub(super) async fn load_atom_partition_rekey_checkpoint(
        &self,
        checkpoint_key: &str,
    ) -> Result<AtomPartitionRekeyCheckpoint, SchemaError> {
        let raw = self
            .raw()
            .get_item::<Value>(checkpoint_key)
            .await
            .map_err(|e| SchemaError::InvalidData(format!("rekey load checkpoint: {e}")))?;
        match raw {
            None => Ok(AtomPartitionRekeyCheckpoint {
                version: 1,
                ..Default::default()
            }),
            Some(v) => {
                // A checkpoint written before `tips_walked_total` existed has
                // no such key, and `#[serde(default)]` cannot tell that absence
                // from a stored `0`. Decide it here, where the raw object is
                // still in hand, so a legitimately-zero fresh checkpoint is
                // left alone and only the legacy shape is seeded.
                //
                // The seed matters beyond one reading: every later pass
                // accumulates onto this baseline (see
                // `persist_rekey_checkpoint`), so a zero inherited here is
                // inherited for the rest of the migration's life. That is how a
                // resumed job reported ~1% walked with ~10% done. `tips_scanned`
                // is the right source: under the old un-paginated walk it
                // tracked the cursor's position, which is what the numerator
                // means.
                let had_walked_total = v.get("tips_walked_total").is_some();
                let mut checkpoint: AtomPartitionRekeyCheckpoint = serde_json::from_value(v)
                    .map_err(|e| {
                        SchemaError::InvalidData(format!("rekey corrupt checkpoint: {e}"))
                    })?;
                if !had_walked_total {
                    checkpoint.tips_walked_total = checkpoint.tips_scanned;
                }
                Ok(checkpoint)
            }
        }
    }
}
