//! Batched per-page migration for the atom partition-prefix rekey.

use super::*;

impl AtomStore {
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
    pub(in crate::db_operations::admin_db) async fn rekey_page(
        &self,
        page: &[RekeySlot],
        options: &AtomPartitionRekeyOptions,
        storage_prefix: Option<&str>,
        report: &mut AtomPartitionRekeyReport,
    ) -> Result<(), SchemaError> {
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
            return self
                .rekey_page_plan(page, options, &writable, &already, report)
                .await;
        }

        let written = self
            .rekey_page_write(page, writable, &already, report)
            .await?;
        self.rekey_page_verify(page, &written).await?;
        self.rekey_page_remove_flat(page, options, &already, &written, report)
            .await
    }

    /// Dry run: count what a real pass would write and delete, changing nothing.
    async fn rekey_page_plan(
        &self,
        page: &[RekeySlot],
        options: &AtomPartitionRekeyOptions,
        writable: &[(usize, Value)],
        already: &[usize],
        report: &mut AtomPartitionRekeyReport,
    ) -> Result<(), SchemaError> {
        // A plan counts remaining work under its own name; `dual_written`
        // stays 0 so no reader can mistake this report for progress.
        report.would_dual_write += writable.len() as u64;
        if let Some(cap) = options.audit_unresolved {
            // Counters count every flat-only tip; only detail rows are
            // capped — same contract as the unresolved audit.
            for (slot, _) in writable {
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
        Ok(())
    }

    /// Phase (3) and the batched write: reconcile locators of already-migrated
    /// slots, write the bodies and locators, and return the slots written.
    async fn rekey_page_write(
        &self,
        page: &[RekeySlot],
        writable: Vec<(usize, Value)>,
        already: &[usize],
        report: &mut AtomPartitionRekeyReport,
    ) -> Result<Vec<usize>, SchemaError> {
        use crate::atom::{atom_locator_codec, AtomPartition};
        use std::collections::BTreeMap;

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

        Ok(written)
    }

    /// Phase (4): prove every body the page wrote landed, before any flat key
    /// can be deleted.
    async fn rekey_page_verify(
        &self,
        page: &[RekeySlot],
        written: &[usize],
    ) -> Result<(), SchemaError> {
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

        Ok(())
    }

    /// Phase (5): delete flat keys whose prefixed copy is confirmed present.
    async fn rekey_page_remove_flat(
        &self,
        page: &[RekeySlot],
        options: &AtomPartitionRekeyOptions,
        already: &[usize],
        written: &[usize],
        report: &mut AtomPartitionRekeyReport,
    ) -> Result<(), SchemaError> {
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
}
