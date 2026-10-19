//! Atom partition-prefix rekey and its checkpoint.

use super::*;

/// Where one rekey walk reads and checkpoints, and how many tips a page holds.
#[derive(Clone, Copy)]
struct RekeyWalk<'a> {
    storage_prefix: Option<&'a str>,
    checkpoint_key: &'a str,
    tip_page: usize,
}

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
    pub async fn rekey_atoms_to_partition_prefix(
        &self,
        options: AtomPartitionRekeyOptions,
    ) -> Result<AtomPartitionRekeyReport, SchemaError> {
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
            return Ok(Self::rekey_progress_report(&options, &checkpoint, tip_page));
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

        let mut report = Self::new_rekey_report(&options, &checkpoint, tip_page);

        // Baseline of the durable counters this call accumulates onto. Held
        // separately so the checkpoint can be re-derived and persisted after
        // every page without double-counting.
        let baseline = checkpoint.clone();

        let (last_tip, saw_remaining) = self
            .walk_rekey_pages(
                &options,
                RekeyWalk {
                    storage_prefix,
                    checkpoint_key: &checkpoint_key,
                    tip_page,
                },
                &baseline,
                &mut report,
                &mut checkpoint,
            )
            .await?;

        // The walk reached the end of the keyspace iff it was not cut short by
        // max_ops. That is a statement about the *scan*, and it is all a dry run
        // can ever establish.
        report.scan_reached_end = !saw_remaining;
        // The migration is complete only when an executing pass walked to the
        // end. A dry run writes nothing, so it cannot complete anything.
        report.completed = report.scan_reached_end && !options.dry_run;

        if options.dry_run {
            Self::apply_dry_run_checkpoint(&mut checkpoint, &baseline, &report, last_tip);
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

    /// Keyset-walk the `mk:` range page by page from the checkpoint cursor,
    /// rekeying each page and persisting the cursor after it. Returns the last
    /// tip walked and whether `max_ops` cut the walk short.
    async fn walk_rekey_pages(
        &self,
        options: &AtomPartitionRekeyOptions,
        walk: RekeyWalk<'_>,
        baseline: &AtomPartitionRekeyCheckpoint,
        report: &mut AtomPartitionRekeyReport,
        checkpoint: &mut AtomPartitionRekeyCheckpoint,
    ) -> Result<(Option<String>, bool), SchemaError> {
        use crate::schema::types::field::FilterUtils;

        let RekeyWalk {
            storage_prefix,
            checkpoint_key,
            tip_page,
        } = walk;

        let mk_prefix = build_storage_key(storage_prefix, "mk:");
        let mk_end = FilterUtils::create_prefix_end(&mk_prefix)?;

        // Keyset cursor. `after_tip_key` is exclusive, but the range start is
        // inclusive, so start *at* the cursor and drop that one row: the
        // alternative — synthesising a successor key — would have to assume how
        // the backend encodes key bounds, and this does not.
        let resume_after = checkpoint.after_tip_key.clone();
        let mut page_start = resume_after.clone().unwrap_or_else(|| mk_prefix.clone());

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
            let mut decoded =
                Self::decode_rekey_page(rows, head.as_deref(), storage_prefix, &mut page_start);
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
                self.rekey_page(&decoded, options, storage_prefix, report)
                    .await?;
            }

            // A finished page is a durable resume point: persist the cursor so a
            // restart mid-call resumes at the next page rather than redoing this
            // call. The cursor only ever names a tip whose work is done.
            if !options.dry_run {
                *checkpoint = self
                    .persist_rekey_checkpoint(checkpoint_key, baseline, report, &last_tip, false)
                    .await?;
            }

            if saw_remaining {
                break;
            }
        }

        Ok((last_tip, saw_remaining))
    }
}

mod audit;
mod checkpoint;
mod exists;
mod page;
mod walk;
