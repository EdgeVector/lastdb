use super::*;

impl PinLog {
    /// Seal every planned batch under the *target* crypto provider. Returns
    /// the sealed units and the frontiers they cover; records the first
    /// writer id on `report`.
    pub(super) async fn seal_upload_units(
        target: &SyncTarget,
        record_batches: &[Vec<PinLogRecord>],
        report: &mut MutationLogUploadReport,
    ) -> Result<(Vec<Vec<MutationLogSegment>>, Vec<u64>), String> {
        // Seal first, then publish to cloud, and only then advance F.
        //
        // Ordering is the whole point. This loop used to seal a record, insert
        // it into `plane` (an in-process HashMap) and immediately advance the
        // published frontier — so `segments_uploaded` and F both moved on a
        // local map write while **nothing left the machine**. On the primary
        // that read as 236 "uploads" against 0 `log/` objects in R2, with lag
        // growing ~1 s/s forever because the frontier was advancing over
        // records that were never durable off-box.
        //
        // A durability counter must be sourced from a cloud-side confirmation,
        // never a local write. If the upload fails we advance nothing, keep the
        // records pending, and let the next cycle retry — local R/W is never
        // blocked either way.
        let records_selected: usize = record_batches.iter().map(Vec::len).sum();
        let mut sealed_units: Vec<Vec<MutationLogSegment>> =
            Vec::with_capacity(record_batches.len());
        let mut confirmed_frontiers = Vec::with_capacity(records_selected);
        for records in record_batches {
            // Keep first writer_id for the single-field report; plane + runtime
            // still track every writer (typical multi-device = one writer/process).
            if report.writer_id.is_empty() {
                report.writer_id = records[0].writer_id.clone();
            }
            confirmed_frontiers.extend(records.iter().map(|record| record.frontier_after));
            // Seal with the *target* crypto provider. Personal uses
            // engine.crypto (same as target.crypto for index 0); org/share
            // destinations must not be sealed under the personal key or peers
            // with only the scoped E2E key cannot open the segment.
            sealed_units.push(seal_mutation_log_publish_unit(records, &target.crypto).await?);
        }
        Ok((sealed_units, confirmed_frontiers))
    }

    /// PUT the sealed units to cloud, flush the per-writer HWM, mirror to the
    /// local plane, and truncate confirmed rows. Returns the per-writer
    /// `(through, record timestamp)` confirmed this cycle (empty when there
    /// was nothing to publish).
    pub(super) async fn publish_sealed_units(
        &self,
        engine: &SyncEngine,
        target: &SyncTarget,
        plane: &mut MutationLogLocalCloud,
        sealed: SealedCycle<'_>,
        report: &mut MutationLogUploadReport,
    ) -> Result<HashMap<String, (u64, u64)>, String> {
        let SealedCycle {
            units: sealed_units,
            record_batches,
            confirmed_frontiers,
        } = sealed;
        let target_id = &report.target_id.clone();
        // writer_id → (through, record timestamp) confirmed this cycle.
        let mut advanced: HashMap<String, (u64, u64)> = HashMap::new();
        if sealed_units.is_empty() {
            return Ok(advanced);
        }
        let objects_in_cycle: usize = sealed_units.iter().map(Vec::len).sum();
        let policy_concurrency = engine.active_upload_caps().await.concurrency;
        report.put_concurrency = mutation_log_put_concurrency(
            policy_concurrency,
            objects_in_cycle,
            mutation_log_put_concurrency_env(),
        );
        let bytes_uploaded = upload_publish_units_batched(engine, target, sealed_units).await?;

        for (unit, records) in sealed_units.iter().zip(record_batches) {
            let last = records
                .last()
                .ok_or_else(|| "sealed mutation-log unit lost its source records".to_string())?;
            let wid = if last.writer_id.is_empty() {
                "unknown-writer".to_string()
            } else {
                last.writer_id.clone()
            };
            let published_at_ms = records.last().map_or(0, |record| record.timestamp_ms);
            let e = advanced.entry(wid).or_insert((0, 0));
            if last.frontier_after >= e.0 {
                *e = (last.frontier_after, published_at_ms);
            }
            report.segments_uploaded = report.segments_uploaded.saturating_add(unit.len());
        }
        report.bytes_uploaded = report.bytes_uploaded.saturating_add(bytes_uploaded);

        // The cloud PUT is not yet a crash-safe confirmation. First
        // max-merge and flush the per-writer HWM. If this fails, return an
        // error without advancing any volatile frontier or deleting a row;
        // the next cycle safely uploads the same cloud object again.
        let confirmed = advanced
            .iter()
            .map(|(writer_id, (through, _))| (writer_id.clone(), *through))
            .collect::<BTreeMap<_, _>>();
        self.persist_published_f(target_id, &confirmed).await?;

        // The meter runs after the flush. A confirm failure does not roll
        // back published-F and does not fail this publish.
        // sealed_units is already &[Vec<MutationLogSegment>].
        for unit in sealed_units {
            engine.meter_uploaded_log_segments(target, unit).await;
        }

        // Local mirror is geometry/bookkeeping only. Update it only after
        // the durable Cloud HWM exists, so a failed HWM flush cannot make a
        // later cycle suppress an unconfirmed local row.
        for (unit, records) in sealed_units.iter().zip(record_batches) {
            for sealed in unit {
                plane.put_segment(sealed)?;
                report.object_keys.push(sealed.segment.object_key.clone());
            }
            let last = records
                .last()
                .ok_or_else(|| "sealed mutation-log unit lost its source records".to_string())?;
            let writer_id = if last.writer_id.is_empty() {
                "unknown-writer"
            } else {
                last.writer_id.as_str()
            };
            plane.advance_published_f(writer_id, last.frontier_after);
        }

        // Only the flushed HWM above authorizes deletion. Drop exact rows,
        // never a range.
        report.records_truncated = report.records_truncated.saturating_add(
            self.truncate_confirmed_pin_log_records(target_id, confirmed_frontiers)
                .await,
        );
        Ok(advanced)
    }

    /// Refresh the report's published frontier and backlog from runtime state.
    /// When segments were uploaded, first fold this cycle's confirmed
    /// per-writer frontiers into that state.
    pub(super) async fn refresh_report_from_runtime(
        &self,
        target_id: &str,
        advanced: &HashMap<String, (u64, u64)>,
        report: &mut MutationLogUploadReport,
    ) {
        if report.segments_uploaded > 0 {
            self.advance_runtime_after_upload(target_id, advanced, report)
                .await;
        } else {
            let state = self.state.lock().await;
            if let Some(runtime) = state.get(target_id) {
                report.published_frontier_after = runtime.published_frontier;
                report.upload_backlog_after = runtime
                    .last_durable_frontier
                    .saturating_sub(runtime.published_frontier);
            }
        }
    }

    /// Fold this cycle's confirmed per-writer frontiers into runtime state and
    /// refresh the report's published frontier and backlog.
    pub(super) async fn advance_runtime_after_upload(
        &self,
        target_id: &str,
        advanced: &HashMap<String, (u64, u64)>,
        report: &mut MutationLogUploadReport,
    ) {
        let mut state = self.state.lock().await;
        if let Some(runtime) = state.get_mut(target_id) {
            for (wid, (through, published_at_ms)) in advanced {
                runtime.advance_published_f(wid, *through, *published_at_ms);
            }
            runtime.segments_uploaded = runtime
                .segments_uploaded
                .saturating_add(report.segments_uploaded as u64);
            report.published_frontier_after = runtime.published_frontier;
            report.upload_backlog_after = runtime
                .last_durable_frontier
                .saturating_sub(runtime.published_frontier);
        } else {
            report.published_frontier_after = advanced
                .values()
                .map(|(through, _)| *through)
                .max()
                .unwrap_or(0);
        }
    }
}
