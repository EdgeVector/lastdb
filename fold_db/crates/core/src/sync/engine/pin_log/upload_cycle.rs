use super::*;

/// Output of the scan and materialize phase of one upload cycle.
struct ScannedPage {
    summary: ScanSummary,
    batch: Vec<PinLogRecord>,
    scan_ms: u128,
    materialize_ms: u128,
}

/// What the scan phase tells the cycle's report.
struct ScanSummary {
    /// `(rows_scanned, scan_complete, row_budget_exhausted)` from the scan.
    scan_stats: (usize, bool, bool),
    records_quarantined: usize,
    last_quarantine_reason: Option<String>,
    retried_truncations: usize,
}

/// Wall time of each phase of one upload cycle, for the cycle log line.
#[derive(Clone, Copy)]
struct CycleTimings {
    scan_ms: u128,
    materialize_ms: u128,
    seal_ms: u128,
    upload_ms: u128,
}

/// The sealed output of one cycle, ready to publish.
struct SealedCycle<'a> {
    units: &'a [Vec<MutationLogSegment>],
    record_batches: &'a [Vec<PinLogRecord>],
    confirmed_frontiers: &'a [u64],
}

impl PinLog {
    /// Seal durable pin-log records above **per-writer** published F into
    /// segments under `log/{writer_id}/{seq}.enc`, put them on `plane`, and
    /// advance that writer's F (Phase B multi-writer).
    ///
    /// Never blocks local R/W (callers must not await this on the write path).
    /// Caps work per cycle via `max_segments` (0 = unlimited for tests).
    /// Does not require a snapshot lock or continuous full-home re-snapshot.
    pub async fn run_mutation_log_segment_upload_cycle(
        &self,
        engine: &SyncEngine,
        target_prefix: &str,
        plane: &mut MutationLogLocalCloud,
        max_segments: usize,
    ) -> Result<MutationLogUploadReport, String> {
        if !self.mutation_log_upload_allowed(engine).await? {
            return Ok(MutationLogUploadReport::default());
        }
        let target = self.sync_target_by_prefix(target_prefix).await?;
        let target_id = target_id_for_prefix(&target.prefix);
        let published_before_max = {
            let state = self.state.lock().await;
            state.get(&target_id).map_or(0, |r| r.published_frontier)
        };
        let ScannedPage {
            summary,
            batch,
            scan_ms,
            materialize_ms,
        } = self
            .scan_and_materialize_page(engine, &target, &target_id, target_prefix, max_segments)
            .await?;
        let page_record_count = batch.len();

        // Group this page by kind of data, then seal each kind into files of
        // at most 1,000 records or 4 MiB of plaintext. Scan order alternates
        // schemas, and a flush on every schema change sealed one record per
        // file. A single oversized record is still emitted alone so it cannot
        // wedge the frontier. `max_segments` keeps a frontier-ordered prefix:
        // a higher frontier is never published while a lower one is dropped.
        let segment_byte_target = self.segment_byte_target();
        let record_batches =
            plan_mutation_log_record_batches(batch, max_segments, segment_byte_target)?;
        let records_selected: usize = record_batches.iter().map(Vec::len).sum();

        let mut report = Self::new_upload_report(
            &target_id,
            published_before_max,
            &summary,
            records_selected,
            page_record_count,
        );
        self.mark_upload_scan_pending(target_prefix, &report).await;

        let seal_started = std::time::Instant::now();
        let (sealed_units, confirmed_frontiers) =
            Self::seal_upload_units(&target, &record_batches, &mut report).await?;
        let seal_ms = seal_started.elapsed().as_millis();

        let upload_started = std::time::Instant::now();
        let advanced = self
            .publish_sealed_units(
                engine,
                &target,
                plane,
                SealedCycle {
                    units: &sealed_units,
                    record_batches: &record_batches,
                    confirmed_frontiers: &confirmed_frontiers,
                },
                &mut report,
            )
            .await?;

        self.refresh_report_from_runtime(&target_id, &advanced, &mut report)
            .await;
        if report.segments_uploaded > 0 {
            Self::log_upload_cycle(
                &target_id,
                &report,
                advanced.len(),
                CycleTimings {
                    scan_ms,
                    materialize_ms,
                    seal_ms,
                    upload_ms: upload_started.elapsed().as_millis(),
                },
            );
        }
        self.clear_upload_scan_pending(target_prefix, &report).await;
        Ok(report)
    }

    /// Scan the durable log, retry confirmed truncations, materialize the
    /// pending records, and drop quarantined rows.
    async fn scan_and_materialize_page(
        &self,
        engine: &SyncEngine,
        target: &SyncTarget,
        target_id: &str,
        target_prefix: &str,
        max_segments: usize,
    ) -> Result<ScannedPage, String> {
        // Only the flushed durable map may classify a row as confirmed and
        // authorize its deletion; volatile process state cannot.
        let durable_per_writer_before = self.read_published_f(target_id).await;
        let scan_started = std::time::Instant::now();
        let page = self
            .scan_pending_records_for_upload(
                target,
                target_id,
                &durable_per_writer_before,
                max_segments,
            )
            .await?;
        // A prior cloud publish can succeed while its best-effort local delete
        // fails. Those rows are no longer pending, so retry their exact deletes
        // while the bounded scan has them in hand. Without this retry they stay
        // at the front of the log forever and every cycle pays to skip them.
        let retry_confirmed = page.not_pending_frontiers.clone();
        // Quarantine drops are deliberately NOT added here. `records_truncated`
        // is documented as "records deleted this cycle because cloud confirmed
        // them", and a quarantined record is the one thing cloud never saw.
        // Its count is `records_quarantined`.
        let retried_truncations = self
            .truncate_confirmed_pin_log_records(target_id, &retry_confirmed)
            .await;
        let scan_ms = scan_started.elapsed().as_millis();
        let materialize_started = std::time::Instant::now();
        let page_rows_scanned = page.rows_scanned;
        let page_scan_complete = page.scan_complete;
        let page_row_budget_exhausted = page.row_budget_exhausted;
        let (batch, quarantined, last_quarantine_reason) =
            Self::materialize_pending_records(engine, target_prefix, page.records).await?;
        let materialize_ms = materialize_started.elapsed().as_millis();
        let records_quarantined = quarantined.len();
        self.record_and_drop_quarantined(
            engine,
            target_id,
            target_prefix,
            &quarantined,
            last_quarantine_reason.as_ref(),
        )
        .await;
        Ok(ScannedPage {
            summary: ScanSummary {
                scan_stats: (
                    page_rows_scanned,
                    page_scan_complete,
                    page_row_budget_exhausted,
                ),
                records_quarantined,
                last_quarantine_reason,
                retried_truncations,
            },
            batch,
            scan_ms,
            materialize_ms,
        })
    }

    /// Gate for the upload cycle: capture mode, cloud plane, and S0 deferral.
    async fn mutation_log_upload_allowed(&self, engine: &SyncEngine) -> Result<bool, String> {
        if !matches!(self.config.capture_mode, CaptureMode::MutationLog) {
            return Ok(false);
        }
        if !engine.cloud_plane_allows_upload().await {
            return Ok(false);
        }
        if self.continuous_sealed_home_backup_demoted()
            && !engine
                .mutation_log_snapshot_base_committed()
                .map_err(|err| err.to_string())?
        {
            tracing::info!(
                target: "fold_db::sync::mutation_log",
                "mutation-log publish deferred until durable snapshot base S0 commits"
            );
            return Ok(false);
        }
        Ok(true)
    }

    fn segment_byte_target(&self) -> usize {
        if self.config.max_upload_bytes_per_cycle == 0 {
            MUTATION_LOG_SEGMENT_TARGET_BYTES
        } else {
            MUTATION_LOG_SEGMENT_TARGET_BYTES.min(self.config.max_upload_bytes_per_cycle)
        }
    }

    fn new_upload_report(
        target_id: &str,
        published_before_max: u64,
        summary: &ScanSummary,
        records_selected: usize,
        page_record_count: usize,
    ) -> MutationLogUploadReport {
        let ScanSummary {
            scan_stats: (rows_scanned, scan_complete, row_budget_exhausted),
            records_quarantined,
            last_quarantine_reason,
            retried_truncations,
        } = summary;
        let (rows_scanned, scan_complete, row_budget_exhausted) =
            (*rows_scanned, *scan_complete, *row_budget_exhausted);
        MutationLogUploadReport {
            target_id: target_id.to_string(),
            writer_id: String::new(),
            records_considered: records_selected,
            segments_uploaded: 0,
            bytes_uploaded: 0,
            published_frontier_before: published_before_max,
            published_frontier_after: published_before_max,
            upload_backlog_after: 0,
            object_keys: Vec::new(),
            records_truncated: *retried_truncations,
            records_quarantined: *records_quarantined,
            last_quarantine_reason: last_quarantine_reason.clone(),
            put_concurrency: 0,
            rows_scanned,
            records_considered_is_lower_bound: !scan_complete
                || records_selected < page_record_count,
            scan_row_budget_exhausted: row_budget_exhausted,
        }
    }

    fn log_upload_cycle(
        target_id: &str,
        report: &MutationLogUploadReport,
        writers: usize,
        timings: CycleTimings,
    ) {
        let CycleTimings {
            scan_ms,
            materialize_ms,
            seal_ms,
            upload_ms,
        } = timings;
        // Vector F geometry is recorded on plane/runtime maps; status still
        // surfaces scalar max. Log writer count so multi-writer cycles are
        // visible without constructing an unused Frontier value.
        tracing::info!(
            target: "fold_db::sync::mutation_log",
            target_id = %target_id,
            writer_id = %report.writer_id,
            writers,
            records = report.records_considered,
            segments = report.segments_uploaded,
            bytes = report.bytes_uploaded,
            published_f = report.published_frontier_after,
            backlog = report.upload_backlog_after,
            put_concurrency = report.put_concurrency,
            scan_ms,
            materialize_ms,
            seal_ms,
            upload_ms,
            "continuous mutation-log segment upload cycle"
        );
    }

    /// Bounded durable scan for records above each writer's published HWM.
    async fn scan_pending_records_for_upload(
        &self,
        target: &SyncTarget,
        target_id: &str,
        durable_per_writer_before: &BTreeMap<String, u64>,
        max_segments: usize,
    ) -> Result<PendingPinLogPage, String> {
        // Phase B: filter by each record's writer HWM, not a single scalar.
        // Phase A single-writer homes still work (one key in the map / 0).
        //
        // Applied DURING the durable scan, not after it. Filtering after a full
        // read is what made the plane's size the cycle's memory cost; the
        // predicate needs only the one record it is handed, so the scan can
        // stop as soon as the batch is full.
        let is_pending = |r: &PinLogRecord| {
            let hwm = durable_per_writer_before
                .get(&r.writer_id)
                .copied()
                .unwrap_or(0);
            r.frontier_after > hwm
        };
        let record_cap = if max_segments == 0 {
            0
        } else {
            max_segments.saturating_mul(MUTATION_LOG_SEGMENT_MAX_RECORDS)
        };
        let page = self
            .read_pending_pin_log_records_paged(
                target,
                &is_pending,
                record_cap,
                self.config.max_upload_bytes_per_cycle,
                pin_log_scan_row_budget(),
            )
            .await?;
        if page.row_budget_exhausted {
            tracing::warn!(
                target: "fold_db::sync::mutation_log",
                target_id = %target_id,
                rows_scanned = page.rows_scanned,
                pending_found = page.records.len(),
                "pin-log scan hit its per-cycle row budget before filling the batch; \
                 the front of the log is a long run of already-published records whose \
                 truncation delete did not land — set LASTDB_PIN_LOG_SCAN_ROW_BUDGET higher \
                 to walk further per cycle"
            );
        }
        Ok(page)
    }

    /// Materialize `MutationIntent` atoms for each pending record, in log
    /// order. Returns the sealable batch plus the quarantined
    /// `(frontier, seal error)` pairs. The error carries the missing atom id
    /// and field name, and is the only surviving description of the hole once
    /// the durable row is gone — so it is tombstoned with the frontier, not
    /// collapsed into a single `last_reason`.
    async fn materialize_pending_records(
        engine: &SyncEngine,
        target_prefix: &str,
        records: Vec<PinLogRecord>,
    ) -> Result<(Vec<PinLogRecord>, Vec<(u64, String)>, Option<String>), String> {
        let mut quarantined: Vec<(u64, String)> = Vec::new();
        let mut last_quarantine_reason = None;
        let mut batch = Vec::with_capacity(records.len());
        // Atom reads per record used to run one at a time. `buffered` keeps
        // log order, so quarantine, batching and F are exactly as before.
        let mut materialized =
            futures::stream::iter(records.into_iter().map(|mut record| async move {
                let result = async {
                    crate::sync::cloud_purge_peer::scope_alias::rebind_entry(
                        engine.store.as_ref(),
                        target_prefix,
                        &mut record.entry,
                    )
                    .await
                    .map_err(|error| error.to_string())?;
                    match &mut record.entry.op {
                        crate::sync::log::LogOp::MutationIntent { mutations } => engine
                            .materialize_mutation_intent(std::mem::take(mutations))
                            .await
                            .map(|ready| *mutations = ready),
                        _ => Ok(()),
                    }?;
                    crate::sync::cloud_purge_peer::scope_alias::wire_entry(
                        engine.store.as_ref(),
                        target_prefix,
                        &mut record.entry,
                    )
                    .await
                    .map_err(|error| error.to_string())
                }
                .await;
                (record, result)
            }))
            .buffered(MUTATION_LOG_MATERIALIZE_CONCURRENCY);
        while let Some((record, result)) = materialized.next().await {
            match result {
                Ok(()) => {}
                Err(err) if crate::sync::mutation_intent::pin_log_record_cannot_seal(&err) => {
                    tracing::warn!(
                        target: "fold_db::sync::mutation_log",
                        target_prefix = %target_prefix,
                        writer_id = %record.writer_id,
                        frontier_after = record.frontier_after,
                        error = %err,
                        "quarantined unsealable MutationIntent (missing atom); later records still upload"
                    );
                    last_quarantine_reason = Some(err.clone());
                    quarantined.push((record.frontier_after, err));
                    continue;
                }
                Err(err) => return Err(err),
            }
            batch.push(record);
        }
        drop(materialized);
        Ok((batch, quarantined, last_quarantine_reason))
    }

    /// Count quarantined records in runtime state, then tombstone and drop
    /// their durable rows.
    async fn record_and_drop_quarantined(
        &self,
        engine: &SyncEngine,
        target_id: &str,
        target_prefix: &str,
        quarantined: &[(u64, String)],
        last_quarantine_reason: Option<&String>,
    ) {
        let records_quarantined = quarantined.len();
        if records_quarantined > 0 {
            let mut state = self.state.lock().await;
            if let Some(runtime) = state.get_mut(target_id) {
                runtime.records_quarantined = runtime
                    .records_quarantined
                    .saturating_add(records_quarantined as u64);
                runtime.last_quarantine_reason = last_quarantine_reason.cloned();
            }
        }
        if !quarantined.is_empty() {
            self.drop_unsealable_pin_log_records(engine, target_id, target_prefix, quarantined)
                .await;
        }
    }

    /// Seal every planned batch under the *target* crypto provider. Returns
    /// the sealed units and the frontiers they cover; records the first
    /// writer id on `report`.
    async fn seal_upload_units(
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
    async fn publish_sealed_units(
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
    async fn refresh_report_from_runtime(
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
    async fn advance_runtime_after_upload(
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

// lint:file-size-ok moved verbatim from pin_log.rs; cohesive unit, split further in a later pass
