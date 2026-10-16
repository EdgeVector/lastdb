use super::*;

mod directory;
mod pending;
mod publish;

pub(super) use directory::validated_relative_key;
pub use directory::MutationLogDirectoryExport;

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
        // authorize its deletion; volatile process state cannot. A failed
        // read stays an error. It must not become an empty map and a read
        // from the start of the prefix.
        let durable_per_writer_before = self.read_published_f_strict(target_id).await?;
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
        // fails. Rows above the flushed cursor that this map already covers
        // are still in the range, so retry those exact deletes. Rows at or
        // below the cursor are outside this read and are not walked from zero.
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
}
