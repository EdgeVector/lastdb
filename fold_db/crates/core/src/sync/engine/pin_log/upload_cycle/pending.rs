use super::*;

impl PinLog {
    /// Bounded durable scan for records above each writer's published HWM.
    pub(super) async fn scan_pending_records_for_upload(
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
    pub(super) async fn materialize_pending_records(
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
    pub(super) async fn record_and_drop_quarantined(
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
}
