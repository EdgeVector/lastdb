use super::*;

impl PinLog {
    pub(crate) async fn append_entry_to_active_pin_logs(
        &self,
        entry: &LogEntry,
        targets: &[SyncTarget],
        partitioner: &Option<SyncPartitioner>,
    ) -> Result<Vec<MutationLogTargetPosition>, String> {
        let mutation_log = matches!(self.config.capture_mode, CaptureMode::MutationLog);
        let active_ids: std::collections::HashSet<String> = {
            let state = self.state.lock().await;
            state
                .values()
                .filter(|runtime| runtime.active)
                .map(|runtime| runtime.target_id.clone())
                .collect()
        };
        if active_ids.is_empty() {
            // Pin-mode: nothing frozen → no-op. MutationLog continuous plane
            // must never claim success with zero active runtimes after ensure.
            if mutation_log {
                return Err(
                    "mutation-log capture has no active continuous target runtimes".to_string(),
                );
            }
            return Ok(Vec::new());
        }

        let partitioned = SyncEngine::partition_entry(partitioner, entry, targets)
            .map_err(|e| format!("partition pin log entry: {e}"))?;
        let mut records = Vec::new();
        for (target_idx, sub_entry) in partitioned {
            let Some(target) = targets.get(target_idx) else {
                return Err(format!("pin log target index {target_idx} is out of range"));
            };
            let target_id = target_id_for_prefix(&target.prefix);
            if !active_ids.contains(&target_id) {
                // Pin-mode intentionally freezes a subset of targets and may
                // drop partitions for inactive ones. Continuous MutationLog
                // must not silently hole scoped destinations: ensure already
                // activated every configured target, so an inactive hit is a
                // real defect — fail closed.
                if mutation_log {
                    return Err(format!(
                        "mutation-log capture dropped inactive target_id={target_id} \
                         prefix='{}' label='{}' (configured destination without continuous runtime)",
                        target.prefix, target.label
                    ));
                }
                continue;
            }
            records.push(PinLogRecord {
                model_version: PIN_LOG_MODEL_VERSION,
                target_id,
                target_label: target.label.clone(),
                target_prefix: target.prefix.clone(),
                writer_id: sub_entry.device_id.clone(),
                frontier_after: sub_entry.seq,
                timestamp_ms: sub_entry.timestamp_ms,
                entry: sub_entry,
            });
        }

        if records.is_empty() {
            return Ok(Vec::new());
        }
        let positions = records
            .iter()
            .map(|record| MutationLogTargetPosition {
                target_id: record.target_id.clone(),
                target_label: record.target_label.clone(),
                writer_id: record.writer_id.clone(),
                frontier: record.frontier_after,
            })
            .collect();
        self.persist_pin_log_records(&records).await?;
        Ok(positions)
    }

    /// A cheap preflight under the caller's target-config lock. Existing
    /// marker receipts already own their frontier, so a retry need not mint a
    /// new process-local sequence before the full receipt check below.
    pub(crate) async fn capture_marker_receipt_presence(
        &self,
        marker_keys: &[Vec<u8>],
    ) -> Result<Vec<bool>, String> {
        let store = self.pin_log_store().await?;
        let keys = marker_keys
            .iter()
            .map(|key| capture_marker_receipt_key(key))
            .collect();
        let rows = store
            .get_many(keys)
            .await
            .map_err(|error| format!("read capture marker receipt presence: {error}"))?;
        if rows.len() != marker_keys.len() {
            return Err("capture marker receipt presence returned the wrong row count".into());
        }
        Ok(rows.into_iter().map(|row| row.is_some()).collect())
    }

    pub(crate) async fn capture_marker_append_receipt(
        &self,
        marker_key: &[u8],
    ) -> Result<MutationLogAppendReceipt, String> {
        let store = self.pin_log_store().await?;
        let raw = store
            .get(&capture_marker_receipt_key(marker_key))
            .await
            .map_err(|error| format!("read capture marker publication receipt: {error}"))?
            .ok_or_else(|| "durable capture marker receipt is absent".to_string())?;
        let receipt: CaptureMarkerAppendReceipt = serde_json::from_slice(&raw)
            .map_err(|error| format!("decode capture marker publication receipt: {error}"))?;
        validate_capture_marker_receipt(&receipt, marker_key, &receipt.op_sha256)?;
        let writer_id = receipt.records[0].writer_id.clone();
        let frontier = receipt.records[0].frontier_after;
        let targets = receipt
            .records
            .into_iter()
            .map(|record| MutationLogTargetPosition {
                target_id: record.target_id,
                target_label: record.target_label,
                writer_id: record.writer_id,
                frontier: record.frontier_after,
            })
            .collect();
        Ok(MutationLogAppendReceipt {
            writer_id,
            frontier,
            durable_capture_written: true,
            targets,
        })
    }

    /// Only call this after the marker plane confirms its delete flush. A
    /// failed receipt delete leaves harmless orphan data; an early delete
    /// could allow a retry to append the same mutation again.
    pub(crate) async fn retire_capture_marker_receipts(
        &self,
        marker_keys: &[Vec<u8>],
    ) -> Result<(), String> {
        if marker_keys.is_empty() {
            return Ok(());
        }
        let _append = self.append_lock.lock().await;
        let store = self.pin_log_store().await?;
        let keys = marker_keys
            .iter()
            .map(|key| capture_marker_receipt_key(key))
            .collect();
        store
            .batch_delete(keys)
            .await
            .map_err(|error| format!("delete retired capture marker receipts: {error}"))?;
        store
            .flush()
            .await
            .map_err(|error| format!("flush retired capture marker receipts: {error}"))
    }

    /// Commit a bounded marker page with three pin-log barriers: reserve the
    /// frontier, persist the receipts, then persist every target row. A retry
    /// uses each receipt's original positions, including after a crash before
    /// marker deletion.
    pub(crate) async fn append_capture_marker_batch(
        &self,
        markers: &[(Vec<u8>, LogEntry)],
        targets: &[SyncTarget],
        partitioner: &Option<SyncPartitioner>,
    ) -> Result<Vec<Vec<u8>>, String> {
        // lint:fn-size-ok moved verbatim from pin_log.rs; splitting this function is separate work.
        if markers.is_empty() {
            return Ok(Vec::new());
        }
        if !matches!(self.config.capture_mode, CaptureMode::MutationLog) {
            return Err("capture marker batch requires the continuous mutation log".into());
        }
        self.ensure_continuous_mutation_log_for_targets(targets)
            .await
            .map_err(|error| format!("ensure capture marker targets: {error}"))?;
        let store = self.pin_log_store().await?;
        let mut seen = BTreeSet::new();
        let mut receipt_keys = Vec::with_capacity(markers.len());
        for (marker_key, _) in markers {
            if marker_key.is_empty() || !seen.insert(marker_key.as_slice()) {
                return Err("capture marker batch has an empty or duplicate marker key".into());
            }
            receipt_keys.push(capture_marker_receipt_key(marker_key));
        }
        let prior = store
            .get_many(receipt_keys.clone())
            .await
            .map_err(|error| format!("read capture marker receipts: {error}"))?;
        if prior.len() != markers.len() {
            return Err("capture marker receipt read returned the wrong row count".into());
        }
        let active_ids: std::collections::HashSet<String> = self
            .state
            .lock()
            .await
            .values()
            .filter(|runtime| runtime.active)
            .map(|runtime| runtime.target_id.clone())
            .collect();
        let mut records = Vec::new();
        let mut receipt_puts = Vec::new();
        let mut existing = Vec::new();
        let mut row_bytes = 0usize;
        let mut receipt_bytes = 0usize;
        for (((marker_key, entry), receipt_key), raw) in markers.iter().zip(receipt_keys).zip(prior)
        {
            let op_sha256 = capture_marker_op_sha256(&entry.op)?;
            if let Some(raw) = raw {
                let receipt: CaptureMarkerAppendReceipt = serde_json::from_slice(&raw)
                    .map_err(|error| format!("decode capture marker receipt: {error}"))?;
                validate_capture_marker_receipt(&receipt, marker_key, &op_sha256)?;
                let expected = SyncEngine::partition_entry(partitioner, entry, targets)
                    .map_err(|error| format!("recheck capture marker destination: {error}"))?;
                let [(target_idx, _)] = expected.as_slice() else {
                    return Err("capture marker receipt has no unique current destination".into());
                };
                let target = targets.get(*target_idx).ok_or_else(|| {
                    format!("capture marker target index {target_idx} is out of range")
                })?;
                if receipt.records[0].target_id != target_id_for_prefix(&target.prefix)
                    || receipt.records[0].target_prefix != target.prefix
                {
                    return Err("capture marker receipt destination changed".into());
                }
                existing.push(receipt);
                continue;
            }

            let partitioned = SyncEngine::partition_entry(partitioner, entry, targets)
                .map_err(|error| format!("partition capture marker: {error}"))?;
            let mut marker_records = Vec::new();
            for (target_idx, sub_entry) in partitioned {
                let target = targets.get(target_idx).ok_or_else(|| {
                    format!("capture marker target index {target_idx} is out of range")
                })?;
                let target_id = target_id_for_prefix(&target.prefix);
                if !active_ids.contains(&target_id) {
                    return Err(format!(
                        "capture marker target '{}' has no active mutation-log runtime",
                        target.label
                    ));
                }
                let record = PinLogRecord {
                    model_version: PIN_LOG_MODEL_VERSION,
                    target_id,
                    target_label: target.label.clone(),
                    target_prefix: target.prefix.clone(),
                    writer_id: sub_entry.device_id.clone(),
                    frontier_after: sub_entry.seq,
                    timestamp_ms: sub_entry.timestamp_ms,
                    entry: sub_entry,
                };
                row_bytes = row_bytes.saturating_add(
                    pin_log_entry_key(&record.target_id, record.frontier_after).len()
                        + serde_json::to_vec(&record)
                            .map_err(|error| format!("encode capture marker target row: {error}"))?
                            .len(),
                );
                if row_bytes > CAPTURE_MARKER_BATCH_MAX_ROW_BYTES {
                    return Err(format!(
                        "capture marker batch exceeds {CAPTURE_MARKER_BATCH_MAX_ROW_BYTES} serialized target-row bytes"
                    ));
                }
                marker_records.push(record);
            }
            if marker_records.is_empty() {
                return Err("capture marker reached no required mutation-log target".into());
            }
            let receipt = CaptureMarkerAppendReceipt {
                version: CAPTURE_MARKER_RECEIPT_VERSION,
                marker_key: marker_key.clone(),
                op_sha256,
                records: marker_records.clone(),
            };
            let bytes = serde_json::to_vec(&receipt)
                .map_err(|error| format!("encode capture marker receipt: {error}"))?;
            receipt_bytes = receipt_bytes.saturating_add(receipt_key.len() + bytes.len());
            if row_bytes.saturating_add(receipt_bytes) > CAPTURE_MARKER_BATCH_MAX_DURABLE_BYTES {
                return Err(format!(
                    "capture marker batch exceeds {CAPTURE_MARKER_BATCH_MAX_DURABLE_BYTES} durable bytes"
                ));
            }
            receipt_puts.push((receipt_key, bytes));
            records.extend(marker_records);
        }

        // An earlier flush may have left the marker in the re-export plane.
        // A published row can be absent because confirmed rows are truncated.
        // A missing, unpublished row is repaired from its durable receipt.
        for receipt in existing {
            let keys = receipt
                .records
                .iter()
                .map(|record| pin_log_entry_key(&record.target_id, record.frontier_after))
                .collect::<Vec<_>>();
            let rows = store
                .get_many(keys)
                .await
                .map_err(|error| format!("read receipt target rows: {error}"))?;
            if rows.len() != receipt.records.len() {
                return Err("capture receipt target read returned the wrong row count".into());
            }
            for (record, row) in receipt.records.into_iter().zip(rows) {
                if let Some(row) = row {
                    let stored: PinLogRecord = serde_json::from_slice(&row)
                        .map_err(|error| format!("decode receipt target row: {error}"))?;
                    if transaction_group_record_sha256(&stored)?
                        != transaction_group_record_sha256(&record)?
                    {
                        return Err("capture receipt target row was replaced".into());
                    }
                    continue;
                }
                let published = self.read_published_f_strict(&record.target_id).await?;
                if published.get(&record.writer_id).copied().unwrap_or(0) < record.frontier_after {
                    row_bytes = row_bytes.saturating_add(
                        pin_log_entry_key(&record.target_id, record.frontier_after).len()
                            + serde_json::to_vec(&record)
                                .map_err(|error| {
                                    format!("encode repaired capture marker row: {error}")
                                })?
                                .len(),
                    );
                    if row_bytes > CAPTURE_MARKER_BATCH_MAX_ROW_BYTES {
                        return Err(format!(
                            "capture marker batch exceeds {CAPTURE_MARKER_BATCH_MAX_ROW_BYTES} serialized target-row bytes"
                        ));
                    }
                    records.push(record);
                }
            }
        }
        let floor_bytes = if records.is_empty() {
            0
        } else {
            PIN_LOG_APPENDED_F_KEY.len() + 8
        };
        if row_bytes
            .saturating_add(receipt_bytes)
            .saturating_add(floor_bytes)
            > CAPTURE_MARKER_BATCH_MAX_DURABLE_BYTES
        {
            return Err(format!(
                "capture marker batch exceeds {CAPTURE_MARKER_BATCH_MAX_DURABLE_BYTES} durable bytes"
            ));
        }
        self.persist_pin_log_records_with_receipts(&records, receipt_puts)
            .await?;
        Ok(markers.iter().map(|(key, _)| key.clone()).collect())
    }
}
