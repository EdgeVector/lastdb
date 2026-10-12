use super::*;

/// Recheck a stored capture-marker receipt against the marker and the
/// current destination, and return it for repair.
pub(super) fn check_existing_capture_marker_receipt(
    raw: &[u8],
    marker_key: &[u8],
    entry: &LogEntry,
    op_sha256: &str,
    targets: &[SyncTarget],
    partitioner: &Option<SyncPartitioner>,
) -> Result<CaptureMarkerAppendReceipt, String> {
    let receipt: CaptureMarkerAppendReceipt = serde_json::from_slice(raw)
        .map_err(|error| format!("decode capture marker receipt: {error}"))?;
    validate_capture_marker_receipt(&receipt, marker_key, op_sha256)?;
    let expected = SyncEngine::partition_entry(partitioner, entry, targets)
        .map_err(|error| format!("recheck capture marker destination: {error}"))?;
    let [(target_idx, _)] = expected.as_slice() else {
        return Err("capture marker receipt has no unique current destination".into());
    };
    let target = targets
        .get(*target_idx)
        .ok_or_else(|| format!("capture marker target index {target_idx} is out of range"))?;
    if receipt.records[0].target_id != target_id_for_prefix(&target.prefix)
        || receipt.records[0].target_prefix != target.prefix
    {
        return Err("capture marker receipt destination changed".into());
    }
    Ok(receipt)
}

/// Build the target-row records for one new marker and charge their
/// serialized size to the batch row-byte budget.
pub(super) fn build_capture_marker_records(
    entry: &LogEntry,
    targets: &[SyncTarget],
    partitioner: &Option<SyncPartitioner>,
    active_ids: &std::collections::HashSet<String>,
    row_bytes: &mut usize,
) -> Result<Vec<PinLogRecord>, String> {
    let partitioned = SyncEngine::partition_entry(partitioner, entry, targets)
        .map_err(|error| format!("partition capture marker: {error}"))?;
    let mut marker_records = Vec::new();
    for (target_idx, sub_entry) in partitioned {
        let target = targets
            .get(target_idx)
            .ok_or_else(|| format!("capture marker target index {target_idx} is out of range"))?;
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
        *row_bytes = (*row_bytes).saturating_add(
            pin_log_entry_key(&record.target_id, record.frontier_after).len()
                + serde_json::to_vec(&record)
                    .map_err(|error| format!("encode capture marker target row: {error}"))?
                    .len(),
        );
        if *row_bytes > CAPTURE_MARKER_BATCH_MAX_ROW_BYTES {
            return Err(format!(
                "capture marker batch exceeds {CAPTURE_MARKER_BATCH_MAX_ROW_BYTES} serialized target-row bytes"
            ));
        }
        marker_records.push(record);
    }
    if marker_records.is_empty() {
        return Err("capture marker reached no required mutation-log target".into());
    }
    Ok(marker_records)
}
