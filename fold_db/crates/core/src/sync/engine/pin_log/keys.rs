use super::*;

pub(super) fn target_id_for_prefix(prefix: &str) -> String {
    if prefix.is_empty() {
        return "personal".to_string();
    }
    sha256_hex(prefix)
}

pub(super) fn pin_log_target_prefix(target_id: &str) -> String {
    format!("{PIN_LOG_ENTRY_PREFIX}{target_id}:entry:")
}

pub(super) fn pin_log_entry_key(target_id: &str, frontier: u64) -> Vec<u8> {
    format!("{}{frontier:020}", pin_log_target_prefix(target_id)).into_bytes()
}

pub(super) fn capture_marker_receipt_key(marker_key: &[u8]) -> Vec<u8> {
    format!("{CAPTURE_MARKER_RECEIPT_PREFIX}{}", sha256_hex(marker_key)).into_bytes()
}

pub(super) fn capture_marker_op_sha256(op: &LogOp) -> Result<String, String> {
    let mut value = serde_json::to_value(op)
        .map_err(|error| format!("encode capture marker operation: {error}"))?;
    sort_json_object_keys(&mut value);
    serde_json::to_vec(&value)
        .map(|bytes| sha256_hex(&bytes))
        .map_err(|error| format!("serialize capture marker operation: {error}"))
}

pub(super) fn validate_capture_marker_receipt(
    receipt: &CaptureMarkerAppendReceipt,
    marker_key: &[u8],
    op_sha256: &str,
) -> Result<(), String> {
    if receipt.version != CAPTURE_MARKER_RECEIPT_VERSION
        || receipt.marker_key != marker_key
        || receipt.op_sha256 != op_sha256
        || receipt.records.len() != 1
    {
        return Err("capture marker receipt identity or payload mismatch".into());
    }
    let record = &receipt.records[0];
    if record.model_version != PIN_LOG_MODEL_VERSION
        || record.target_id != target_id_for_prefix(&record.target_prefix)
        || record.writer_id.is_empty()
        || record.writer_id != record.entry.device_id
        || record.frontier_after == 0
        || record.frontier_after != record.entry.seq
        || record.timestamp_ms != record.entry.timestamp_ms
        || capture_marker_op_sha256(&record.entry.op)? != receipt.op_sha256
    {
        return Err("capture marker receipt target row is inconsistent".into());
    }
    Ok(())
}

pub(super) fn pin_log_published_f_key(target_id: &str) -> Vec<u8> {
    format!("{PIN_LOG_PUBLISHED_F_PREFIX}{target_id}").into_bytes()
}

/// Rows read per durable page by the bounded pin-log scan.
///
/// This is the cycle's RAM knob: peak is roughly this many records' worth of
/// raw bytes plus their decoded forms, regardless of how large the plane is.
/// Override with `LASTDB_PIN_LOG_SCAN_PAGE`.
pub(super) fn pin_log_scan_page_size() -> usize {
    env_flag::var_or("LASTDB_PIN_LOG_SCAN_PAGE", 256usize).clamp(1, 4096)
}

/// Durable rows one upload cycle may examine before giving up and retrying next
/// cycle.
///
/// Only binds when the front of the log is a long run of records this cycle
/// judges not pending — published records whose truncation delete failed. Those
/// still have to be read to be skipped, so without a budget a single cycle can
/// walk the entire plane (bounded RAM, unbounded time) and starve the rest of
/// the sync loop. Override with `LASTDB_PIN_LOG_SCAN_ROW_BUDGET`.
pub(super) fn pin_log_scan_row_budget() -> usize {
    env_flag::var_or("LASTDB_PIN_LOG_SCAN_ROW_BUDGET", 50_000usize).clamp(1, 10_000_000)
}

/// Exclusive upper bound for a byte-prefix range scan.
///
/// Increments the last byte that is not `0xFF`, dropping the `0xFF` tail.
///
/// Returns `None` for an empty or all-`0xFF` prefix, which have no finite
/// successor. `None` rather than a sentinel because every fallback here is
/// wrong in a way the caller must not silently inherit: returning the prefix
/// unchanged gives `start >= end`, which the range-scan contract answers with
/// an empty result — a scan that reads nothing while reporting success. Pin-log
/// prefixes always end in `:`, so this is unreachable for the real key format;
/// the caller still handles it rather than assuming.
pub(super) fn prefix_upper_bound(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end = prefix.to_vec();
    while let Some(last) = end.pop() {
        if last != u8::MAX {
            end.push(last + 1);
            return Some(end);
        }
    }
    None
}

/// Smallest key strictly greater than `key`, for keyset pagination.
pub(super) fn key_after(key: &[u8]) -> Vec<u8> {
    let mut next = key.to_vec();
    next.push(0);
    next
}

/// One bounded window of pending records off a target's durable pin log.
#[derive(Debug, Default)]
pub(crate) struct PendingPinLogPage {
    /// Pending records found, in ascending frontier order, capped at `want`.
    pub(crate) records: Vec<PinLogRecord>,
    /// Exact frontiers the caller classified as already published while
    /// scanning. Cloud cycles retry their best-effort local deletes; test-plane
    /// cycles keep them because a local mirror is not cloud confirmation.
    pub(crate) not_pending_frontiers: Vec<u64>,
    /// Durable rows actually read — the cycle's read cost.
    pub(crate) rows_scanned: usize,
    /// `false` when the scan stopped before the end of the plane, so
    /// `records.len()` is a floor on what is pending, not the total.
    pub(crate) scan_complete: bool,
    /// `true` when the stop was the row budget rather than a filled batch.
    pub(crate) row_budget_exhausted: bool,
}
