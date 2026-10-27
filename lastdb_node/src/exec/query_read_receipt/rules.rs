//! Query receipt and batch response rules. No clocks, tasks, host, or storage access.

use std::time::Duration;

use fold_db::read_receipts::DiskReadReceipt;
use lastdb_uds::uds_http::UdsResponse;
use serde_json::Value;

pub(super) fn receipt_json(
    r: DiskReadReceipt,
    elapsed: Duration,
    socket_queue_wait: Option<Duration>,
) -> Value {
    serde_json::json!({
        "version": 1,
        "scope": "query_storage",
        "requested_keys": r.requested_keys,
        "logical_warm_hits": r.logical_warm_hits,
        "logical_warm_misses": r.logical_warm_misses,
        "file_open_attempts": r.file_open_attempts,
        "file_opens": r.file_opens,
        "metadata_operations": r.metadata_operations,
        "read_operations": r.read_operations,
        "footer_bytes": r.footer_bytes,
        "payload_bytes": r.payload_bytes,
        "format_probe_bytes": r.format_probe_bytes,
        "decoded_bytes": r.decoded_bytes,
        "decrypted_bytes": r.decrypted_bytes,
        "temporary_bytes_peak": r.temporary_bytes_peak,
        "read_nanos": r.read_nanos,
        "decode_nanos": r.decode_nanos,
        "open_nanos": r.open_nanos,
        "metadata_nanos": r.metadata_nanos,
        "decrypt_nanos": r.decrypt_nanos,
        "lock_wait_nanos": r.lock_wait_nanos,
        "blocking_queue_wait_nanos": r.queue_wait_nanos,
        "socket_queue_wait_nanos": socket_queue_wait
            .map_or(0, |elapsed| u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX)),
        "query_storage_elapsed_nanos": u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX),
        "filesystem_pages": "unknown",
        "temporary_bytes_scope": "instrumented_byte_buffers"
    })
}

/// Restore request order, decode each body, and retain its optional read receipt.
/// Move decoded bodies into their entries without a second JSON value copy.
pub(in crate::exec) fn batch_results(mut finished: Vec<(usize, UdsResponse)>) -> Vec<Value> {
    finished.sort_by_key(|(index, _)| *index);
    finished
        .into_iter()
        .map(|(_, response)| {
            let body = serde_json::from_slice::<Value>(&response.body).unwrap_or_else(|_| {
                Value::String(String::from_utf8_lossy(&response.body).into_owned())
            });
            let mut entry = serde_json::json!({ "status": response.status });
            entry["response"] = body;
            if let Some(receipt) = response
                .headers
                .iter()
                .find(|(name, _)| name.eq_ignore_ascii_case("x-lastdb-read-receipt"))
                .and_then(|(_, value)| serde_json::from_str::<Value>(value).ok())
            {
                entry["read_receipt"] = receipt;
            }
            entry
        })
        .collect()
}
