//! Opt-in query cost receipts. No record values or paths enter a receipt.

use super::*;

pub(super) async fn render_query(
    req: &UdsRequest,
    ctx: &AccessContext,
    work: impl std::future::Future<Output = Result<Value, HostError>>,
) -> UdsResponse {
    if !req
        .header("x-lastdb-read-receipt")
        .is_some_and(env_flag::truthy)
    {
        return render(work.await, ctx);
    }
    let started = std::time::Instant::now();
    let (result, receipt) = fold_db::read_receipts::observe(Box::pin(work)).await;
    let mut receipt = receipt_json(receipt, started.elapsed());
    let response = render(result, ctx);
    receipt["returned_body_bytes"] = Value::from(response.body.len());
    response.with_header("x-lastdb-read-receipt", receipt.to_string())
}

fn receipt_json(r: fold_db::read_receipts::DiskReadReceipt, elapsed: Duration) -> Value {
    serde_json::json!({
        "version": 1,
        "scope": "query_storage",
        "requested_keys": r.requested_keys,
        "logical_warm_hits": r.logical_warm_hits,
        "logical_warm_misses": r.logical_warm_misses,
        "loader_calls": r.loader_calls,
        "group_opens": r.group_opens,
        "group_load_nanos": r.group_load_nanos,
        "file_open_attempts": r.file_open_attempts,
        "file_opens": r.file_opens,
        "metadata_operations": r.metadata_operations,
        "directory_reads": r.directory_reads,
        "directory_entries": r.directory_entries,
        "read_operations": r.read_operations,
        "legacy_replay_bytes": r.legacy_replay_bytes,
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
        "socket_queue_wait_nanos": lastdb_uds::worker_pool::current_queue_wait()
            .map_or(0, |elapsed| u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX)),
        "query_storage_elapsed_nanos": u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX),
        "filesystem_pages": "unknown",
        "temporary_bytes_scope": "instrumented_byte_buffers"
    })
}

pub(super) fn batch_entry(response: &UdsResponse, body: Value) -> Value {
    let mut entry = serde_json::json!({ "status": response.status, "response": body });
    if let Some(receipt) = response
        .headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("x-lastdb-read-receipt"))
        .and_then(|(_, value)| serde_json::from_str::<Value>(value).ok())
    {
        entry["read_receipt"] = receipt;
    }
    entry
}
