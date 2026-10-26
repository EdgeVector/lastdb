//! Numeric, opt-in write diagnostics. No key, schema, body, or path enters a record.

use std::collections::BTreeMap;
use std::time::Instant;

pub(super) struct Timeline {
    started_us: u64,
    started: Instant,
    marked: Instant,
    phases: BTreeMap<&'static str, u64>,
}

pub(super) fn start() -> Option<Timeline> {
    laststore::commit_timing::enabled().then(|| {
        let started = Instant::now();
        Timeline {
            started_us: laststore::commit_timing::unix_us(),
            started,
            marked: started,
            phases: BTreeMap::new(),
        }
    })
}

pub(super) fn mark(timing: &mut Option<Timeline>, phase: &'static str) {
    if let Some(timing) = timing {
        let now = Instant::now();
        timing.phases.insert(
            phase,
            now.duration_since(timing.marked)
                .as_micros()
                .try_into()
                .unwrap_or(u64::MAX),
        );
        timing.marked = now;
    }
}

/// Observe only awaited preparation and its explicitly attached storage work.
/// Background tasks do not inherit this task-local read receipt.
pub(super) async fn observe_preparation<F: std::future::Future>(
    work: F,
) -> (F::Output, Option<crate::read_receipts::DiskReadReceipt>) {
    if !laststore::commit_timing::enabled() {
        return (work.await, None);
    }
    let (result, reads) = crate::read_receipts::observe(work).await;
    (result, Some(reads))
}

pub(super) fn request(
    timing: Option<Timeline>,
    sequence: u64,
    background: bool,
    preparation_reads: Option<crate::read_receipts::DiskReadReceipt>,
) {
    if let Some(timing) = timing {
        eprintln!(
            "LASTDB_SNAPSHOT_TIMING {}",
            serde_json::json!({
                "kind": "scope", "sequence": sequence, "background": background,
                "started_us": timing.started_us, "ended_us": laststore::commit_timing::unix_us(),
                "total_us": timing.started.elapsed().as_micros() as u64, "phases_us": timing.phases,
                "preparation_reads": preparation_reads.map(read_costs),
            })
        );
    }
}

fn read_costs(reads: crate::read_receipts::DiskReadReceipt) -> serde_json::Value {
    serde_json::json!({
        "requested_keys": reads.requested_keys,
        "warm_hits": reads.logical_warm_hits, "warm_misses": reads.logical_warm_misses,
        "file_opens": reads.file_opens, "metadata_operations": reads.metadata_operations,
        "read_operations": reads.read_operations,
        "footer_bytes": reads.footer_bytes, "payload_bytes": reads.payload_bytes,
        "format_bytes": reads.format_probe_bytes, "decoded_bytes": reads.decoded_bytes,
        "decrypted_bytes": reads.decrypted_bytes, "temporary_bytes_peak": reads.temporary_bytes_peak,
        "open_us": reads.open_nanos / 1000, "metadata_us": reads.metadata_nanos / 1000,
        "read_us": reads.read_nanos / 1000, "decode_us": reads.decode_nanos / 1000,
        "decrypt_us": reads.decrypt_nanos / 1000, "lock_wait_us": reads.lock_wait_nanos / 1000,
        "queue_wait_us": reads.queue_wait_nanos / 1000,
    })
}

pub(super) fn commit(
    report: Option<laststore::commit_timing::Report>,
    result: &super::StorageResult<(u64, Vec<std::sync::Arc<super::transaction::PendingBatch>>)>,
) {
    if let Some(report) = report {
        let (sequence, requests) = result.as_ref().map_or((0, 0), |(s, r)| (*s, r.len()));
        eprintln!(
            "LASTDB_SNAPSHOT_TIMING {}",
            serde_json::json!({
                "kind": "commit", "sequence": sequence,
                "first_sequence": sequence.saturating_sub(requests.saturating_sub(1) as u64),
                "requests": requests, "ok": result.is_ok(),
                "started_us": report.started_us, "ended_us": report.ended_us,
                "total_us": report.total_us, "phases_us": report.phases_us, "counts": report.counts,
            })
        );
    }
}
