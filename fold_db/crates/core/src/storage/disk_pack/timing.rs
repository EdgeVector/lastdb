//! Numeric, opt-in write diagnostics. No key, schema, body, or path enters a record.

use std::collections::BTreeMap;
use std::time::Instant;

pub(super) struct Timeline {
    started_us: u64,
    started: Instant,
    marked: Instant,
    phases: BTreeMap<&'static str, u64>,
    trace_id: Option<u64>,
}

pub(super) fn start() -> Option<Timeline> {
    laststore::commit_timing::enabled().then(|| {
        let started = Instant::now();
        Timeline {
            started_us: laststore::commit_timing::unix_us(),
            started,
            marked: started,
            phases: BTreeMap::new(),
            trace_id: crate::request_phases::current_native_trace_id(),
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

pub(super) fn request(timing: Option<Timeline>, sequence: u64, background: bool) {
    if let Some(timing) = timing {
        eprintln!(
            "LASTDB_SNAPSHOT_TIMING {}",
            serde_json::json!({
                "kind": "scope", "sequence": sequence, "background": background,
                "trace_id": timing.trace_id,
                "started_us": timing.started_us, "ended_us": laststore::commit_timing::unix_us(),
                "total_us": timing.started.elapsed().as_micros() as u64, "phases_us": timing.phases,
            })
        );
    }
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
