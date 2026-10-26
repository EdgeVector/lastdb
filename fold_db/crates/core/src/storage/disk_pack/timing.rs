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

pub(super) fn request(timing: Option<Timeline>, sequence: u64, background: bool) {
    if let Some(timing) = timing {
        let total_us = timing.started.elapsed().as_micros() as u64;
        let covered_us: u64 = timing.phases.values().copied().sum();
        eprintln!(
            "LASTDB_SNAPSHOT_TIMING {}",
            serde_json::json!({
                "kind": "scope", "sequence": sequence, "background": background,
                "started_us": timing.started_us, "ended_us": laststore::commit_timing::unix_us(),
                "total_us": total_us, "phases_us": timing.phases,
                "covered_us": covered_us, "unassigned_us": total_us.saturating_sub(covered_us),
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
                "covered_us": report.covered_us, "unassigned_us": report.unassigned_us,
            })
        );
    }
}

/// A background step owns a separate thread-local capture. Its intervals can
/// overlap foreground commits and must never enter request latency totals.
pub(super) fn merge(
    report: Option<laststore::commit_timing::Report>,
    result: &laststore::Result<Option<laststore::disk_run::home::RunMerge>>,
    home: usize,
) {
    if let Some(report) = report {
        let step = result.as_ref().ok().and_then(Option::as_ref);
        let kind = step.map_or(0, |step| match step.kind {
            laststore::disk_run::home::MergeKind::Tiered { .. } => 1,
            laststore::disk_run::home::MergeKind::Base { .. } => 2,
        });
        eprintln!(
            "LASTDB_SNAPSHOT_TIMING {}",
            serde_json::json!({
                "kind": "merge", "home": home, "merge_kind": kind, "ok": result.is_ok(),
                "work": step.is_some(), "inputs": step.map_or(0, |step| step.inputs),
                "records": step.map_or(0, |step| step.records),
                "run_bytes": step.map_or(0, |step| step.run_bytes),
                "started_us": report.started_us, "ended_us": report.ended_us,
                "total_us": report.total_us, "phases_us": report.phases_us, "counts": report.counts,
                "covered_us": report.covered_us, "unassigned_us": report.unassigned_us,
            })
        );
    }
}
