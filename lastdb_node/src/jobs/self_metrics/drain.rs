use super::*;

/// Publish one status snapshot and append the JSONL vitals line.
///
/// The snapshot is what `GET /api/status`, `lastdb status`, and `lastdb ops`
/// read. The log line is the copy that remains when LastDB is wedged
/// (Tom 2026-07-19). This path does not write LastDB.
pub(super) fn record_sample(host: &Host, cpu: &mut CpuProbe) -> Result<u64, String> {
    let snapshot = build_snapshot(host, cpu);
    host.self_metrics.publish_status(snapshot.clone());
    let log_path = self_metrics_log_path(&host.home);
    append_self_metrics_log_line(&log_path, &snapshot)?;
    Ok(snapshot.sampled_at)
}
