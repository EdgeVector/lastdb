use super::*;

/// Render configured retention series and the counters that prove their cost.
pub fn local_retention_lines(snapshot: &StatusSnapshot) -> Vec<String> {
    let health = &snapshot.local_retention;
    let last = health
        .last_sweep_unix_secs
        .map_or_else(|| "never".to_string(), |value| value.to_string());
    let mut lines = vec![format!(
        "Local retention: enabled={} policies={} passes={} rows_reaped={} last_sweep_unix_secs={} failures={} in_flight={} settled={}",
        health.enabled,
        health.policies.len(),
        health.passes,
        health.rows_reaped,
        last,
        health.failures,
        health.in_flight,
        health.settled,
    )];
    lines.push(format!(
        "  last_pass: finished_backlog={} keys_left={} cut={} schemas_failed={} missing_dropped={} schema_failures_total={}",
        health.last_pass.finished_backlog,
        health.last_pass.keys_left,
        health.last_pass.cut,
        health.last_pass.schemas_failed,
        health.last_pass.missing_dropped,
        health.schema_failures,
    ));
    if let Some(next) = health.next_attempt_unix_secs {
        lines.push(format!(
            "  sweep_waits: failed_passes_in_a_row={} next_attempt_unix_secs={next} (the wait doubles with each failed pass, up to {} s; a pass that succeeds ends it)",
            health.failures,
            super::ttl_backoff::TTL_BACKOFF_CAP.as_secs(),
        ));
    }
    if let Some(error) = health.policy_error.as_deref() {
        lines.push(format!("  policy_read_error={error}"));
    }
    let mut names: Vec<&String> = health.policies.keys().collect();
    names.sort();
    for name in names {
        let policy = &health.policies[name];
        let partitions = if policy.hash_partitions.is_empty() {
            "-".to_string()
        } else {
            policy.hash_partitions.join(",")
        };
        lines.push(format!(
            "  {name}: ttl_seconds={} source={} hash_partitions={}",
            policy.ttl_seconds,
            policy.source.as_str(),
            partitions,
        ));
    }
    lines
}
