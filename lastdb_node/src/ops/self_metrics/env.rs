use super::*;

pub fn retention_max_samples_from_env() -> usize {
    // An explicit row cap still wins, exactly as it does for the rollup plane:
    // an operator who has measured their own plane should not have to work
    // backwards through the derivation.
    if let Some(rows) =
        env_flag::var_parsed::<usize>("LASTDB_SELF_METRICS_MAX_SAMPLES").filter(|n| *n > 0)
    {
        return rows;
    }
    let max_bytes = env_flag::var_parsed::<u64>("LASTDB_SELF_METRICS_MAX_BYTES")
        .filter(|n| *n > 0)
        .unwrap_or(DEFAULT_SELF_METRIC_MAX_BYTES);
    max_rows_for_budget(max_bytes, SELF_METRIC_FIELDS.len())
}

/// Row cap derived from a byte budget and the plane's WORST-CASE per-row cost.
///
/// Worst case, not typical: the cap has to hold when every declared field is
/// populated, so the divisor is the plane's full declared field count. That
/// makes the derivation conservative in the safe direction — sparse rows (the
/// norm, see `insert_if_nonzero`) use less than the budget, never more.
///
/// The property this buys, which a row constant cannot: adding an
/// observability field now *shrinks* the row cap in proportion, instead of
/// silently growing the plane. Retention is the thing that gives, and it
/// gives visibly, at the moment the field is added.
///
/// Schema-agnostic, and named that way on purpose. It was called
/// `request_ops_rollup_max_rows_for_budget`, and that name is the whole reason
/// the sibling plane sat on a raw row cap for a day after the fix landed: the
/// function that would have fixed it read as though it belonged to the plane
/// that already had it.
#[must_use]
pub fn max_rows_for_budget(max_bytes: u64, fields_per_row: usize) -> usize {
    let per_row = ROLLUP_TIP_BYTES.saturating_mul(fields_per_row.max(1) as u64);
    usize::try_from(max_bytes / per_row.max(1))
        .unwrap_or(usize::MAX)
        .max(1)
}

pub fn request_ops_rollup_retention_from_env() -> usize {
    // An explicit row cap still wins: an operator who has measured their own
    // plane should not have to work backwards through the derivation.
    if let Some(rows) =
        env_flag::var_parsed::<usize>("LASTDB_REQUEST_OPS_ROLLUP_MAX_ROWS").filter(|n| *n > 0)
    {
        return rows;
    }
    let max_bytes = env_flag::var_parsed::<u64>("LASTDB_REQUEST_OPS_ROLLUP_MAX_BYTES")
        .filter(|n| *n > 0)
        .unwrap_or(DEFAULT_REQUEST_OPS_ROLLUP_MAX_BYTES);
    max_rows_for_budget(max_bytes, REQUEST_OPS_ROLLUP_FIELDS.len())
}

/// How long a measured data-dir size stays fresh before a background refresh
/// is kicked off. See [`DataDirSizeCache`] for why this is cached.
pub fn data_dir_size_ttl_from_env() -> Duration {
    env_flag::var_parsed::<u64>("LASTDB_DATA_DIR_SIZE_TTL_SECS")
        .filter(|n| *n > 0)
        .map_or(DEFAULT_DATA_DIR_SIZE_TTL, Duration::from_secs)
}

pub fn sample_interval_from_env() -> Duration {
    env_flag::var_parsed::<u64>("LASTDB_SELF_METRICS_SAMPLE_SECS")
        .filter(|n| *n > 0)
        .map_or(DEFAULT_SAMPLE_INTERVAL, Duration::from_secs)
}

/// When true (default **false**), also best-effort write SelfMetricSample into
/// LastDB after the log-file line. Log file is always the primary sink so a
/// wedged node still leaves a trail (Tom 2026-07-19).
pub fn self_metrics_db_write_enabled() -> bool {
    env_flag::var_truthy("LASTDB_SELF_METRICS_TO_DB")
}

/// Absolute path for the self-metrics JSONL file.
/// Override with `LASTDB_SELF_METRICS_LOG` (absolute or relative to home).
pub fn self_metrics_log_path(home: &Path) -> std::path::PathBuf {
    if let Ok(p) = std::env::var("LASTDB_SELF_METRICS_LOG") {
        let path = std::path::PathBuf::from(p);
        if path.is_absolute() {
            return path;
        }
        return home.join(path);
    }
    home.join(DEFAULT_SELF_METRICS_LOG_REL)
}

pub(super) fn self_metrics_log_max_bytes() -> u64 {
    env_flag::var_parsed("LASTDB_SELF_METRICS_LOG_MAX_BYTES")
        .filter(|n| *n > 0)
        .unwrap_or(DEFAULT_SELF_METRICS_LOG_MAX_BYTES)
}
