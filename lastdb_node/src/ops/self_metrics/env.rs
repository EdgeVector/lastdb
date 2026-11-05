use super::*;

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
