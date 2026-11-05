use super::*;

/// How long a measured data-dir size is served before it is refreshed in the
/// background. The walk is a display gauge, not a correctness input, so a
/// minute of staleness is cheaper than re-walking the store per request.
pub const DEFAULT_DATA_DIR_SIZE_TTL: Duration = Duration::from_secs(60);
pub const DEFAULT_SAMPLE_INTERVAL: Duration = Duration::from_secs(60);
/// Relative path under node home for the out-of-band vitals log (JSONL).
/// Survives LastDB being wedged.
pub const DEFAULT_SELF_METRICS_LOG_REL: &str = "logs/self-metrics.jsonl";
/// Rotate the JSONL file when it exceeds this many bytes (default 64 MiB).
pub const DEFAULT_SELF_METRICS_LOG_MAX_BYTES: u64 = 64 * 1024 * 1024;
