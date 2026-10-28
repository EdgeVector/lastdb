use super::*;

/// Process-lifetime at-rest compression counters exposed by `lastdb status`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AtRestCompressionHealth {
    pub bytes_in: u64,
    pub bytes_out: u64,
    pub sealed_compressed: u64,
    pub sealed_plain: u64,
    /// Deflate calls made while qualifying values for compression.
    #[serde(default)]
    pub compression_attempts: u64,
    /// Current-thread CPU time spent in deflate qualification.
    #[serde(default)]
    pub compression_attempt_cpu_ns: u64,
    /// Bytes removed by successful deflate qualification.
    #[serde(default)]
    pub bytes_saved: u64,
    /// Values below the configured compression floor.
    #[serde(default)]
    pub skipped_below_min_bytes: u64,
    /// Values above the configured inflated-size ceiling.
    #[serde(default)]
    pub skipped_above_inflate_ceiling: u64,
    /// Values whose deflated form did not reduce stored bytes.
    #[serde(default)]
    pub skipped_output_not_smaller: u64,
    /// Values that did not qualify because compression was disabled.
    #[serde(default)]
    pub skipped_compression_disabled: u64,
    /// Whether the KV seam may compress new `cas_blobs` values
    /// (`LASTDB_KV_AT_REST_COMPRESS`).
    pub enabled: bool,
}

impl AtRestCompressionHealth {
    pub(super) fn current() -> Self {
        let stats = fold_db::crypto::at_rest_compression_stats();
        Self {
            bytes_in: stats.bytes_in,
            bytes_out: stats.bytes_out,
            sealed_compressed: stats.sealed_compressed,
            sealed_plain: stats.sealed_plain,
            compression_attempts: stats.compression_attempts,
            compression_attempt_cpu_ns: stats.compression_attempt_cpu_ns,
            bytes_saved: stats.bytes_saved,
            skipped_below_min_bytes: stats.skipped_below_min_bytes,
            skipped_above_inflate_ceiling: stats.skipped_above_inflate_ceiling,
            skipped_output_not_smaller: stats.skipped_output_not_smaller,
            skipped_compression_disabled: stats.skipped_compression_disabled,
            enabled: fold_db::crypto::kv_at_rest_compression_enabled(),
        }
    }

    pub(super) fn percent_saved(self) -> f64 {
        if self.bytes_in == 0 {
            return 0.0;
        }
        100.0 * (1.0 - self.bytes_out as f64 / self.bytes_in as f64)
    }
}
