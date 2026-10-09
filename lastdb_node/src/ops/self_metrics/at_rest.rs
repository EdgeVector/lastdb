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
    pub enabled: bool,
    /// Rows read from an encrypted namespace with no at-rest envelope, and so
    /// served as absent. `decision-2026-09-14-drop-dual-read-unsealed-is-gone`.
    /// A non-zero value on a restored home means its restore did not seal its
    /// mutation-log replay.
    pub unsealed_discarded: u64,
    /// Un-enveloped rows `lastdb db reap-unsealed --execute` deleted since
    /// process start. Sits next to `unsealed_discarded` so one line shows how
    /// many such rows reads have hidden and how many the reaper has removed.
    #[serde(default)]
    pub unsealed_reaped_rows: u64,
    /// Stored value bytes those deletions returned since process start.
    #[serde(default)]
    pub unsealed_reaped_bytes: u64,
}

/// Codec policy: effective writer format and supported read formats.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CodecPolicyHealth {
    /// Effective writer policy: format and compression (e.g. "ENC" or "ENB:deflate").
    pub writer_policy: String,
    /// Supported read formats in order: ENC, ENZ, ENB.
    pub supported_read_formats: Vec<String>,
}

impl CodecPolicyHealth {
    pub fn current(home: &Path) -> Self {
        let writer_policy = determine_writer_policy();
        let marker_path = home.join(crate::host::CODEC_FORMAT_REQUIREMENT_FILE);
        let supported_read_formats = if marker_path.exists() {
            vec!["ENC".to_string(), "ENZ".to_string(), "ENB".to_string()]
        } else {
            vec!["ENC".to_string()]
        };
        Self {
            writer_policy,
            supported_read_formats,
        }
    }
}

/// Determine the effective writer policy based on environment flags.
pub(super) fn determine_writer_policy() -> String {
    let base_format = if fold_db::crypto::at_rest_raw_enabled() {
        "ENB"
    } else {
        "ENC"
    };

    if fold_db::crypto::at_rest_compression_enabled() {
        format!("{base_format}:deflate")
    } else {
        base_format.to_string()
    }
}

impl AtRestCompressionHealth {
    pub(super) fn current() -> Self {
        let stats = fold_db::crypto::at_rest_compression_stats();
        let (reaped_rows, reaped_bytes) = fold_db::crypto::unsealed_reaped_totals();
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
            enabled: fold_db::crypto::at_rest_compression_enabled(),
            unsealed_discarded: fold_db::crypto::unsealed_discarded_count(),
            unsealed_reaped_rows: reaped_rows,
            unsealed_reaped_bytes: reaped_bytes,
        }
    }

    pub(super) fn percent_saved(self) -> f64 {
        if self.bytes_in == 0 {
            return 0.0;
        }
        100.0 * (1.0 - self.bytes_out as f64 / self.bytes_in as f64)
    }
}
