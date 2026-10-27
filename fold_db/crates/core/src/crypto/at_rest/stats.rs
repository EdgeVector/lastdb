//! At-rest compression and process-lifetime effectiveness counters reported
//! by `lastdb status`.

use std::sync::atomic::{AtomicU64, Ordering};

/// Process-lifetime counters for at-rest compression qualification.
///
/// The codec uses the one process-wide instance, [`COMPRESS_COUNTERS`].
pub(super) struct AtRestCompressionCounters {
    pub(super) bytes_in: AtomicU64,
    pub(super) bytes_out: AtomicU64,
    pub(super) sealed_compressed: AtomicU64,
    pub(super) sealed_plain: AtomicU64,
    pub(super) compression_attempts: AtomicU64,
    pub(super) compression_attempt_cpu_ns: AtomicU64,
    pub(super) bytes_saved: AtomicU64,
    pub(super) skipped_below_min_bytes: AtomicU64,
    pub(super) skipped_above_inflate_ceiling: AtomicU64,
    pub(super) skipped_output_not_smaller: AtomicU64,
    pub(super) skipped_compression_disabled: AtomicU64,
}

impl AtRestCompressionCounters {
    pub(super) const fn new() -> Self {
        Self {
            bytes_in: AtomicU64::new(0),
            bytes_out: AtomicU64::new(0),
            sealed_compressed: AtomicU64::new(0),
            sealed_plain: AtomicU64::new(0),
            compression_attempts: AtomicU64::new(0),
            compression_attempt_cpu_ns: AtomicU64::new(0),
            bytes_saved: AtomicU64::new(0),
            skipped_below_min_bytes: AtomicU64::new(0),
            skipped_above_inflate_ceiling: AtomicU64::new(0),
            skipped_output_not_smaller: AtomicU64::new(0),
            skipped_compression_disabled: AtomicU64::new(0),
        }
    }

    fn snapshot(&self) -> AtRestCompressionStats {
        AtRestCompressionStats {
            bytes_in: self.bytes_in.load(Ordering::Relaxed),
            bytes_out: self.bytes_out.load(Ordering::Relaxed),
            sealed_compressed: self.sealed_compressed.load(Ordering::Relaxed),
            sealed_plain: self.sealed_plain.load(Ordering::Relaxed),
            compression_attempts: self.compression_attempts.load(Ordering::Relaxed),
            compression_attempt_cpu_ns: self.compression_attempt_cpu_ns.load(Ordering::Relaxed),
            bytes_saved: self.bytes_saved.load(Ordering::Relaxed),
            skipped_below_min_bytes: self.skipped_below_min_bytes.load(Ordering::Relaxed),
            skipped_above_inflate_ceiling: self
                .skipped_above_inflate_ceiling
                .load(Ordering::Relaxed),
            skipped_output_not_smaller: self.skipped_output_not_smaller.load(Ordering::Relaxed),
            skipped_compression_disabled: self.skipped_compression_disabled.load(Ordering::Relaxed),
        }
    }
}

/// Process-wide source for the `lastdb status` compression metrics.
pub(super) static COMPRESS_COUNTERS: AtRestCompressionCounters = AtRestCompressionCounters::new();

/// Observed at-rest compression effectiveness for this process.
///
/// Reported so an operator can see ratio and percent saved rather than trust
/// an estimate — policy item 5 of
/// `preference-lastdb-storage-compress-atoms-and-blobs`.
///
/// **Scope: the KV seam.** The only site that compresses, the namespaced KV
/// seam (`EncryptingKvStore`) that wraps only `cas_blobs`, counts through
/// `compress_for_seal`; the raw-key seals never deflate. A reading of
/// "0 compressed / N plain" while `LASTDB_KV_AT_REST_COMPRESS` is unset is
/// therefore expected, not a fault: the KV seam's write switch defaults off.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct AtRestCompressionStats {
    /// Plaintext bytes presented for sealing.
    pub bytes_in: u64,
    /// Bytes handed to the sealer after the compress-or-skip decision.
    pub bytes_out: u64,
    /// Values sealed after deflate (`ENB:` with the deflate flag).
    pub sealed_compressed: u64,
    /// Values sealed without deflate (`ENB:` without the flag).
    pub sealed_plain: u64,
    /// Deflate calls made while qualifying values for compression.
    pub compression_attempts: u64,
    /// Current-thread CPU time spent in deflate qualification.
    pub compression_attempt_cpu_ns: u64,
    /// Bytes removed by successful deflate qualification.
    pub bytes_saved: u64,
    /// Values below the configured compression floor.
    pub skipped_below_min_bytes: u64,
    /// Values above the configured inflated-size ceiling.
    pub skipped_above_inflate_ceiling: u64,
    /// Values whose deflated form did not meet the stored-byte savings gate.
    /// The field name stays stable for existing telemetry consumers.
    pub skipped_output_not_smaller: u64,
    /// Values that did not qualify because compression was disabled.
    pub skipped_compression_disabled: u64,
}

impl AtRestCompressionStats {
    /// Percent of presented plaintext bytes removed before sealing.
    ///
    /// Returns 0.0 when nothing has been presented yet, so a cold process
    /// reports "no saving observed" rather than a divide-by-zero.
    #[must_use]
    pub fn percent_saved(&self) -> f64 {
        if self.bytes_in == 0 {
            return 0.0;
        }
        100.0 * (1.0 - (self.bytes_out as f64 / self.bytes_in as f64))
    }
}

/// Read the process-wide at-rest compression counters.
#[must_use]
pub fn at_rest_compression_stats() -> AtRestCompressionStats {
    COMPRESS_COUNTERS.snapshot()
}

/// Return current-thread user and system CPU time in nanoseconds.
///
/// Deflate runs synchronously on its caller's thread. This clock therefore
/// measures compression CPU without charging scheduler delay to qualification.
#[cfg(unix)]
pub(super) fn current_thread_cpu_ns() -> Option<u64> {
    let mut time = std::mem::MaybeUninit::<libc::timespec>::uninit();
    // SAFETY: clock_gettime initializes `time` when it returns zero.
    let rc = unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, time.as_mut_ptr()) };
    if rc != 0 {
        return None;
    }
    // SAFETY: the successful call above initialized `time`.
    let time = unsafe { time.assume_init() };
    let secs = u64::try_from(time.tv_sec).ok()?;
    let nanos = u64::try_from(time.tv_nsec).ok()?;
    secs.checked_mul(1_000_000_000)?.checked_add(nanos)
}

#[cfg(not(unix))]
pub(super) fn current_thread_cpu_ns() -> Option<u64> {
    None
}
