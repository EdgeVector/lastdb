//! At-rest compression and process-lifetime effectiveness counters, plus the
//! unsealed-row discard/reap totals reported by `lastdb status`.

use std::sync::atomic::{AtomicU64, Ordering};

/// Process-lifetime counters for at-rest compression qualification.
///
/// The production codec uses [`COMPRESS_COUNTERS`]. Tests can use a fresh
/// instance to assert an exact decision partition without concurrent tests
/// adding to the process-wide status metrics.
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

/// Rows read from an **encrypted** namespace that carried no at-rest envelope.
///
/// `decision-2026-09-14-drop-dual-read-unsealed-is-gone`: a value without
/// `ENC:` / `ENZ:` / `ENB:` in a namespace LastDB encrypts is not user data. It
/// reads as absent rather than being served as cleartext or re-sealed on the
/// read path, which is how a stray plaintext write used to be laundered into
/// the store.
///
/// Plaintext-by-policy namespaces never reach this counter: `open_namespace`
/// returns the inner store unwrapped for them, so no `EncryptingKvStore`
/// exists to discard anything.
static UNSEALED_DISCARDED: AtomicU64 = AtomicU64::new(0);

/// Namespaces already named in a discard warning this process, so the log
/// carries the fact once per namespace instead of once per row.
static UNSEALED_WARNED: std::sync::Mutex<Option<std::collections::BTreeSet<String>>> =
    std::sync::Mutex::new(None);

/// Count one unsealed row discarded in `namespace`, and warn the first time
/// that namespace discards anything.
///
/// Never logs the key or the value. A discarded row is by definition not
/// something this store wrote, so its bytes are untrusted.
pub fn record_unsealed_discard(namespace: &str) {
    UNSEALED_DISCARDED.fetch_add(1, Ordering::Relaxed);
    // A caller that counts the discards of its own reads (see
    // [`count_unsealed_discards`]) sees this one. No other task does.
    let _ = SCOPED_DISCARDS.try_with(|count| count.fetch_add(1, Ordering::Relaxed));
    let first = match UNSEALED_WARNED.lock() {
        Ok(mut guard) => guard
            .get_or_insert_with(std::collections::BTreeSet::new)
            .insert(namespace.to_string()),
        // A poisoned lock must not swallow the read or silence every later
        // warning; log this one and carry on.
        Err(_) => true,
    };
    if first {
        tracing::warn!(
            namespace,
            "at-rest: discarding unsealed row(s) in an encrypted namespace; they read as absent. \
             Count is in `lastdb status`. If this is a restored home, check that the restore \
             sealed its mutation-log replay."
        );
    }
}

tokio::task_local! {
    /// Discards in the reads of one [`count_unsealed_discards`] scope.
    static SCOPED_DISCARDS: std::sync::Arc<AtomicU64>;
}

/// Run `read` and count the rows it discarded for carrying no at-rest envelope.
///
/// A scan that drops such a row returns fewer rows than it read, so a short
/// page does not mean the end of the range. A walk adds the count to the rows
/// it got to know whether the page was full. The count belongs to this read
/// alone: a concurrent read of another task does not change it.
pub async fn count_unsealed_discards<T>(read: impl std::future::Future<Output = T>) -> (T, u64) {
    let count = std::sync::Arc::new(AtomicU64::new(0));
    let out = SCOPED_DISCARDS
        .scope(std::sync::Arc::clone(&count), read)
        .await;
    (out, count.load(Ordering::Relaxed))
}

/// Rows discarded for carrying no at-rest envelope, since process start.
#[must_use]
pub fn unsealed_discarded_count() -> u64 {
    UNSEALED_DISCARDED.load(Ordering::Relaxed)
}

/// Un-enveloped rows the `reap-unsealed` pass deleted, since process start.
static UNSEALED_REAPED_ROWS: AtomicU64 = AtomicU64::new(0);
/// Stored value bytes those deletions returned, since process start.
static UNSEALED_REAPED_BYTES: AtomicU64 = AtomicU64::new(0);

/// Count rows and bytes removed by one `reap-unsealed` invocation.
///
/// Lives next to [`record_unsealed_discard`] on purpose: `lastdb status` shows
/// how many unsealed rows reads have *hidden* and how many the reaper has
/// *removed* on the same line, so a home that keeps discarding after a reap
/// completed is visibly still writing past the seam.
pub fn record_unsealed_reap(rows: u64, bytes: u64) {
    if rows == 0 && bytes == 0 {
        return;
    }
    UNSEALED_REAPED_ROWS.fetch_add(rows, Ordering::Relaxed);
    UNSEALED_REAPED_BYTES.fetch_add(bytes, Ordering::Relaxed);
}

/// `(rows, bytes)` removed by `reap-unsealed` since process start.
#[must_use]
pub fn unsealed_reaped_totals() -> (u64, u64) {
    (
        UNSEALED_REAPED_ROWS.load(Ordering::Relaxed),
        UNSEALED_REAPED_BYTES.load(Ordering::Relaxed),
    )
}

/// Observed at-rest compression effectiveness for this process.
///
/// Reported so an operator can see ratio and percent saved rather than trust
/// an estimate — policy item 5 of
/// `preference-lastdb-storage-compress-atoms-and-blobs`.
///
/// **Scope: the KV seam.** The only site that compresses, the namespaced KV
/// seam (`EncryptingKvStore`) that wraps the product store, counts through
/// `compress_for_seal`; the raw-key seals never deflate. A reading of
/// "0 compressed / N plain" while `LASTDB_KV_AT_REST_COMPRESS` is unset is
/// therefore expected, not a fault: the KV seam's write switch defaults off.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct AtRestCompressionStats {
    /// Plaintext bytes presented for sealing.
    pub bytes_in: u64,
    /// Bytes handed to the sealer after the compress-or-skip decision.
    pub bytes_out: u64,
    /// Values stored as `ENZ:`.
    pub sealed_compressed: u64,
    /// Values stored as `ENC:`.
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
