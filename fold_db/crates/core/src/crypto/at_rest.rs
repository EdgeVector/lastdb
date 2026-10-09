//! Shared synchronous at-rest value codec for direct-Sled-tree stores that
//! bypass the [`EncryptingNamespacedStore`](crate::storage) seam.
//!
//! Several stores open a Sled tree directly instead of routing through the
//! namespaced encryption seam — the node-config store, the app-consent
//! ledger, the revocation ledger, the job tracker, the trigger-runner's
//! per-view state, and the sync engine's download-cursor bookkeeping (see
//! `docs/security/at-rest-threat-model.md` §5.3). Each must encrypt its
//! values at rest, and before this module each hand-rolled the *same*
//! `ENC:<base64>` framing — exactly the per-store drift the threat model
//! warns about ("N stores × N implementations of the same wrapper; no
//! single audit point").
//!
//! This is that single audit point. It is deliberately **synchronous** —
//! keyed directly on the 32-byte E2E content key rather than the async
//! [`CryptoProvider`](super::CryptoProvider) trait — so the sync read paths
//! of those stores can adopt it without an async cascade, and it reuses the
//! same [`encrypt_envelope`]/[`decrypt_envelope`] AES-256-GCM primitive the
//! rest of the crate uses (no second crypto implementation).
//!
//! **Legacy wire format:** `ENC:` ++ base64(envelope) — identical to the
//! [`EncryptingNamespacedStore`](crate::storage::EncryptingNamespacedStore) seam, so a value sealed here reads
//! exactly like a value sealed there and the stored bytes stay valid UTF-8.
//! Values *without* the prefix are treated as legacy pre-migration plaintext
//! and returned verbatim (**dual-read**), enabling lazy rewrite-on-write
//! migration with no separate pass — a node upgraded across the seal keeps
//! reading its old rows, and the next write re-seals them.
//!
//! **Second legacy wire format — `ENZ:` ++
//! base64(envelope(deflate(plaintext))).**
//! Ciphertext is incompressible, so a payload that is sealed and never
//! compressed can never be shrunk again by any downstream plane: compaction,
//! the backup chunk store, and the cloud bill all carry the full size
//! forever. [`seal_at_rest_deflate`] therefore compresses *before* sealing —
//! the order Tom locked in `preference-lastdb-storage-compress-atoms-and-blobs`
//! (plaintext -> compress -> encrypt, never encrypt first).
//!
//! **Binary wire format — `ENB:` ++ flags ++ envelope.** The low bit of the
//! one-byte flags field records whether the plaintext was deflated before it
//! was sealed. This format removes the base64 storage tax while one marker
//! still covers both compressed and uncompressed ciphertext.
//!
//! All prefixes are 4 bytes and end in `:`, which is outside the base64
//! alphabet, so none can be confused with another, with a base64 body, or
//! with JSON (`{`/`[`). [`is_sealed_at_rest`] accepts all three — a caller that
//! gates on "is this sealed?" (Operation Trinity strict open, idempotent
//! reseal) must never see `ENZ:` as unsealed.
//!
//! **Reads never depend on the write-side switch.** [`open_at_rest`] always
//! understands `ENZ:`, whatever [`at_rest_compression_enabled`] says, so
//! disabling compression stops new compressed writes without stranding a
//! single row already written.
//!
//! **The compression half is shared with the KV seam.** The namespaced
//! `EncryptingKvStore` — which wraps
//! `atoms`, `tips`, `indexes`, `metadata`, `change_feed`, `cas_blobs`, the
//! order log and the locators, i.e. essentially the whole store — cannot reuse
//! [`seal_at_rest_deflate`] because it seals through the async
//! [`CryptoProvider`](super::CryptoProvider) rather than a raw key. It reuses
//! the parts that must not drift instead: the `ENZ:` prefix, the
//! compress-or-skip decision (`compress_for_seal`), the bounded inflate
//! (`inflate_at_rest`), and these counters. One codec, two seal sites.

use super::envelope::{decrypt_envelope, encrypt_envelope};
use super::error::{CryptoError, CryptoResult};
use base64::{engine::general_purpose::STANDARD as B64, Engine};
use flate2::read::DeflateDecoder;
use flate2::write::DeflateEncoder;
use flate2::Compression;
use std::io::{Read, Write};
use std::sync::atomic::{AtomicU64, Ordering};

/// Prefix marker for at-rest-encrypted values. A sealed value is
/// `ENC:` ++ base64(ciphertext envelope) and is therefore valid UTF-8.
///
/// `:` is not in the base64 alphabet, so a base64-encoded legacy value (and
/// JSON, which starts with `{`/`[`) can never be mistaken for a sealed one.
pub const AT_REST_ENC_PREFIX: &str = "ENC:";

/// Prefix marker for binary at-rest values.
///
/// The stored form is `ENB:` + one flags byte + the raw ciphertext envelope.
/// Bit 0 means the plaintext was deflated before encryption. Other bits are
/// reserved and rejected so a future extension cannot be misread by an older
/// binary.
pub const AT_REST_ENC_BINARY_PREFIX: &str = "ENB:";

const AT_REST_BINARY_FLAG_DEFLATED: u8 = 0x01;
const AT_REST_BINARY_KNOWN_FLAGS: u8 = AT_REST_BINARY_FLAG_DEFLATED;

/// True when `value` carries an at-rest envelope marker (`ENC:`, `ENZ:`, or
/// `ENB:`) — i.e. it was sealed through this codec (or the equivalent KvStore
/// seam). Lets migration sweeps and dual-read paths tell legacy plaintext rows
/// from sealed ones.
///
/// This **must** cover every supported marker. Callers use it to answer "is
/// this already sealed?", and two of them are load-bearing: Operation Trinity
/// strict open rejects anything unsealed, and `reseal_atom_json_if_plain`
/// skips anything already sealed. A predicate that only knew `ENC:` would make
/// every compressed atom look like unsealed plaintext — read failures on a
/// strict primary, and an infinite reseal loop on the migrate path.
#[must_use]
pub fn is_sealed_at_rest(value: &[u8]) -> bool {
    value.starts_with(AT_REST_ENC_PREFIX.as_bytes())
        || value.starts_with(AT_REST_ENC_DEFLATE_PREFIX.as_bytes())
        || value.starts_with(AT_REST_ENC_BINARY_PREFIX.as_bytes())
}

/// Seal `plaintext` under the 32-byte content key.
///
/// `LASTDB_AT_REST_RAW=1` emits the binary `ENB:` form. The default remains
/// the legacy UTF-8 `ENC:` form so the read side can ship before any node
/// writes the new wire format.
pub fn seal_at_rest(key: &[u8; 32], plaintext: &[u8]) -> CryptoResult<Vec<u8>> {
    if at_rest_raw_enabled() {
        return seal_at_rest_raw(key, plaintext);
    }
    seal_at_rest_utf8(key, plaintext)
}

/// Seal `plaintext` as legacy UTF-8 `ENC:` + base64(envelope).
///
/// Use this only at a storage boundary that requires a JSON/string value.
/// Byte-oriented stores should use [`seal_at_rest`] so the binary write switch
/// can remove the base64 tax after the fleet can read `ENB:`.
pub fn seal_at_rest_utf8(key: &[u8; 32], plaintext: &[u8]) -> CryptoResult<Vec<u8>> {
    let ciphertext = encrypt_envelope(key, plaintext)?;
    let encoded = B64.encode(&ciphertext);
    let mut out = Vec::with_capacity(AT_REST_ENC_PREFIX.len() + encoded.len());
    out.extend_from_slice(AT_REST_ENC_PREFIX.as_bytes());
    out.extend_from_slice(encoded.as_bytes());
    Ok(out)
}

/// Seal `plaintext` as `ENB:` + flags + raw ciphertext envelope.
pub fn seal_at_rest_raw(key: &[u8; 32], plaintext: &[u8]) -> CryptoResult<Vec<u8>> {
    let ciphertext = encrypt_envelope(key, plaintext)?;
    Ok(encode_binary_ciphertext(&ciphertext, false))
}

/// Whether byte-oriented direct stores may emit `ENB:`.
///
/// Reads never consult this switch. It defaults off for the two-phase rollout:
/// first ship and install the reader, then set `LASTDB_AT_REST_RAW=1`.
#[must_use]
pub fn at_rest_raw_enabled() -> bool {
    matches!(env_flag::var_parse("LASTDB_AT_REST_RAW"), Some(true))
}

pub(crate) fn encode_binary_ciphertext(ciphertext: &[u8], deflated: bool) -> Vec<u8> {
    let mut out = Vec::with_capacity(AT_REST_ENC_BINARY_PREFIX.len() + 1 + ciphertext.len());
    out.extend_from_slice(AT_REST_ENC_BINARY_PREFIX.as_bytes());
    out.push(u8::from(deflated) * AT_REST_BINARY_FLAG_DEFLATED);
    out.extend_from_slice(ciphertext);
    out
}

pub(crate) fn decode_binary_ciphertext(stored: &[u8]) -> CryptoResult<Option<(bool, &[u8])>> {
    if !stored.starts_with(AT_REST_ENC_BINARY_PREFIX.as_bytes()) {
        return Ok(None);
    }
    let Some((&flags, ciphertext)) = stored[AT_REST_ENC_BINARY_PREFIX.len()..].split_first() else {
        return Err(CryptoError::InvalidFormat(
            "at-rest ENB envelope is missing its flags byte".to_string(),
        ));
    };
    if flags & !AT_REST_BINARY_KNOWN_FLAGS != 0 {
        return Err(CryptoError::InvalidFormat(format!(
            "at-rest ENB envelope has unsupported flags 0x{flags:02x}"
        )));
    }
    Ok(Some((
        flags & AT_REST_BINARY_FLAG_DEFLATED != 0,
        ciphertext,
    )))
}

/// Prefix marker for at-rest values that were **compressed before sealing**.
/// A sealed value is `ENZ:` ++ base64(envelope(deflate(plaintext))).
///
/// Same shape and same guarantees as [`AT_REST_ENC_PREFIX`]: 4 bytes ending in
/// `:`, which is not in the base64 alphabet, so it can never collide with a
/// base64 body or with JSON.
pub const AT_REST_ENC_DEFLATE_PREFIX: &str = "ENZ:";

/// Default minimum plaintext size to attempt compression on, in bytes.
///
/// Measured on a real 1,455-atom sample drawn from this workspace's live
/// brain + kanban records (2026-08-18): 94.7% of atoms are under 512 B and the
/// median atom is 10 B, but the bytes are concentrated in a small tail of large
/// bodies. Compressing at a 256 B floor touches ~7% of atoms and still returns
/// **52.5% of sealed atom bytes**; dropping the floor to 128 B buys 0.6 more
/// points for 50% more compression calls on the write path. 256 is the knee.
pub const AT_REST_COMPRESS_MIN_BYTES_DEFAULT: usize = 256;

/// Default ceiling on the plaintext an `ENZ:`/`ENB:`-deflated payload may
/// inflate to.
///
/// The payload is AES-GCM authenticated, so only a holder of the key could
/// have produced it and this is not an untrusted-input defence. It bounds the
/// blast radius of a *corrupt* frame that still passes the tag (or a future
/// caller sealing something unbounded) so a bad row cannot OOM the node. Real
/// atoms are capped far below this by `LASTDB_MAX_ATOM_CONTENT_BYTES`
/// (default 64 KiB, absolute max 1 MiB).
///
/// **It is also the write-side ceiling** — see [`compress_for_seal`]. Both
/// sides read [`at_rest_max_inflated_bytes`] so a value this binary sealed
/// compressed is always a value this binary can inflate.
pub const AT_REST_MAX_INFLATED_BYTES_DEFAULT: u64 = 64 * 1024 * 1024;

/// The ceiling in force for this process, from
/// `LASTDB_AT_REST_MAX_INFLATED_BYTES`.
///
/// One knob, read by the seal side and the open side, because they are two
/// halves of one invariant: **nothing sealed compressed may be un-inflatable.**
/// Before 2026-09-06 the ceiling bound only the open side. `compress_for_seal`
/// had a lower bound and no upper bound, so the KV seam
/// (`LASTDB_KV_AT_REST_COMPRESS=1`, which wraps `atoms`, `tips`, `indexes`,
/// `metadata`, `change_feed`, `cas_blobs`, the order log and the locators)
/// could seal a value larger than the ceiling and then refuse to read it back
/// forever. Measured on Tom's primary that day: personal-log compaction failed
/// nine times in one day with `at-rest inflated payload exceeds 67108864
/// bytes`, which left the remote personal log at 112,823 entries and the
/// download cursor seven days behind.
///
/// Raising it is the recovery path for a row already written over the old
/// ceiling: the write side stops producing them, and an operator can lift the
/// read side high enough to get the existing ones back out.
#[must_use]
pub fn at_rest_max_inflated_bytes() -> u64 {
    env_flag::var_parsed::<u64>("LASTDB_AT_REST_MAX_INFLATED_BYTES")
        .filter(|v| *v > 0)
        .unwrap_or(AT_REST_MAX_INFLATED_BYTES_DEFAULT)
}

/// Process-lifetime counters for at-rest compression qualification.
///
/// The production codec uses [`COMPRESS_COUNTERS`]. Tests can use a fresh
/// instance to assert an exact decision partition without concurrent tests
/// adding to the process-wide status metrics.
struct AtRestCompressionCounters {
    bytes_in: AtomicU64,
    bytes_out: AtomicU64,
    sealed_compressed: AtomicU64,
    sealed_plain: AtomicU64,
    compression_attempts: AtomicU64,
    compression_attempt_cpu_ns: AtomicU64,
    bytes_saved: AtomicU64,
    skipped_below_min_bytes: AtomicU64,
    skipped_above_inflate_ceiling: AtomicU64,
    skipped_output_not_smaller: AtomicU64,
    skipped_compression_disabled: AtomicU64,
}

impl AtRestCompressionCounters {
    const fn new() -> Self {
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
static COMPRESS_COUNTERS: AtRestCompressionCounters = AtRestCompressionCounters::new();

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
/// **Scope: both seals.** Every site that can emit `ENZ:` counts through
/// `compress_for_seal`, so these totals cover the direct-Sled-tree codec here
/// *and* the namespaced KV seam
/// (`EncryptingKvStore`) that wraps the
/// product store. A reading of "0 compressed / N plain" while
/// `LASTDB_KV_AT_REST_COMPRESS` is unset is therefore expected, not a fault:
/// the KV seam's write switch defaults off.
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
fn current_thread_cpu_ns() -> Option<u64> {
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
fn current_thread_cpu_ns() -> Option<u64> {
    None
}

/// Whether new writes may compress before sealing.
///
/// Kill switch: set `LASTDB_AT_REST_COMPRESS=0` (or `false`/`no`) to stop
/// emitting `ENZ:`. **Reads are unaffected** — [`open_at_rest`] decodes `ENZ:`
/// regardless, so flipping this off never strands a row already written.
#[must_use]
pub fn at_rest_compression_enabled() -> bool {
    !matches!(env_flag::var_parse("LASTDB_AT_REST_COMPRESS"), Some(false))
}

/// Minimum plaintext size to attempt compression on.
///
/// Override with `LASTDB_AT_REST_COMPRESS_MIN_BYTES`. An unparseable or absent
/// value falls back to [`AT_REST_COMPRESS_MIN_BYTES_DEFAULT`].
#[must_use]
pub fn at_rest_compress_min_bytes() -> usize {
    env_flag::var_or(
        "LASTDB_AT_REST_COMPRESS_MIN_BYTES",
        AT_REST_COMPRESS_MIN_BYTES_DEFAULT,
    )
}

/// Whether the **namespaced KV seam** may compress before sealing.
///
/// This is a second, independent switch from [`at_rest_compression_enabled`],
/// and it defaults **off**. The two seals cover different data: the `at_rest`
/// codec wraps a handful of small direct-Sled trees, while the KV seam wraps
/// the whole product store — `atoms`, `tips`, `indexes`, `metadata`,
/// `change_feed`, `cas_blobs`, the order log and the locators.
///
/// It defaults off because turning it on changes the **stored format of the
/// primary's largest planes**, and a node that later runs a build predating
/// `ENZ:` support at this seam could not read what it wrote. Ship the read
/// side, prove it installed, and only then flip this on
/// (`LASTDB_KV_AT_REST_COMPRESS=1`). Reads never consult this switch —
/// `EncryptingKvStore` decodes `ENZ:` whatever it says.
#[must_use]
pub fn kv_at_rest_compression_enabled() -> bool {
    matches!(
        env_flag::var_parse("LASTDB_KV_AT_REST_COMPRESS"),
        Some(true)
    )
}

/// Minimum plaintext size the KV seam attempts compression on.
///
/// Override with `LASTDB_KV_AT_REST_COMPRESS_MIN_BYTES`; falls back to
/// [`at_rest_compress_min_bytes`] so one knob still moves both seals when an
/// operator only sets the general one.
#[must_use]
pub fn kv_at_rest_compress_min_bytes() -> usize {
    env_flag::var_parsed::<usize>("LASTDB_KV_AT_REST_COMPRESS_MIN_BYTES")
        .unwrap_or_else(at_rest_compress_min_bytes)
}

/// Largest deflated body, as a percent of the plaintext, that stays stored.
///
/// A fast-deflate body near 82 percent of the plaintext still shrinks the
/// row, but the matching inflate makes a warm decode 21 to 32 percent slower
/// than the `ENC:` control on the 4 KiB, 16 KiB, and 56 KiB qualification
/// payloads. That misses the 20 percent `ENB_COMPRESS` budget, so those
/// payloads are sealed verbatim.
///
/// The same home stores atom-edge JSON that deflates to about 68 percent.
/// A 60 percent cutoff leaves those edges verbatim, and the json-1k store
/// then misses the 30 percent byte floor. On current main, a 70 percent
/// cutoff leaves the JSON home at 61,974 bytes against an 88,528-byte ENC
/// control: 29.995 percent saved. A 75 percent cutoff gives small metadata
/// values more room while still rejecting the costly 82 percent payloads.
///
/// The release five-sample report records the named clauses against those
/// budgets, including the json-1k decode check:
/// `scripts/feature-proof/lastdb-codec-qualification/release-five-sample-clauses.json`.
/// The 128 KiB cas-large body is a period-251 sequence. Fast raw deflate
/// shrinks it to about 1 percent of the plaintext, so this gate stores it.
/// That report scores the stored body on the ENB 10 percent budget and the
/// ENB_COMPRESS 20 percent budget for cpu, warm p95, and p99.
const AT_REST_DEFLATE_MAX_PLAINTEXT_PERCENT: usize = 75;

/// Raw-deflate `plaintext`, or `None` when the deflated body is not worth storing.
///
/// Raw deflate (no zlib header, no adler32) because AES-GCM already
/// authenticates the payload — a second checksum would cost 6 bytes per atom
/// and prove nothing the tag does not already prove.
///
/// The fast profile keeps the write cheap. The size gate then rejects a result
/// that does not fall to [`AT_REST_DEFLATE_MAX_PLAINTEXT_PERCENT`] of the
/// plaintext, because a milder shrink misses the warm-decode budget.
fn deflate_if_smaller(plaintext: &[u8]) -> Option<Vec<u8>> {
    let mut encoder = DeflateEncoder::new(Vec::new(), Compression::fast());
    encoder.write_all(plaintext).ok()?;
    let compressed = encoder.finish().ok()?;
    let keeps_read_budget = compressed.len().saturating_mul(100)
        <= plaintext
            .len()
            .saturating_mul(AT_REST_DEFLATE_MAX_PLAINTEXT_PERCENT);
    keeps_read_budget.then_some(compressed)
}

/// Decide whether to compress `plaintext` before sealing, and record the
/// process-lifetime counters `lastdb status` reports.
///
/// Returns `Some(deflated)` when compression is enabled, the plaintext sits
/// between `min_bytes` and [`at_rest_max_inflated_bytes`], and the deflated
/// form is at most [`AT_REST_DEFLATE_MAX_PLAINTEXT_PERCENT`] of the plaintext;
/// otherwise `None`, meaning "seal this verbatim".
/// Every seal site that can emit `ENZ:` goes through here, so the counters
/// describe the whole `ENZ:` population rather than one codec's share of it —
/// the single audit point this module exists to be. Callers must count each
/// value exactly once.
///
/// **`max_bytes` is the read side's ceiling, not a second policy.** A value
/// sealed compressed must be inflatable, and [`inflate_bounded`] refuses
/// anything over [`at_rest_max_inflated_bytes`]; a plaintext above the ceiling
/// is therefore sealed verbatim, where no inflate bound applies. Sealing it
/// compressed would write a row this same binary could never read back. Every
/// caller passes [`at_rest_max_inflated_bytes`]; it is an argument, like
/// `min_bytes` and `enabled`, so the decision stays a pure function of its
/// inputs and the tests need no process-global env var.
#[must_use]
pub(crate) fn compress_for_seal(
    plaintext: &[u8],
    min_bytes: usize,
    max_bytes: u64,
    enabled: bool,
) -> Option<Vec<u8>> {
    compress_for_seal_with_counters(plaintext, min_bytes, max_bytes, enabled, &COMPRESS_COUNTERS)
}

/// Decide whether to compress `plaintext` and record the result in `counters`.
///
/// Production calls route through [`compress_for_seal`] and the process-wide
/// counters. The separate state makes an exact counter-partition test immune
/// to concurrent codec tests that correctly update those process-wide values.
#[must_use]
fn compress_for_seal_with_counters(
    plaintext: &[u8],
    min_bytes: usize,
    max_bytes: u64,
    enabled: bool,
    counters: &AtRestCompressionCounters,
) -> Option<Vec<u8>> {
    counters
        .bytes_in
        .fetch_add(plaintext.len() as u64, Ordering::Relaxed);

    let compressed = if !enabled {
        counters
            .skipped_compression_disabled
            .fetch_add(1, Ordering::Relaxed);
        None
    } else if plaintext.len() < min_bytes {
        counters
            .skipped_below_min_bytes
            .fetch_add(1, Ordering::Relaxed);
        None
    } else if plaintext.len() as u64 > max_bytes {
        counters
            .skipped_above_inflate_ceiling
            .fetch_add(1, Ordering::Relaxed);
        None
    } else {
        counters
            .compression_attempts
            .fetch_add(1, Ordering::Relaxed);
        let cpu_started = current_thread_cpu_ns();
        let compressed = deflate_if_smaller(plaintext);
        if let (Some(started), Some(finished)) = (cpu_started, current_thread_cpu_ns()) {
            counters
                .compression_attempt_cpu_ns
                .fetch_add(finished.saturating_sub(started), Ordering::Relaxed);
        }
        if compressed.is_none() {
            counters
                .skipped_output_not_smaller
                .fetch_add(1, Ordering::Relaxed);
        }
        compressed
    };

    let Some(compressed) = compressed else {
        counters
            .bytes_out
            .fetch_add(plaintext.len() as u64, Ordering::Relaxed);
        counters.sealed_plain.fetch_add(1, Ordering::Relaxed);
        return None;
    };

    counters
        .bytes_out
        .fetch_add(compressed.len() as u64, Ordering::Relaxed);
    counters.bytes_saved.fetch_add(
        (plaintext.len() - compressed.len()) as u64,
        Ordering::Relaxed,
    );
    counters.sealed_compressed.fetch_add(1, Ordering::Relaxed);
    Some(compressed)
}

/// Inflate an `ENZ:` payload under the [`at_rest_max_inflated_bytes`] cap.
///
/// Shared with the namespaced KV seam so there is exactly one bounded
/// decompressor in the product, not one per seal site.
pub(crate) fn inflate_at_rest(compressed: &[u8]) -> CryptoResult<Vec<u8>> {
    inflate_bounded(compressed, at_rest_max_inflated_bytes())
}

/// Seal `plaintext`, compressing first when that is smaller.
///
/// Emits `ENZ:` ++ base64(envelope(deflate(plaintext))) when compression helps,
/// and otherwise falls back to exactly [`seal_at_rest`] (`ENC:`). Callers get a
/// value that [`open_at_rest`] and [`is_sealed_at_rest`] already understand, so
/// adopting this is a drop-in swap at any seal site.
///
/// Compression is skipped when the plaintext is under
/// [`at_rest_compress_min_bytes`], when it is over
/// [`at_rest_max_inflated_bytes`], when the kill switch is off, or when the
/// compressed form is not actually smaller — so this never *grows* a value
/// relative to `seal_at_rest`, and never produces a row [`open_at_rest`]
/// would refuse to inflate.
pub fn seal_at_rest_deflate(key: &[u8; 32], plaintext: &[u8]) -> CryptoResult<Vec<u8>> {
    // Read the process-global config exactly once, here at the entry point, and
    // pass it down explicitly. Everything below is a pure function of its
    // arguments, which is what lets the tests exercise the codec without
    // touching env vars — see `seal_at_rest_deflate_with`.
    seal_at_rest_deflate_with_format(
        key,
        plaintext,
        at_rest_compress_min_bytes(),
        at_rest_compression_enabled(),
        at_rest_raw_enabled(),
    )
}

/// Seal `plaintext` through the raw `ENB:` form, with compression when useful.
///
/// Binary-safe nested containers use this entry point. It does not consult the
/// direct-store writer switch because the caller already selected a byte
/// container that can carry the result.
pub fn seal_at_rest_deflate_raw(key: &[u8; 32], plaintext: &[u8]) -> CryptoResult<Vec<u8>> {
    seal_at_rest_deflate_with_format(
        key,
        plaintext,
        at_rest_compress_min_bytes(),
        at_rest_compression_enabled(),
        true,
    )
}

/// Seal with compression when useful, but keep the result UTF-8.
///
/// Atom content and other nested JSON/string fields use this explicit legacy
/// form. Their container cannot carry raw ciphertext bytes.
pub fn seal_at_rest_deflate_utf8(key: &[u8; 32], plaintext: &[u8]) -> CryptoResult<Vec<u8>> {
    seal_at_rest_deflate_with(
        key,
        plaintext,
        at_rest_compress_min_bytes(),
        at_rest_compression_enabled(),
    )
}

/// [`seal_at_rest_deflate`] with the policy passed in rather than read from the
/// environment.
///
/// Exists because `LASTDB_AT_REST_COMPRESS` is process-global while Rust runs
/// tests on parallel threads in one binary: a test that flips the kill switch
/// would otherwise race every test that depends on compression being on. Taking
/// the config as an argument removes that whole class of flake instead of
/// papering it over with a mutex every test has to remember to take.
pub(crate) fn seal_at_rest_deflate_with(
    key: &[u8; 32],
    plaintext: &[u8],
    min_bytes: usize,
    enabled: bool,
) -> CryptoResult<Vec<u8>> {
    seal_at_rest_deflate_with_format(key, plaintext, min_bytes, enabled, false)
}

fn seal_at_rest_deflate_with_format(
    key: &[u8; 32],
    plaintext: &[u8],
    min_bytes: usize,
    enabled: bool,
    raw: bool,
) -> CryptoResult<Vec<u8>> {
    let Some(compressed) =
        compress_for_seal(plaintext, min_bytes, at_rest_max_inflated_bytes(), enabled)
    else {
        return if raw {
            seal_at_rest_raw(key, plaintext)
        } else {
            seal_at_rest_utf8(key, plaintext)
        };
    };

    let ciphertext = encrypt_envelope(key, &compressed)?;
    if raw {
        return Ok(encode_binary_ciphertext(&ciphertext, true));
    }
    let encoded = B64.encode(&ciphertext);
    let mut out = Vec::with_capacity(AT_REST_ENC_DEFLATE_PREFIX.len() + encoded.len());
    out.extend_from_slice(AT_REST_ENC_DEFLATE_PREFIX.as_bytes());
    out.extend_from_slice(encoded.as_bytes());
    Ok(out)
}

/// Inflate a raw-deflate payload under an explicit ceiling.
///
/// The ceiling arrives as an argument, read once by the public entry point,
/// for the same reason the compression config does: this stays a pure function
/// of its inputs, so a test can drive a small ceiling without mutating a
/// process-global env var that every concurrent read would then see.
///
/// The error names the knob that lifts the ceiling, because the only rows that
/// can trip it now are rows sealed by a binary that had no write-side ceiling,
/// and an operator meeting this message needs to know there is a way out.
fn inflate_bounded(compressed: &[u8], max: u64) -> CryptoResult<Vec<u8>> {
    let mut decoder = DeflateDecoder::new(compressed).take(max + 1);
    // Most LastDB values inflate by less than four times their deflated size.
    // Reserve that common case to avoid repeated Vec growth while retaining
    // the bounded decoder for values with a larger expansion ratio.
    let initial_capacity = compressed
        .len()
        .saturating_mul(4)
        .min(usize::try_from(max).unwrap_or(usize::MAX));
    let mut out = Vec::with_capacity(initial_capacity);
    decoder
        .read_to_end(&mut out)
        .map_err(|e| CryptoError::InvalidFormat(format!("at-rest inflate failed: {e}")))?;
    if out.len() as u64 > max {
        return Err(CryptoError::InvalidFormat(format!(
            "at-rest inflated payload exceeds {max} bytes \
             (raise LASTDB_AT_REST_MAX_INFLATED_BYTES to read a row sealed over the ceiling)"
        )));
    }
    Ok(out)
}

/// Open a stored value. Values *without* a sealed prefix are returned
/// verbatim (legacy pre-migration plaintext, dual-read). `ENC:`- and
/// `ENZ:`-prefixed values are base64-decoded and decrypted under `key`, and
/// `ENB:` values read the raw envelope after the flags byte. Compressed forms
/// are additionally inflated.
///
/// `ENZ:` is decoded unconditionally — it is **not** gated on
/// [`at_rest_compression_enabled`]. The kill switch stops new compressed
/// writes; it must never strand rows already on disk.
pub fn open_at_rest(key: &[u8; 32], stored: &[u8]) -> CryptoResult<Vec<u8>> {
    open_at_rest_with_ceiling(key, stored, at_rest_max_inflated_bytes)
}

// Verbatim reads do not inflate, so they do not need an environment lookup
// for the inflate ceiling. Compressed reads still fetch the current ceiling
// for each open and enforce it after authenticated decryption.
fn open_at_rest_with_ceiling<F>(
    key: &[u8; 32],
    stored: &[u8],
    max_bytes: F,
) -> CryptoResult<Vec<u8>>
where
    F: FnOnce() -> u64,
{
    if let Some((deflated, ciphertext)) = decode_binary_ciphertext(stored)? {
        let plaintext = decrypt_envelope(key, ciphertext)?;
        return if deflated {
            inflate_bounded(&plaintext, max_bytes())
        } else {
            Ok(plaintext)
        };
    }
    let deflated = stored.starts_with(AT_REST_ENC_DEFLATE_PREFIX.as_bytes());
    if !deflated && !stored.starts_with(AT_REST_ENC_PREFIX.as_bytes()) {
        // Legacy plaintext written before this tree was sealed. Dual-read
        // keeps it readable; the next write through `seal_at_rest` re-seals it.
        return Ok(stored.to_vec());
    }
    // Both markers are the same width, so one slice serves both.
    let b64_part = &stored[AT_REST_ENC_PREFIX.len()..];
    let ciphertext = B64
        .decode(b64_part)
        .map_err(|e| CryptoError::InvalidFormat(format!("at-rest base64 decode failed: {e}")))?;
    let plaintext = decrypt_envelope(key, &ciphertext)?;
    if deflated {
        return inflate_bounded(&plaintext, max_bytes());
    }
    Ok(plaintext)
}
