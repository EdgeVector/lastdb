//! Compress-before-seal decision, bounded inflate and the `ENZ:`/`ENB:` deflate
//! seal entry points.

use super::config::{
    at_rest_compress_min_bytes, at_rest_compression_enabled, at_rest_max_inflated_bytes,
    at_rest_raw_enabled,
};
use super::stats::{current_thread_cpu_ns, AtRestCompressionCounters, COMPRESS_COUNTERS};
use super::{
    encode_binary_ciphertext, seal_at_rest_raw, seal_at_rest_utf8, AT_REST_ENC_DEFLATE_PREFIX,
};
use crate::crypto::envelope::encrypt_envelope;
use crate::crypto::error::{CryptoError, CryptoResult};
use base64::{engine::general_purpose::STANDARD as B64, Engine};
use flate2::read::DeflateDecoder;
use flate2::write::DeflateEncoder;
use flate2::Compression;
use std::io::{Read, Write};
use std::sync::atomic::Ordering;

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
pub(super) fn inflate_bounded(compressed: &[u8], max: u64) -> CryptoResult<Vec<u8>> {
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
