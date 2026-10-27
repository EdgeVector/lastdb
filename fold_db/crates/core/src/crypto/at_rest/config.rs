//! Environment-driven policy for the at-rest codec: write switches, size floors and
//! the inflate ceiling.

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
/// (default 256 KiB, absolute max 1 MiB).
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

/// Whether `LASTDB_AT_REST_COMPRESS` is unset or on (`0`, `false`, or `no`
/// turns it off).
///
/// No writer consults this switch: the raw-key seals in this module never
/// deflate, and the KV seam has its own [`kv_at_rest_compression_enabled`].
/// Only the `lastdb_node` status labels read it (`determine_writer_policy`
/// and `AtRestCompressionHealth::enabled`). **Reads are unaffected** —
/// [`open_at_rest`] decodes `ENZ:` regardless.
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
/// codec seals values inside records and fields under a raw key, while the KV
/// seam wraps the whole product store — `atoms`, `tips`, `indexes`,
/// `metadata`, `change_feed`, `cas_blobs`, the order log and the locators.
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

/// Whether `LASTDB_AT_REST_RAW` is set.
///
/// No writer consults this switch: each seal call site picks its own form
/// (`seal_at_rest_utf8`, `seal_at_rest_raw`, or the KV seam's `ENB:` form), and
/// reads accept every form. Only the `codec_policy.writer_policy` status label
/// reads it.
#[must_use]
pub fn at_rest_raw_enabled() -> bool {
    matches!(env_flag::var_parse("LASTDB_AT_REST_RAW"), Some(true))
}
