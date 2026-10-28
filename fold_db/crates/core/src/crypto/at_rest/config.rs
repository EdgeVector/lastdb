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

/// Default ceiling on the plaintext a deflated `ENB:` payload may inflate to.
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
/// (`LASTDB_KV_AT_REST_COMPRESS=1`, which then wrapped `atoms`, `tips`, `indexes`,
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

/// Minimum plaintext size to attempt compression on.
///
/// Override with `LASTDB_AT_REST_COMPRESS_MIN_BYTES`. An unparseable or absent
/// value falls back to [`AT_REST_COMPRESS_MIN_BYTES_DEFAULT`]. The KV seam
/// uses it when `LASTDB_KV_AT_REST_COMPRESS_MIN_BYTES` is unset or does not
/// parse.
#[must_use]
pub fn at_rest_compress_min_bytes() -> usize {
    env_flag::var_or(
        "LASTDB_AT_REST_COMPRESS_MIN_BYTES",
        AT_REST_COMPRESS_MIN_BYTES_DEFAULT,
    )
}

/// Whether the **namespaced KV seam** may compress before sealing
/// (`LASTDB_KV_AT_REST_COMPRESS=1`). It defaults **off**.
///
/// The KV seam is the only seal that compresses, and it wraps only the local
/// CAS blob records (`cas_blobs`). The raw-key `at_rest` seals of values
/// inside records and fields never compress. Reads never consult this
/// switch — `EncryptingKvStore` inflates a deflated `ENB:` value whatever it
/// says. The `lastdb status` label `at_rest_compression.enabled` reports it.
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
/// [`at_rest_compress_min_bytes`], so the general
/// `LASTDB_AT_REST_COMPRESS_MIN_BYTES` still moves the KV seam when an
/// operator sets only that one.
#[must_use]
pub fn kv_at_rest_compress_min_bytes() -> usize {
    env_flag::var_parsed::<usize>("LASTDB_KV_AT_REST_COMPRESS_MIN_BYTES")
        .unwrap_or_else(at_rest_compress_min_bytes)
}
