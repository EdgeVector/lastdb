//! Configured peer field value limit.
//!
//! The status reports this limit. No write path reads it. A local write uses
//! the record cap. See `fold_db/docs/FIELD_VALUE_SIZE_LIMIT.md`.
//!
//! ## Configuration
//!
//! | | |
//! |--|--|
//! | Env | `LASTDB_MAX_FIELD_VALUE_BYTES` |
//! | Default | [`DEFAULT_MAX_FIELD_VALUE_BYTES`] (256 KiB) |
//! | Absolute max | [`ABSOLUTE_MAX_FIELD_VALUE_BYTES`] (1 MiB) — cannot raise higher via env |
//! | Status | `GET /api/status` → `limits.max_field_value_bytes` |

use std::sync::OnceLock;

/// The fixed numbers and text are model data; this module resolves the limit.
pub use lastdb_model::content_limit::{
    ABSOLUTE_MAX_FIELD_VALUE_BYTES, DEFAULT_MAX_FIELD_VALUE_BYTES, FIELD_VALUE_TOO_LARGE_HINT,
    MAX_FIELD_VALUE_BYTES_ENV, MIN_MAX_FIELD_VALUE_BYTES,
};

static EFFECTIVE_LIMIT: OnceLock<usize> = OnceLock::new();

/// Effective field value size limit for this process (bytes).
///
/// Resolved once from [`MAX_FIELD_VALUE_BYTES_ENV`], clamped to
/// \[`MIN_MAX_FIELD_VALUE_BYTES`, `ABSOLUTE_MAX_FIELD_VALUE_BYTES`\],
/// defaulting to [`DEFAULT_MAX_FIELD_VALUE_BYTES`].
#[must_use]
pub fn max_field_value_bytes() -> usize {
    *EFFECTIVE_LIMIT.get_or_init(resolve_effective_limit)
}

fn resolve_effective_limit() -> usize {
    let limit = parse_max_field_value_bytes(std::env::var(MAX_FIELD_VALUE_BYTES_ENV).ok());
    if limit != DEFAULT_MAX_FIELD_VALUE_BYTES {
        tracing::info!(
            target: "fold_db::content_limit",
            limit,
            default = DEFAULT_MAX_FIELD_VALUE_BYTES,
            absolute_max = ABSOLUTE_MAX_FIELD_VALUE_BYTES,
            env = MAX_FIELD_VALUE_BYTES_ENV,
            "peer field value size limit overridden from env"
        );
    }
    limit
}

/// Parse env value → clamped limit. `None` / empty / unparseable → default.
///
/// Accepts plain decimal bytes (`262144`) or a trailing unit:
/// `k`/`kb`/`kib`, `m`/`mb`/`mib` (case-insensitive).
#[must_use]
pub fn parse_max_field_value_bytes(raw: Option<String>) -> usize {
    let Some(raw) = raw else {
        return DEFAULT_MAX_FIELD_VALUE_BYTES;
    };
    let s = raw.trim();
    if s.is_empty() {
        return DEFAULT_MAX_FIELD_VALUE_BYTES;
    }
    let parsed = parse_byte_size(s).unwrap_or_else(|| {
        tracing::warn!(
            raw = %s,
            env = MAX_FIELD_VALUE_BYTES_ENV,
            "invalid LASTDB_MAX_FIELD_VALUE_BYTES; using default {}",
            DEFAULT_MAX_FIELD_VALUE_BYTES
        );
        DEFAULT_MAX_FIELD_VALUE_BYTES
    });
    clamp_field_value_limit(parsed)
}

fn parse_byte_size(s: &str) -> Option<usize> {
    let lower = s.to_ascii_lowercase();
    let (num, mult) = if let Some(rest) = lower.strip_suffix("kib") {
        (rest.trim(), 1024usize)
    } else if let Some(rest) = lower.strip_suffix("mib") {
        (rest.trim(), 1024 * 1024)
    } else if let Some(rest) = lower.strip_suffix("kb") {
        (rest.trim(), 1024)
    } else if let Some(rest) = lower.strip_suffix("mb") {
        (rest.trim(), 1024 * 1024)
    } else if let Some(rest) = lower.strip_suffix('k') {
        (rest.trim(), 1024)
    } else if let Some(rest) = lower.strip_suffix('m') {
        (rest.trim(), 1024 * 1024)
    } else {
        (lower.as_str(), 1)
    };
    let n: usize = num.trim().parse().ok()?;
    n.checked_mul(mult)
}

fn clamp_field_value_limit(n: usize) -> usize {
    if n < MIN_MAX_FIELD_VALUE_BYTES {
        tracing::warn!(
            requested = n,
            min = MIN_MAX_FIELD_VALUE_BYTES,
            env = MAX_FIELD_VALUE_BYTES_ENV,
            "field value limit below minimum; clamping"
        );
        return MIN_MAX_FIELD_VALUE_BYTES;
    }
    if n > ABSOLUTE_MAX_FIELD_VALUE_BYTES {
        tracing::warn!(
            requested = n,
            absolute_max = ABSOLUTE_MAX_FIELD_VALUE_BYTES,
            env = MAX_FIELD_VALUE_BYTES_ENV,
            "field value limit above absolute max; clamping (use file-blob/CAS for larger payloads)"
        );
        return ABSOLUTE_MAX_FIELD_VALUE_BYTES;
    }
    n
}
