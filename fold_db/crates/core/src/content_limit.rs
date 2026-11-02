//! Configured peer field value limit.
//!
//! The shell reads [`MAX_FIELD_VALUE_BYTES_ENV`] once, parses it, and passes
//! the usize to [`set_max_field_value_bytes`]. This module does not read the
//! environment. The status reports the installed limit. No write path reads
//! it. A local write uses the record cap.
//!
//! ## Configuration
//!
//! | | |
//! |--|--|
//! | Env | `LASTDB_MAX_FIELD_VALUE_BYTES` (read once by the shell) |
//! | Default | [`DEFAULT_MAX_FIELD_VALUE_BYTES`] (256 KiB) |
//! | Absolute max | [`ABSOLUTE_MAX_FIELD_VALUE_BYTES`] (1 MiB) — cannot raise higher via env |
//! | Status | `GET /api/status` → `limits.max_field_value_bytes` |

use std::sync::atomic::{AtomicUsize, Ordering};

/// The fixed numbers and text are model data; this module holds the installed limit.
pub use lastdb_model::content_limit::{
    ABSOLUTE_MAX_FIELD_VALUE_BYTES, DEFAULT_MAX_FIELD_VALUE_BYTES, MAX_FIELD_VALUE_BYTES_ENV,
    MIN_MAX_FIELD_VALUE_BYTES,
};

static EFFECTIVE_LIMIT: AtomicUsize = AtomicUsize::new(DEFAULT_MAX_FIELD_VALUE_BYTES);

/// Install the limit the shell parsed from [`MAX_FIELD_VALUE_BYTES_ENV`].
///
/// Pass the result of [`parse_max_field_value_bytes`]. The same variable name
/// and default apply. A later install of a different value replaces it.
pub fn set_max_field_value_bytes(limit: usize) {
    let previous = EFFECTIVE_LIMIT.swap(limit, Ordering::AcqRel);
    if previous == limit || limit == DEFAULT_MAX_FIELD_VALUE_BYTES {
        return;
    }
    tracing::info!(
        target: "fold_db::content_limit",
        limit,
        default = DEFAULT_MAX_FIELD_VALUE_BYTES,
        absolute_max = ABSOLUTE_MAX_FIELD_VALUE_BYTES,
        env = MAX_FIELD_VALUE_BYTES_ENV,
        "peer field value size limit overridden from env"
    );
}

/// Effective field value size limit for this process (bytes).
///
/// [`DEFAULT_MAX_FIELD_VALUE_BYTES`] until the shell calls
/// [`set_max_field_value_bytes`]. The shell's value is already clamped to
/// \[`MIN_MAX_FIELD_VALUE_BYTES`, `ABSOLUTE_MAX_FIELD_VALUE_BYTES`\].
#[must_use]
pub fn max_field_value_bytes() -> usize {
    EFFECTIVE_LIMIT.load(Ordering::Acquire)
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
