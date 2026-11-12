//! Configured peer field value limit.
//!
//! The shell reads [`MAX_FIELD_VALUE_BYTES_ENV`] once, parses it, and passes
//! the usize to [`set_max_field_value_bytes`]. This module does not read the
//! environment. The status reports the installed limit. No write path reads
//! it. A local write uses the record cap.

use std::sync::atomic::{AtomicUsize, Ordering};

/// The fixed numbers and text are model data; this module holds the installed limit.
pub use lastdb_model::content_limit::{
    ABSOLUTE_MAX_FIELD_VALUE_BYTES, DEFAULT_MAX_FIELD_VALUE_BYTES, MAX_FIELD_VALUE_BYTES_ENV,
    MIN_MAX_FIELD_VALUE_BYTES,
};

static EFFECTIVE_LIMIT: AtomicUsize = AtomicUsize::new(DEFAULT_MAX_FIELD_VALUE_BYTES);

/// Install the limit the shell parsed from [`MAX_FIELD_VALUE_BYTES_ENV`].
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
#[must_use]
pub fn max_field_value_bytes() -> usize {
    EFFECTIVE_LIMIT.load(Ordering::Acquire)
}

/// Parse env value → clamped limit. `None` / empty / unparseable → default.
#[must_use]
pub fn parse_max_field_value_bytes(raw: Option<String>) -> usize {
    let Some(s) = raw.as_deref().map(str::trim).filter(|s| !s.is_empty()) else {
        return DEFAULT_MAX_FIELD_VALUE_BYTES;
    };
    let Some(parsed) = parse_byte_size(s) else {
        tracing::warn!(
            raw = %s,
            env = MAX_FIELD_VALUE_BYTES_ENV,
            "invalid LASTDB_MAX_FIELD_VALUE_BYTES; using default {}",
            DEFAULT_MAX_FIELD_VALUE_BYTES
        );
        return DEFAULT_MAX_FIELD_VALUE_BYTES;
    };
    clamp_field_value_limit(parsed)
}

fn parse_byte_size(s: &str) -> Option<usize> {
    const UNITS: [(&str, usize); 6] = [
        ("kib", 1024),
        ("mib", 1024 * 1024),
        ("kb", 1024),
        ("mb", 1024 * 1024),
        ("k", 1024),
        ("m", 1024 * 1024),
    ];
    let lower = s.to_ascii_lowercase();
    let (num, mult) = UNITS
        .iter()
        .find_map(|(unit, mult)| lower.strip_suffix(unit).map(|rest| (rest, *mult)))
        .unwrap_or((lower.as_str(), 1));
    num.trim().parse::<usize>().ok()?.checked_mul(mult)
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
            "field value limit above absolute max; clamping (use the record's file for larger payloads)"
        );
        return ABSOLUTE_MAX_FIELD_VALUE_BYTES;
    }
    n
}
