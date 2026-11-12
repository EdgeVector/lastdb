//! Configured peer field-value byte limit.

use std::sync::atomic::{AtomicUsize, Ordering};

pub use lastdb_model::content_limit::{
    ABSOLUTE_MAX_FIELD_VALUE_BYTES, DEFAULT_MAX_FIELD_VALUE_BYTES, MAX_FIELD_VALUE_BYTES_ENV,
    MIN_MAX_FIELD_VALUE_BYTES,
};

static EFFECTIVE_LIMIT: AtomicUsize = AtomicUsize::new(DEFAULT_MAX_FIELD_VALUE_BYTES);

pub fn set_max_field_value_bytes(limit: usize) {
    let previous = EFFECTIVE_LIMIT.swap(limit, Ordering::AcqRel);
    if previous != limit && limit != DEFAULT_MAX_FIELD_VALUE_BYTES {
        tracing::info!(
            target: "fold_db::content_limit",
            limit,
            default = DEFAULT_MAX_FIELD_VALUE_BYTES,
            absolute_max = ABSOLUTE_MAX_FIELD_VALUE_BYTES,
            env = MAX_FIELD_VALUE_BYTES_ENV,
            "peer field value size limit overridden from env"
        );
    }
}

#[must_use]
pub fn max_field_value_bytes() -> usize {
    EFFECTIVE_LIMIT.load(Ordering::Acquire)
}

#[must_use]
pub fn parse_max_field_value_bytes(raw: Option<String>) -> usize {
    let Some(raw) = raw else {
        return DEFAULT_MAX_FIELD_VALUE_BYTES;
    };
    let s = raw.trim();
    if s.is_empty() {
        return DEFAULT_MAX_FIELD_VALUE_BYTES;
    }
    let Some(parsed) = parse_byte_size(s) else {
        tracing::warn!(
            raw = %s,
            env = MAX_FIELD_VALUE_BYTES_ENV,
            "invalid LASTDB_MAX_FIELD_VALUE_BYTES; using default {}",
            DEFAULT_MAX_FIELD_VALUE_BYTES
        );
        return DEFAULT_MAX_FIELD_VALUE_BYTES;
    };
    if parsed < MIN_MAX_FIELD_VALUE_BYTES {
        tracing::warn!(
            requested = parsed,
            min = MIN_MAX_FIELD_VALUE_BYTES,
            env = MAX_FIELD_VALUE_BYTES_ENV,
            "field value limit below minimum; clamping"
        );
        MIN_MAX_FIELD_VALUE_BYTES
    } else if parsed > ABSOLUTE_MAX_FIELD_VALUE_BYTES {
        tracing::warn!(
            requested = parsed, absolute_max = ABSOLUTE_MAX_FIELD_VALUE_BYTES, env = MAX_FIELD_VALUE_BYTES_ENV,
            "field value limit above absolute max; clamping (use the record's file for larger payloads)"
        );
        ABSOLUTE_MAX_FIELD_VALUE_BYTES
    } else {
        parsed
    }
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
        .find_map(|(u, m)| lower.strip_suffix(u).map(|r| (r, *m)))
        .unwrap_or((lower.as_str(), 1));
    num.trim().parse::<usize>().ok()?.checked_mul(mult)
}
