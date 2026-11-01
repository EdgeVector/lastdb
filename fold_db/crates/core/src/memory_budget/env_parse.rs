//! Env value parsing for budget knobs.

use super::*;

/// Byte knob → value, falling back to `default` on unset/invalid/zero. Zero is
/// treated as unset.
#[must_use]
pub fn parse_bytes(raw: Option<String>, default: u64) -> u64 {
    raw.and_then(|s| s.trim().parse::<u64>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(default)
}

/// `LASTDBD_RSS_LIMIT_MB` → bytes. Invalid or zero falls back to the guard's
/// own default so the projection is never compared against a limit of nothing.
#[must_use]
pub fn parse_rss_limit_bytes(raw: Option<String>) -> u64 {
    parse_bytes(raw, DEFAULT_RSS_LIMIT_MB).saturating_mul(MIB)
}

/// `LASTDB_RSS_BUDGET_MULTIPLIER` → clamped multiplier. Invalid values log and
/// use the measured default.
#[must_use]
pub fn parse_rss_multiplier(raw: Option<String>) -> f64 {
    let Some(raw) = raw else {
        return DEFAULT_RSS_MULTIPLIER;
    };
    match raw.trim().parse::<f64>() {
        Ok(v) if v.is_finite() && v > 0.0 => v.clamp(MIN_RSS_MULTIPLIER, MAX_RSS_MULTIPLIER),
        _ => {
            tracing::warn!(
                raw = %raw,
                env = RSS_MULTIPLIER_ENV,
                default = DEFAULT_RSS_MULTIPLIER,
                "invalid LASTDB_RSS_BUDGET_MULTIPLIER; using the measured default"
            );
            DEFAULT_RSS_MULTIPLIER
        }
    }
}

/// `LASTDB_RESIDENT_MAX_DEFERRED_BYTES` → explicit cap. An explicit `0`
/// disables deferral (every batch persists inline) and is honoured as such —
/// unlike the count knob, zero here is a meaningful operator choice.
#[must_use]
pub fn parse_deferred_cap_override(raw: Option<String>) -> Option<u64> {
    let raw = raw?;
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    if let Ok(bytes) = trimmed.parse::<u64>() {
        return Some(bytes);
    }
    tracing::warn!(
        raw = %trimmed,
        env = DEFERRED_BYTES_ENV,
        "invalid LASTDB_RESIDENT_MAX_DEFERRED_BYTES; deriving from headroom"
    );
    None
}
