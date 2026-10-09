//! Env value parsing for budget knobs.

use super::*;

/// Byte knob → value, falling back to `default` on unset/invalid/zero. Zero is
/// treated as unset to match `hash_group_warm_bytes_from_env`, which filters
/// non-positive values back to the preset.
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

/// `LASTDB_DEFER_LANE_FAIR_SHARE_PERCENT` → 1..=100. Invalid or unset uses
/// [`DEFAULT_LANE_FAIR_SHARE_PERCENT`].
#[must_use]
pub fn parse_lane_fair_share_percent(raw: Option<String>) -> u8 {
    let Some(raw) = raw else {
        return DEFAULT_LANE_FAIR_SHARE_PERCENT;
    };
    match raw.trim().parse::<u8>() {
        Ok(v) if (1..=100).contains(&v) => v,
        _ => {
            tracing::warn!(
                raw = %raw,
                env = LANE_FAIR_SHARE_PERCENT_ENV,
                default = DEFAULT_LANE_FAIR_SHARE_PERCENT,
                "invalid LASTDB_DEFER_LANE_FAIR_SHARE_PERCENT; using the default"
            );
            DEFAULT_LANE_FAIR_SHARE_PERCENT
        }
    }
}

/// Process-wide fair-share percent, resolved once.
#[must_use]
pub fn lane_fair_share_percent() -> u8 {
    static PERCENT: OnceLock<u8> = OnceLock::new();
    *PERCENT.get_or_init(|| {
        parse_lane_fair_share_percent(std::env::var(LANE_FAIR_SHARE_PERCENT_ENV).ok())
    })
}

/// Bytes one persist lane may occupy of `cap_bytes`.
#[must_use]
pub fn lane_fair_share_bytes(cap_bytes: u64, percent: u8) -> u64 {
    let percent = percent.clamp(1, 100) as u64;
    cap_bytes.saturating_mul(percent) / 100
}

/// `LASTDB_DEFER_WRITE_THROUGH_BYTES` → threshold. Invalid or unset uses
/// [`DEFAULT_WRITE_THROUGH_BYTES`]. Explicit `0` disables write-through.
#[must_use]
pub fn parse_write_through_bytes(raw: Option<String>) -> u64 {
    let Some(raw) = raw else {
        return DEFAULT_WRITE_THROUGH_BYTES;
    };
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return DEFAULT_WRITE_THROUGH_BYTES;
    }
    if let Ok(bytes) = trimmed.parse::<u64>() {
        bytes
    } else {
        tracing::warn!(
            raw = %trimmed,
            env = WRITE_THROUGH_BYTES_ENV,
            default = DEFAULT_WRITE_THROUGH_BYTES,
            "invalid LASTDB_DEFER_WRITE_THROUGH_BYTES; using the default"
        );
        DEFAULT_WRITE_THROUGH_BYTES
    }
}

/// Process-wide write-through threshold, resolved once.
#[must_use]
pub fn write_through_threshold_bytes() -> u64 {
    static THRESHOLD: OnceLock<u64> = OnceLock::new();
    *THRESHOLD
        .get_or_init(|| parse_write_through_bytes(std::env::var(WRITE_THROUGH_BYTES_ENV).ok()))
}

/// True when `bytes` should skip the deferred window and persist inline.
#[must_use]
pub fn should_write_through(bytes: u64) -> bool {
    let threshold = write_through_threshold_bytes();
    threshold > 0 && bytes >= threshold
}
