/// Seconds the governor state has held, from the transition stamp.
///
/// 0 when the governor has not stamped a transition yet (`since == 0`), so a
/// cold process reads "unknown", not "changed this second". A `since` in the
/// future (a clock step between the stamp and the sample) reads 0 rather
/// than wrapping to a huge age.
#[must_use]
pub(super) fn governor_state_held_secs(since_epoch_secs: u64, now_epoch_secs: u64) -> u64 {
    if since_epoch_secs == 0 {
        return 0;
    }
    now_epoch_secs.saturating_sub(since_epoch_secs)
}

/// Render the held age. 0 is "unknown" (no transition stamped yet), never
/// `0s`: a cold process must not read as a state that just changed.
#[must_use]
pub(super) fn format_held_secs(secs: u64) -> String {
    if secs == 0 {
        return "unknown".to_string();
    }
    format_age_secs(secs)
}

pub(super) fn format_unix_utc(ts: u64) -> String {
    use chrono::{TimeZone, Utc};
    match Utc.timestamp_opt(ts as i64, 0).single() {
        Some(dt) => dt.format("%Y-%m-%dT%H:%MZ").to_string(),
        None => format!("unix:{ts}"),
    }
}

/// Render an age in seconds compactly (`45s`, `12m`, `3h`, `2d5h`).
pub(super) fn format_age_secs(secs: u64) -> String {
    if secs < 60 {
        return format!("{secs}s");
    }
    if secs < 3600 {
        return format!("{}m", secs / 60);
    }
    if secs < 86_400 {
        return format!("{}h", secs / 3600);
    }
    let days = secs / 86_400;
    let hours = (secs % 86_400) / 3600;
    if hours == 0 {
        format!("{days}d")
    } else {
        format!("{days}d{hours}h")
    }
}

pub(super) fn format_bytes(bytes: u64) -> String {
    const KIB: f64 = 1024.0;
    const MIB: f64 = KIB * 1024.0;
    const GIB: f64 = MIB * 1024.0;
    let b = bytes as f64;
    if b >= GIB {
        format!("{:.2} GiB", b / GIB)
    } else if b >= MIB {
        format!("{:.2} MiB", b / MIB)
    } else if b >= KIB {
        format!("{:.2} KiB", b / KIB)
    } else {
        format!("{bytes} B")
    }
}
