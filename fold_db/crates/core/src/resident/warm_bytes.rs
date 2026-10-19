//! Warm set byte limit (design: docs/lastdb-warm-set-byte-limit-design.md, D2 and D4).
//!
//! Stage 1 reports the limit next to the charged bytes. The record count in
//! [`super::RESIDENT_KEY_CAP`] still controls admission until stage 3.

/// Default warm set byte limit (Tom, 2026-10-19: 256 MiB to start).
pub const DEFAULT_CORE_WARM_BYTES: usize = 256 * 1024 * 1024;

/// Env override for the warm set byte limit. Zero and invalid values are refused.
pub const CORE_WARM_BYTES_ENV: &str = "LASTDB_CORE_WARM_BYTES";

/// Unflushed writes may use this percentage of the limit (D4).
pub const CORE_WARM_DIRTY_PERCENT: usize = 25;

static CORE_WARM_BYTES_IN_FORCE: std::sync::OnceLock<usize> = std::sync::OnceLock::new();

/// Parse the limit override. `None` (unset) is [`DEFAULT_CORE_WARM_BYTES`].
pub fn parse_core_warm_bytes(raw: Option<&str>) -> Result<usize, String> {
    let Some(raw) = raw else {
        return Ok(DEFAULT_CORE_WARM_BYTES);
    };
    match raw.trim().parse::<usize>() {
        Ok(0) => Err(format!(
            "{CORE_WARM_BYTES_ENV}=0 is refused; the limit must be positive"
        )),
        Ok(bytes) => Ok(bytes),
        Err(error) => Err(format!("invalid {CORE_WARM_BYTES_ENV}={raw:?}: {error}")),
    }
}

/// Read [`CORE_WARM_BYTES_ENV`] once, at boot. The daemon refuses to start on a bad value.
pub fn init_core_warm_bytes_from_env() -> Result<usize, String> {
    let bytes = parse_core_warm_bytes(std::env::var(CORE_WARM_BYTES_ENV).ok().as_deref())?;
    Ok(*CORE_WARM_BYTES_IN_FORCE.get_or_init(|| bytes))
}

/// The limit in force: [`DEFAULT_CORE_WARM_BYTES`] until boot ran the env parse.
#[must_use]
pub fn core_warm_bytes() -> usize {
    CORE_WARM_BYTES_IN_FORCE
        .get()
        .copied()
        .unwrap_or(DEFAULT_CORE_WARM_BYTES)
}

/// The dirty share of `limit`.
#[must_use]
pub fn core_warm_dirty_bytes(limit: usize) -> usize {
    limit / 100 * CORE_WARM_DIRTY_PERCENT
}
