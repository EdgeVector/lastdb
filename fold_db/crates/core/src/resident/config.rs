//! Native write limits, conservative process reserve, and flush delay.
//!
//! | Env | Meaning |
//! |-----|---------|
//! | `LASTDB_RESIDENT_BYTES` | Conservative process headroom reserve. Default 2 GiB. This does not bound native core metadata, body owners, or logical paths. |
//! | `LASTDB_RESIDENT_PERSIST_MS` | Flush delay from the first accepted request in a prefix. Default 500 ms; native open requires 1–500 ms. |
//! | `LASTDB_RESIDENT_MAX_DEFERRED` | In-flight write reservation and purge queue count limit. Default `512`. Byte and complete-request limits also apply. |

use std::time::Duration;

/// Env: conservative process headroom reserve, separate from native core owners.
pub const RESIDENT_BYTES_ENV: &str = "LASTDB_RESIDENT_BYTES";

/// Env: native memory-ack flush delay in milliseconds (must be 1–500 when active).
pub const RESIDENT_PERSIST_MS_ENV: &str = "LASTDB_RESIDENT_PERSIST_MS";

/// Env: maximum in-flight write reservations and queued purge tasks.
pub const RESIDENT_MAX_DEFERRED_ENV: &str = "LASTDB_RESIDENT_MAX_DEFERRED";

/// Default conservative process reserve (`LASTDB_RESIDENT_BYTES`).
///
/// The process projection retains this headroom after the graph cache removal.
/// Native core values use charged owners; logical paths have a separate count cap.
pub const DEFAULT_RESIDENT_BYTES: u64 = 2 * 1024 * 1024 * 1024;

/// Default native memory-ack flush delay, validated by the native store.
pub const DEFAULT_RESIDENT_PERSIST_MS: u64 = 500;

/// Default count cap for in-flight write reservations and purge tasks.
pub const DEFAULT_RESIDENT_MAX_DEFERRED: usize = 512;

/// Resolved native write policy and process reserve for this process.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResidentPolicy {
    pub budget_bytes: u64,
    /// Native flush delay. Store admission requires a value in 1–500 ms.
    pub persist_interval: Option<Duration>,
    /// Count cap for in-flight write reservations and purge tasks.
    pub max_deferred_persists: usize,
}

impl Default for ResidentPolicy {
    fn default() -> Self {
        Self {
            budget_bytes: DEFAULT_RESIDENT_BYTES,
            persist_interval: Some(Duration::from_millis(DEFAULT_RESIDENT_PERSIST_MS)),
            max_deferred_persists: DEFAULT_RESIDENT_MAX_DEFERRED,
        }
    }
}

impl ResidentPolicy {
    /// Parse from environment (and defaults) via the free `parse_*` helpers.
    pub fn from_env() -> Self {
        Self {
            budget_bytes: parse_resident_bytes(std::env::var(RESIDENT_BYTES_ENV).ok()),
            persist_interval: parse_resident_persist_interval(
                std::env::var(RESIDENT_PERSIST_MS_ENV).ok(),
            ),
            max_deferred_persists: parse_resident_max_deferred(
                std::env::var(RESIDENT_MAX_DEFERRED_ENV).ok(),
            ),
        }
    }
}

/// `LASTDB_RESIDENT_MAX_DEFERRED` → cap. Invalid values log and use the
/// default. Zero is rejected so the reservation count remains positive.
#[must_use]
pub fn parse_resident_max_deferred(raw: Option<String>) -> usize {
    let Some(raw) = raw else {
        return DEFAULT_RESIDENT_MAX_DEFERRED;
    };
    match raw.trim().parse::<usize>() {
        Ok(0) | Err(_) => {
            tracing::warn!(
                raw = %raw,
                env = RESIDENT_MAX_DEFERRED_ENV,
                default = DEFAULT_RESIDENT_MAX_DEFERRED,
                "invalid LASTDB_RESIDENT_MAX_DEFERRED; using default"
            );
            DEFAULT_RESIDENT_MAX_DEFERRED
        }
        Ok(cap) => cap,
    }
}

/// `LASTDB_RESIDENT_BYTES` → budget. Invalid values log and use the default.
#[must_use]
pub fn parse_resident_bytes(raw: Option<String>) -> u64 {
    let Some(raw) = raw else {
        return DEFAULT_RESIDENT_BYTES;
    };
    if let Ok(bytes) = raw.trim().parse::<u64>() {
        bytes
    } else {
        tracing::warn!(
            raw = %raw,
            env = RESIDENT_BYTES_ENV,
            default = DEFAULT_RESIDENT_BYTES,
            "invalid LASTDB_RESIDENT_BYTES; using default"
        );
        DEFAULT_RESIDENT_BYTES
    }
}

/// Parse the native flush delay; `0` means None and native store admission rejects it.
#[must_use]
pub fn parse_resident_persist_interval(raw: Option<String>) -> Option<Duration> {
    let Some(raw) = raw else {
        return Some(Duration::from_millis(DEFAULT_RESIDENT_PERSIST_MS));
    };
    let trimmed = raw.trim();
    match trimmed.parse::<u64>() {
        Ok(0) => None,
        Ok(ms) => Some(Duration::from_millis(ms)),
        Err(_) => {
            tracing::warn!(
                raw = %trimmed,
                env = RESIDENT_PERSIST_MS_ENV,
                default_ms = DEFAULT_RESIDENT_PERSIST_MS,
                "invalid LASTDB_RESIDENT_PERSIST_MS; using default"
            );
            Some(Duration::from_millis(DEFAULT_RESIDENT_PERSIST_MS))
        }
    }
}
