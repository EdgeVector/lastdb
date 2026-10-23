//! Native write limits and conservative process reserve.
//!
//! | Env | Meaning |
//! |-----|---------|
//! | `LASTDB_RESIDENT_BYTES` | Conservative process headroom reserve. Default 2 GiB. This does not bound native core metadata, body owners, or logical paths. |
//! | `LASTDB_RESIDENT_MAX_DEFERRED` | In-flight write reservation and purge queue count limit. Default `512`. Byte and complete-request limits also apply. |

/// Env: conservative process headroom reserve, separate from native core owners.
pub const RESIDENT_BYTES_ENV: &str = "LASTDB_RESIDENT_BYTES";

/// Env: maximum in-flight write reservations and queued purge tasks.
pub const RESIDENT_MAX_DEFERRED_ENV: &str = "LASTDB_RESIDENT_MAX_DEFERRED";

/// Default conservative process reserve (`LASTDB_RESIDENT_BYTES`).
///
/// The process projection retains this headroom after the graph cache removal.
/// Native core values use charged owners; logical paths have a separate count cap.
pub const DEFAULT_RESIDENT_BYTES: u64 = 2 * 1024 * 1024 * 1024;

/// Default count cap for in-flight write reservations and purge tasks.
pub const DEFAULT_RESIDENT_MAX_DEFERRED: usize = 512;

/// Resolved native write policy and process reserve for this process.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResidentPolicy {
    pub budget_bytes: u64,
    /// Count cap for in-flight write reservations and purge tasks.
    pub max_deferred_persists: usize,
}

impl Default for ResidentPolicy {
    fn default() -> Self {
        Self {
            budget_bytes: DEFAULT_RESIDENT_BYTES,
            max_deferred_persists: DEFAULT_RESIDENT_MAX_DEFERRED,
        }
    }
}

impl ResidentPolicy {
    /// Parse from environment (and defaults) via the free `parse_*` helpers.
    pub fn from_env() -> Self {
        Self {
            budget_bytes: parse_resident_bytes(std::env::var(RESIDENT_BYTES_ENV).ok()),
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
