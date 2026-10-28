//! Native write limits.
//!
//! | Env | Meaning |
//! |-----|---------|
//! | `LASTDB_RESIDENT_MAX_DEFERRED` | In-flight write reservation and purge queue count limit. Default `512`. Byte and complete-request limits also apply. |

/// Env: maximum in-flight write reservations and queued purge tasks.
pub const RESIDENT_MAX_DEFERRED_ENV: &str = "LASTDB_RESIDENT_MAX_DEFERRED";

/// Default count cap for in-flight write reservations and purge tasks.
pub const DEFAULT_RESIDENT_MAX_DEFERRED: usize = 512;

/// Resolved native write policy for this process.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResidentPolicy {
    /// Count cap for in-flight write reservations and purge tasks.
    pub max_deferred_persists: usize,
}

impl Default for ResidentPolicy {
    fn default() -> Self {
        Self {
            max_deferred_persists: DEFAULT_RESIDENT_MAX_DEFERRED,
        }
    }
}

impl ResidentPolicy {
    /// Parse from environment (and defaults) via the free `parse_*` helpers.
    pub fn from_env() -> Self {
        Self {
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
