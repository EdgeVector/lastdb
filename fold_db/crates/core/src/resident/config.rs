//! Resident-graph policy — mode, memory budget, persist cadence.
//!
//! | Env | Meaning |
//! |-----|---------|
//! | `LASTDB_RESIDENT_MODE` | `off` (default) / `read` / `write` — how far resident-primary routing is enabled. `write` acks mutations after resident tip+atom install and defers LastStore puts (tracked on `pending_tasks`). |
//! | `LASTDB_RESIDENT_BYTES` | Byte budget for the ResidentGraph (`LASTDB_RESIDENT_MODE=write`). Default 2 GiB. Clean graph entries evict LRU-first over budget; dirty entries are never evicted. Does not size the logical resident set (that cap is [`crate::resident::RESIDENT_KEY_CAP`]). |
//! | `LASTDB_RESIDENT_PERSIST_MS` | Legacy repair-worker cadence. FoldDB does not start this worker. Schema lanes own mutation persistence. |
//! | `LASTDB_RESIDENT_MAX_DEFERRED` | Cap on fast resident acknowledgments. Over the byte window, a write waits for its schema-lane envelope. Default `512`. |

use std::time::Duration;

/// Env: resident-primary routing mode (`off` / `read` / `write`).
pub const RESIDENT_MODE_ENV: &str = "LASTDB_RESIDENT_MODE";

/// Env: ResidentGraph byte budget. Does not size the logical resident set.
pub const RESIDENT_BYTES_ENV: &str = "LASTDB_RESIDENT_BYTES";

/// Env: legacy explicit repair-worker cadence in milliseconds (`0` = off).
pub const RESIDENT_PERSIST_MS_ENV: &str = "LASTDB_RESIDENT_PERSIST_MS";

/// Env: max in-flight fast resident acknowledgments before a write waits for
/// schema-lane durability.
pub const RESIDENT_MAX_DEFERRED_ENV: &str = "LASTDB_RESIDENT_MAX_DEFERRED";

/// Default ResidentGraph byte budget (`LASTDB_RESIDENT_BYTES`).
///
/// This bounds the ResidentGraph (`LASTDB_RESIDENT_MODE=write`). It does not
/// size the logical resident set. That set is capped by
/// [`crate::resident::RESIDENT_KEY_CAP`] used records (tips and atoms).
pub const DEFAULT_RESIDENT_BYTES: u64 = 2 * 1024 * 1024 * 1024;

/// Default legacy repair cadence. FoldDB production construction ignores it.
pub const DEFAULT_RESIDENT_PERSIST_MS: u64 = 500;

/// Default deferred-persist cap. Each task holds one batch's atoms + molecule
/// state plus its write guards; 512 in flight is a burst, not a leak — the
/// 2026-07-29 guard-kill incident showed an UNBOUNDED queue reaches 12+ GiB
/// RSS in under a minute (brain `lastdb-resident-write-rss-balloon-guard-kill`).
pub const DEFAULT_RESIDENT_MAX_DEFERRED: usize = 512;

/// How far resident-primary routing is enabled on this node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ResidentMode {
    /// No new routing; resident stays a passive install target.
    #[default]
    Off,
    /// Read paths serve resident-first.
    Read,
    /// Reads resident-first AND mutations ack on resident apply.
    Write,
}

impl ResidentMode {
    pub fn reads_resident_first(self) -> bool {
        !matches!(self, Self::Off)
    }

    pub fn acks_on_resident(self) -> bool {
        matches!(self, Self::Write)
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::Read => "read",
            Self::Write => "write",
        }
    }
}

/// Resolved resident policy for this process.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResidentPolicy {
    pub mode: ResidentMode,
    pub budget_bytes: u64,
    /// Legacy repair-worker policy. FoldDB production construction does not
    /// start this worker.
    pub persist_interval: Option<Duration>,
    /// Cap on in-flight deferred persist tasks under mode=write.
    pub max_deferred_persists: usize,
}

impl Default for ResidentPolicy {
    fn default() -> Self {
        Self {
            mode: ResidentMode::Off,
            budget_bytes: DEFAULT_RESIDENT_BYTES,
            persist_interval: Some(Duration::from_millis(DEFAULT_RESIDENT_PERSIST_MS)),
            max_deferred_persists: DEFAULT_RESIDENT_MAX_DEFERRED,
        }
    }
}

impl ResidentPolicy {
    /// Parse from environment (and defaults). Pure via the free `parse_*`
    /// helpers so unit tests never race `std::env`.
    pub fn from_env() -> Self {
        Self {
            mode: parse_resident_mode(std::env::var(RESIDENT_MODE_ENV).ok()),
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
/// default; `0` is rejected (it would silently disable mode=write's ack path)
/// and also falls back to the default.
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

/// `LASTDB_RESIDENT_MODE` → mode. Unknown values log and stay `Off` (fail
/// closed: never enable routing on a typo).
#[must_use]
pub fn parse_resident_mode(raw: Option<String>) -> ResidentMode {
    let Some(raw) = raw else {
        return ResidentMode::Off;
    };
    match raw.trim().to_ascii_lowercase().as_str() {
        "" | "off" | "0" | "false" => ResidentMode::Off,
        "read" => ResidentMode::Read,
        "write" => ResidentMode::Write,
        other => {
            tracing::warn!(
                raw = %other,
                env = RESIDENT_MODE_ENV,
                "unknown resident mode; staying off"
            );
            ResidentMode::Off
        }
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

/// `LASTDB_RESIDENT_PERSIST_MS` → legacy repair cadence (`0` = off).
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
