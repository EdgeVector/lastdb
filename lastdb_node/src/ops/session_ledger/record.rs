//! Session record, restart constants and start summary types. Moved verbatim from `session_ledger.rs`.

use super::*;

/// Stable causes that live restart producers may write.
pub const RESTART_CAUSE_UPGRADE: &str = "upgrade";
pub const RESTART_CAUSE_GUARD_MEMORY: &str = "guard-memory";
pub const RESTART_CAUSE_OPERATOR: &str = "operator";

/// A restart intent is useful only for the immediate supervised restart. The
/// previous-PID fence is primary; this age fence also rejects abandoned files.
pub(super) const RESTART_INTENT_MAX_AGE_SECS: u64 = 15 * 60;
pub(super) const RESTART_INTENT_MAX_BYTES: u64 = 4 * 1024;

/// The newest boot row must remain a bounded point read. A session row has a
/// small fixed shape; this cap prevents a damaged or hostile ledger tail from
/// turning the canary identity probe into a full ledger scan.
pub(super) const LAST_RECORD_MAX_BYTES: u64 = 64 * 1024;

/// The owner can ask for a short boot history for canary reconciliation. Keep
/// this limit small so the route remains a bounded tail read.
pub const RECENT_RECORD_LIMIT: usize = 64;

/// One session's row in `sessions.jsonl`.
///
/// Written once at start (no `end_ts`); the `end_ts`/`exit` fields are filled
/// in by [`mark_clean_shutdown`] on graceful exit. `last_heartbeat_ts` is NOT
/// stored here — it lives in `current-session.json` so we don't rewrite the
/// whole ledger every minute — but a copy is folded back in at clean shutdown
/// so a fully-recorded clean session is self-describing.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SessionRecord {
    /// OS process id of this session.
    pub pid: u32,
    /// Unix epoch seconds when this session started.
    pub start_ts: u64,
    /// `FOLDDB_BUILD_VERSION` / crate version of the binary that started it.
    pub build_version: String,
    /// HTTP port the node bound (best-effort — the canonical 9001 most of the
    /// time, a fallback otherwise).
    pub port: u16,

    /// Why this process started after the prior session. This is a stable
    /// canary-policy token, not a free-form diagnostic. It makes every boot
    /// row self-contained: build, PID, start time, and restart cause arrive in
    /// one bounded read.
    #[serde(default = "unknown_restart_cause")]
    pub restart_cause: String,

    /// If this session took over the database from another LastDB process, the
    /// PID it took over from.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub took_over_from_pid: Option<u32>,

    /// Whether the *previous* session ended cleanly (had an `end_ts`). `true`
    /// only when a prior session existed AND recorded a clean shutdown.
    pub prev_session_clean: bool,
    /// Whether the previous session recorded any terminal disposition. This
    /// distinguishes a supervised shutdown interrupted during its drain from a
    /// native crash that left no exit record at all.
    #[serde(default)]
    pub prev_session_exit_recorded: bool,
    /// Downtime between the previous session's last sign of life (its `end_ts`
    /// if clean, else its `last_heartbeat_ts`) and this session's `start_ts`,
    /// in seconds. `None` when there was no previous session.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prev_downtime_secs: Option<u64>,
    /// Best-effort OS "previous shutdown cause" captured at this start. macOS
    /// only (read from `pmset -g log`); `None` elsewhere or when unavailable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prev_shutdown_cause: Option<String>,

    /// Unix epoch seconds of the clean shutdown, set by [`mark_clean_shutdown`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end_ts: Option<u64>,
    /// Exit disposition (`"clean"`). Absent ⇒ the session never shut down
    /// cleanly (still running, or died uncleanly).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit: Option<String>,
    /// WHY the session exited, for clean exits that know their trigger
    /// ("quit (tray menu)", "startup: another owner holds the lock", …).
    /// Recorded so the next start — and the crash analyzer behind it — can
    /// tell a deliberate quit from a crash instead of lumping them together
    /// (the 2026-07-10 incident: menu-quits reported to Sentry as
    /// `unclean_exit`). Absent on records written before this field existed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_reason: Option<String>,
    /// The last heartbeat timestamp folded in at clean shutdown, so a finished
    /// clean record carries its own liveness tail.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_heartbeat_ts: Option<u64>,
}

impl SessionRecord {
    /// Did this (completed) session end cleanly?
    pub fn ended_clean(&self) -> bool {
        self.exit.as_deref() == Some(EXIT_CLEAN) && self.end_ts.is_some()
    }

    /// Did this session record why it was ending, even if its drain did not
    /// reach the final clean stamp before the supervisor stopped the process?
    pub fn ended_with_recorded_disposition(&self) -> bool {
        self.exit.is_some() && self.end_ts.is_some()
    }
}

pub(super) fn unknown_restart_cause() -> String {
    "unknown".to_string()
}

/// The mutable per-session heartbeat, persisted to `current-session.json`. Kept
/// tiny and rewritten in full each tick so a heartbeat is a single small write,
/// never an append-and-grow.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct CurrentSession {
    pub(super) pid: u32,
    pub(super) start_ts: u64,
    pub(super) last_heartbeat_ts: u64,
}

/// Durable handoff from a supervisor to the next daemon process.
///
/// The previous PID prevents a stale marker from labelling a later restart.
/// `created_at` limits damage when the intended restart never occurs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct RestartIntent {
    pub(super) cause: String,
    pub(super) previous_pid: u32,
    pub(super) created_at: u64,
}

#[derive(Debug, Default)]
pub(super) struct RestartIntentRead {
    pub(super) marker_present: bool,
    pub(super) cause: Option<String>,
}

/// What [`Ledger::record_start`] learned about the previous session — the
/// "how long was it down, and did it end cleanly?" answer, ready to log.
#[derive(Debug, Clone)]
pub struct StartSummary {
    /// Whether a previous session existed at all.
    pub had_previous: bool,
    /// Whether the previous session ended cleanly.
    pub prev_session_clean: bool,
    /// Whether the previous session left any explicit exit disposition.
    pub prev_session_exit_recorded: bool,
    /// Downtime since the previous session's last sign of life, in seconds.
    pub prev_downtime_secs: Option<u64>,
    /// OS shutdown cause captured at this start (macOS only).
    pub prev_shutdown_cause: Option<String>,
    /// The previous session's recorded exit reason, when it left one (clean
    /// exits that knew their trigger). `None` for unclean ends and old records.
    pub prev_exit_reason: Option<String>,
    /// The previous session's recorded exit disposition ([`EXIT_CLEAN`],
    /// [`EXIT_SHUTDOWN_STARTED`], [`EXIT_STARTUP_FAILED`]). `None` when it left
    /// none — the native-crash case. Distinguishes the *kinds* of recorded exit
    /// that [`Self::prev_session_exit_recorded`] collapses into one flag.
    pub prev_exit: Option<String>,
}

impl StartSummary {
    /// A one-line, human-readable summary suitable for a WARN/INFO log. Returns
    /// `None` on a first-ever start (nothing to summarize).
    pub fn log_line(&self) -> Option<String> {
        if !self.had_previous {
            return None;
        }
        let downtime = self
            .prev_downtime_secs
            .map_or_else(|| "unknown".to_string(), format_duration);
        let cause = self
            .prev_shutdown_cause
            .as_deref()
            .map(|c| format!(", prior shutdown cause: {c}"))
            .unwrap_or_default();
        if self.prev_session_clean {
            let reason = self
                .prev_exit_reason
                .as_deref()
                .map(|r| format!(" ({r})"))
                .unwrap_or_default();
            Some(format!(
                "Previous session ended cleanly{reason}; down for {downtime} before this start{cause}."
            ))
        } else if self.prev_exit.as_deref() == Some(EXIT_STARTUP_FAILED) {
            let reason = self
                .prev_exit_reason
                .as_deref()
                .map(|r| format!(": {r}"))
                .unwrap_or_default();
            Some(format!(
                "Previous session FAILED TO START and exited with a recorded error{reason}; \
                 down for {downtime} before this start{cause}."
            ))
        } else if self.prev_session_exit_recorded {
            let reason = self
                .prev_exit_reason
                .as_deref()
                .map(|r| format!(" ({r})"))
                .unwrap_or_default();
            Some(format!(
                "Previous session entered supervised shutdown{reason} but did not complete its clean drain; \
                 down for {downtime} before this start{cause}."
            ))
        } else {
            Some(format!(
                "Previous session ended UNCLEANLY (no clean shutdown record); \
                 down for {downtime} before this start{cause}."
            ))
        }
    }
}
