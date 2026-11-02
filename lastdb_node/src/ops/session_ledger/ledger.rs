//! The `Ledger` handle: start, heartbeat and own-record access. Moved verbatim from `session_ledger.rs`.

use super::*;

/// Handle to a node's session ledger, scoped to a `$LASTDB_HOME` directory.
///
/// Holds the current session's identity so [`Ledger::heartbeat`] and
/// [`mark_clean_shutdown`] can target the right ledger line without re-reading
/// it. Cheap to clone.
#[derive(Debug, Clone)]
pub struct Ledger {
    pub(super) home: PathBuf,
    pub(super) pid: u32,
    pub(super) start_ts: u64,
    /// This session's own boot row, exactly as appended to `sessions.jsonl`
    /// at start. Kept separate from a re-read of the ledger file because a
    /// different process can boot against this same home afterward and
    /// append a newer row — [`Self::own_record`] must still answer with THIS
    /// session's identity, never "whatever is newest in the shared file".
    pub(super) own: SessionRecord,
}

/// Facts known at session start that fill the new boot row.
struct SessionStart<'a> {
    pid: u32,
    start_ts: u64,
    port: u16,
    took_over_from_pid: Option<u32>,
    restart_cause: Option<String>,
    prev: Option<&'a SessionRecord>,
    prev_session_clean: bool,
    prev_session_exit_recorded: bool,
    prev_downtime_secs: Option<u64>,
    prev_shutdown_cause: Option<String>,
}

impl SessionRecord {
    /// The boot row for a new session. A producer-annotated restart cause wins;
    /// otherwise the cause is classified from the previous session.
    fn for_start(start: SessionStart<'_>) -> Self {
        let build_version = env!("FOLDDB_BUILD_VERSION").to_string();
        let restart_cause = start.restart_cause.unwrap_or_else(|| {
            classify_restart_cause(
                start.prev,
                &build_version,
                start.prev.and_then(|p| p.exit.as_deref()),
                start.prev.and_then(|p| p.exit_reason.as_deref()),
                start.prev_shutdown_cause.as_deref(),
            )
        });
        Self {
            pid: start.pid,
            start_ts: start.start_ts,
            build_version,
            port: start.port,
            restart_cause,
            took_over_from_pid: start.took_over_from_pid,
            prev_session_clean: start.prev_session_clean,
            prev_session_exit_recorded: start.prev_session_exit_recorded,
            prev_downtime_secs: start.prev_downtime_secs,
            prev_shutdown_cause: start.prev_shutdown_cause,
            end_ts: None,
            exit: None,
            exit_reason: None,
            last_heartbeat_ts: None,
        }
    }
}

impl Ledger {
    /// `$LASTDB_HOME/sessions.jsonl`.
    pub fn ledger_path(home: &Path) -> PathBuf {
        home.join("sessions.jsonl")
    }

    /// `$LASTDB_HOME/current-session.json`.
    pub fn current_session_path(home: &Path) -> PathBuf {
        home.join("current-session.json")
    }

    /// `$LASTDB_HOME/restart-intent.json`.
    pub fn restart_intent_path(home: &Path) -> PathBuf {
        home.join(RESTART_INTENT_FILE)
    }

    /// Record the start of a new session. Reads the previous session record (if
    /// any), determines clean/unclean + downtime, captures the OS shutdown
    /// cause, appends the new record to `sessions.jsonl`, and writes a fresh
    /// `current-session.json`.
    ///
    /// Best-effort: returns an `Err` (so the caller can WARN) but a node should
    /// treat any failure here as non-fatal.
    pub fn record_start(
        home: &Path,
        pid: u32,
        port: u16,
        took_over_from_pid: Option<u32>,
    ) -> std::io::Result<(Self, StartSummary)> {
        let start_ts = unix_secs();
        let prev = read_last_record(home);
        let current = read_current_session(home);

        let restart_intent = read_restart_intent(home, prev.as_ref(), start_ts);
        let prev_last_alive = previous_last_alive(prev.as_ref(), current.as_ref());

        let prev_session_clean = prev.as_ref().is_some_and(SessionRecord::ended_clean);
        let prev_session_exit_recorded = prev
            .as_ref()
            .is_some_and(SessionRecord::ended_with_recorded_disposition);
        let prev_exit_reason = prev.as_ref().and_then(|p| p.exit_reason.clone());
        let prev_exit = prev.as_ref().and_then(|p| p.exit.clone());
        let prev_downtime_secs = prev_last_alive.map(|t| start_ts.saturating_sub(t));
        // `pmset -g log` dumps the machine's ENTIRE power log (routinely tens
        // of MB — ~2-3s wall clock), so only pay for it when the answer
        // matters: the previous session ended UNCLEAN and the OS shutdown
        // cause is the "did the machine die under us" forensic signal. A
        // clean relaunch (the every-day case) and a first run skip it — the
        // unconditional capture was silently costing every single app launch
        // multiple seconds on the startup path.
        let prev_shutdown_cause =
            if should_capture_shutdown_cause(prev.is_some(), prev_session_exit_recorded) {
                capture_shutdown_cause()
            } else {
                None
            };
        let record = SessionRecord::for_start(SessionStart {
            pid,
            start_ts,
            port,
            took_over_from_pid,
            restart_cause: restart_intent.cause.clone(),
            prev: prev.as_ref(),
            prev_session_clean,
            prev_session_exit_recorded,
            prev_downtime_secs,
            prev_shutdown_cause: prev_shutdown_cause.clone(),
        });

        append_record(home, &record)?;
        // Consume only after the boot row is durable. A failed append leaves
        // the producer annotation available for the next start attempt.
        if restart_intent.marker_present {
            let _ = std::fs::remove_file(Self::restart_intent_path(home));
        }
        // Seed the heartbeat file immediately so a very short-lived session
        // still leaves a liveness tail for the next start.
        write_current_session(
            home,
            &CurrentSession {
                pid,
                start_ts,
                last_heartbeat_ts: start_ts,
            },
        )?;

        let summary = StartSummary {
            had_previous: prev.is_some(),
            prev_session_clean,
            prev_session_exit_recorded,
            prev_downtime_secs,
            prev_shutdown_cause,
            prev_exit_reason,
            prev_exit,
        };

        Ok((
            Self {
                home: home.to_path_buf(),
                pid,
                start_ts,
                own: record,
            },
            summary,
        ))
    }

    /// This session's own boot identity — pid, start time, build, restart
    /// cause — captured once at [`Ledger::record_start`]. Use this for any
    /// "who am I" answer (e.g. `/api/system/boot-identity`); never re-derive
    /// it from [`read_last_record`], which reads whatever line is newest in
    /// the shared per-home ledger file and can be a different process's row
    /// (papercut-lastdb-primary-boot-identity-stale-phantom-pid-20260927: a
    /// stray process sharing this home appended a newer row, and the route
    /// served that foreign pid/build as the live primary's own identity for
    /// 5+ hours).
    pub fn own_record(&self) -> SessionRecord {
        self.own.clone()
    }

    /// Update the current session's `last_heartbeat_ts` by rewriting the small
    /// `current-session.json` file. Cheap — never touches the append-only
    /// ledger. Best-effort.
    pub fn heartbeat(&self) -> std::io::Result<()> {
        self.heartbeat_at(unix_secs())
    }

    pub(super) fn heartbeat_at(&self, last_heartbeat_ts: u64) -> std::io::Result<()> {
        write_current_session(
            &self.home,
            &CurrentSession {
                pid: self.pid,
                start_ts: self.start_ts,
                last_heartbeat_ts,
            },
        )
    }
}

/// The previous session's "last sign of life": its clean end_ts if it shut
/// down cleanly, else the most recent heartbeat. current-session.json
/// (rewritten every ~60s) usually has a fresher timestamp than the ledger
/// line, so prefer it when it belongs to the same prior pid.
fn previous_last_alive(
    prev: Option<&SessionRecord>,
    current: Option<&CurrentSession>,
) -> Option<u64> {
    prev.and_then(|p| {
        if p.ended_with_recorded_disposition() {
            p.end_ts
        } else {
            // Heartbeat file (if it's the prior session's) beats the ledger
            // line, which for an unclean session has no liveness tail.
            current
                .filter(|c| c.pid == p.pid)
                .map(|c| c.last_heartbeat_ts)
                .or(p.last_heartbeat_ts)
                .or(Some(p.start_ts))
        }
    })
}
