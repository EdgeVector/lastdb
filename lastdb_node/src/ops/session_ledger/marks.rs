//! Shutdown and startup-failure marks on the ledger. Moved verbatim from `session_ledger.rs`.

use super::*;

/// Mark the current session in `$FOLDDB_HOME/sessions.jsonl` as cleanly shut
/// down. Remove `current-session.json` first, then stamp `end_ts` and
/// `exit: "clean"` on the newest ledger line whose pid matches.
///
/// The caller must stop the heartbeat before this call. A marker removal error
/// prevents the clean stamp. A repeated call keeps the first exit reason.
pub fn mark_clean_shutdown(home: &Path, pid: u32) -> std::io::Result<()> {
    mark_clean_shutdown_with_reason(home, pid, None)
}

/// Persist the supervisor's shutdown request before asynchronous drain work.
///
/// This leaves `current-session.json` in place while the process is still
/// draining. A later clean stamp promotes the same record to [`EXIT_CLEAN`].
/// If the supervisor's kill window expires first, the next boot can distinguish
/// the recorded shutdown attempt from a native crash with no disposition.
pub fn mark_shutdown_started_with_reason(
    home: &Path,
    pid: u32,
    reason: Option<&str>,
) -> std::io::Result<()> {
    let mut records = read_all_records(home);
    let end_ts = unix_secs();
    let last_heartbeat_ts = read_current_session(home).map(|c| c.last_heartbeat_ts);

    if let Some(rec) = records
        .iter_mut()
        .rev()
        .find(|r| r.pid == pid && r.exit.is_none())
    {
        rec.end_ts = Some(end_ts);
        rec.exit = Some(EXIT_SHUTDOWN_STARTED.to_string());
        rec.exit_reason = reason.map(str::to_string);
        rec.last_heartbeat_ts = last_heartbeat_ts.or(Some(end_ts));
        rewrite_ledger(home, &records)?;
    }
    Ok(())
}

/// Record that this session is exiting because its own startup or serve path
/// returned a fatal error, and say which one.
///
/// Without this stamp the ledger line stays open, and the next boot cannot tell
/// a self-diagnosed startup failure from a SIGKILL / panic / power loss — so it
/// reports a phantom "native crash" instead of the real reason. A `lastdbd`
/// that cannot open its store exits this way on every launchd restart, which
/// turned one boot-loop into a stream of misattributed crash events
/// (Sentry `rust` issue 7601263508: 40 events in four minutes on 2026-08-16,
/// every one of them actually `cannot open existing store`).
///
/// Best-effort, like the rest of the ledger: a failing process must not fail
/// harder because it also couldn't write down why it was failing.
pub fn mark_startup_failed(home: &Path, pid: u32, reason: Option<&str>) -> std::io::Result<()> {
    let mut records = read_all_records(home);
    let end_ts = unix_secs();
    let last_heartbeat_ts = read_current_session(home).map(|c| c.last_heartbeat_ts);

    if let Some(rec) = records
        .iter_mut()
        .rev()
        .find(|r| r.pid == pid && r.exit.is_none())
    {
        rec.end_ts = Some(end_ts);
        rec.exit = Some(EXIT_STARTUP_FAILED.to_string());
        rec.exit_reason = reason.map(str::to_string);
        rec.last_heartbeat_ts = last_heartbeat_ts.or(Some(end_ts));
        rewrite_ledger(home, &records)?;
    }

    // The session is over before it ever served; drop the live heartbeat marker
    // so the next start doesn't read it as a still-live session.
    let _ = std::fs::remove_file(Ledger::current_session_path(home));
    Ok(())
}

/// [`mark_clean_shutdown`] carrying WHY the session is exiting ("quit (tray
/// menu)", "startup: relaunched last-known-good", …). First stamp wins: a
/// record already closed clean keeps its original reason, so a specific
/// quit-path mark followed by a generic process-exit fallback never loses the
/// specific reason.
pub fn mark_clean_shutdown_with_reason(
    home: &Path,
    pid: u32,
    reason: Option<&str>,
) -> std::io::Result<()> {
    let mut records = read_all_records(home);
    let end_ts = unix_secs();
    let last_heartbeat_ts = read_current_session(home).map(|c| c.last_heartbeat_ts);

    // A clean stamp must not outlive the live marker. The caller stops and
    // joins the heartbeat thread before this removal.
    remove_current_session(home)?;

    // Stamp an open record, or promote a prior shutdown-started disposition to
    // clean after the drain and final flush complete.
    if let Some(rec) = records
        .iter_mut()
        .rev()
        .find(|r| r.pid == pid && r.exit.as_deref() != Some(EXIT_CLEAN))
    {
        rec.end_ts = Some(end_ts);
        rec.exit = Some(EXIT_CLEAN.to_string());
        if rec.exit_reason.is_none() {
            rec.exit_reason = reason.map(str::to_string);
        }
        rec.last_heartbeat_ts = last_heartbeat_ts.or(Some(end_ts));
        rewrite_ledger(home, &records)?;
    }

    Ok(())
}

pub(super) fn remove_current_session(home: &Path) -> std::io::Result<()> {
    match std::fs::remove_file(Ledger::current_session_path(home)) {
        Ok(()) => std::fs::File::open(home)?.sync_all(),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}
