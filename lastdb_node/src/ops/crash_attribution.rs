//! Daemon-side wiring for uptime + crash attribution.
//!
//! Ties together the two ported building blocks — [`crate::session_ledger`]
//! (the uptime / clean-vs-unclean ledger) and [`crate::crash_telemetry`] (the
//! Sentry promotion of crash evidence) — into the shapes the `lastdbd` boot
//! path and the `lastdb status` CLI actually call:
//!
//! - [`build_version`] — the release tag shared by crash reports, Sentry crash
//!   events, and the ERROR-level Sentry tracing layer installed by `lastdbd`.
//! - [`install_crash_hook`] / [`scan_previous_crashes`] — write a durable local
//!   crash report on every panic and surface unseen reports on the next boot.
//! - [`previous_log_tail`] — read the tail of the previous daemon log so an
//!   unclean-exit event can answer "what was it doing when it died".
//! - [`promote_previous_crash_evidence`] — the boot-time promotion call.
//! - [`status_lines`] — the human-readable "uptime + why it last died" lines
//!   the `lastdb status` command prints (the card's one-command answer).
//!
//! Everything here is best-effort: attribution must never keep the daemon from
//! booting or the CLI from answering.

use std::path::{Path, PathBuf};

use crate::session_ledger::{self, SessionRecord, StartSummary};

/// Cap on the previous-session log tail we read + attach (bytes). Matches the
/// event-side cap in [`crate::crash_telemetry`] so we never read materially
/// more than can ride the event.
const LOG_TAIL_READ_CAP: usize = 16 * 1024;

/// Env override naming the daemon log file whose tail rides an unclean-exit
/// event. Primarily a test seam; in production the launchd stdio path is
/// auto-resolved (see [`resolve_log_path`]).
const LOG_PATH_ENV: &str = "LASTDBD_LOG_PATH";

/// Homebrew launchd `StandardOutPath` default for `lastdbd` — the file the
/// sibling `lastdb-lastdbd-brew-log-rotation` card bounds. Used as the last
/// resort when the running fd cannot be resolved to a regular file.
const BREW_LOG_PATH: &str = "/opt/homebrew/var/log/lastdb/lastdbd.log";

/// The baked build version (`FOLDDB_BUILD_VERSION`, stamped by `build.rs`).
/// Same source the `--version` flag and the session ledger use, so a crash /
/// unclean-exit event is attributable to a real shipped tag.
pub fn build_version() -> &'static str {
    env!("FOLDDB_BUILD_VERSION")
}

/// Decide which release string this process reports to Sentry, given the baked
/// build version and whatever `OBS_SENTRY_RELEASE` was configured with.
///
/// **The baked version wins, always.** A running binary knows its own identity
/// with certainty; an operator-supplied release string is a hand-maintained
/// copy of that identity, and a copy can only ever be equal or stale. There is
/// no state in which the environment knows the running code better than the
/// code does.
///
/// It went stale exactly as predicted: `OBS_SENTRY_RELEASE` in the primary's
/// launchd plist read `lastdbd 0.22.10-canary.20260717-508-g94be90d94` while
/// the daemon ran `0.23.3-686-g32b0707e6` — 31 days and two minor versions
/// apart, across at least four cutovers that each edited other keys in the same
/// plist and left this one alone. Every Sentry event from that daemon named a
/// release it had not run for a month, so regression-by-release triage and
/// release health were both silently wrong. Brain:
/// `papercut-lastdbd-sentry-release-still-stale-after-the-0231-139-cutover`.
///
/// Returns the release to use plus, when a non-empty configured value
/// DISAGREED with the baked one, that ignored value — so the caller can warn
/// once rather than drop it silently. An operator who set it deliberately
/// deserves to be told it did nothing; an equal value is not worth a line.
#[must_use]
pub fn resolve_sentry_release<'a>(
    baked: &'a str,
    configured: Option<&str>,
) -> (&'a str, Option<String>) {
    let ignored = configured
        .map(str::trim)
        .filter(|v| !v.is_empty() && *v != baked)
        .map(ToOwned::to_owned);
    (baked, ignored)
}

/// Install [`build_version`] as this process's `OBS_SENTRY_RELEASE`, returning
/// any disagreeing operator value that was overridden.
///
/// Call this before the tracing ERROR layer binds a Sentry client in `lastdbd`.
/// The layer reads the env var, so the release comes from the baked version.
pub fn install_baked_sentry_release() -> Option<String> {
    use observability::layers::error::OBS_SENTRY_RELEASE_ENV;

    let configured = std::env::var(OBS_SENTRY_RELEASE_ENV).ok();
    let (release, ignored) = resolve_sentry_release(build_version(), configured.as_deref());
    std::env::set_var(OBS_SENTRY_RELEASE_ENV, release);
    ignored
}

// ---------------------------------------------------------------------------
// Crash-report hook + boot-time promotion
// ---------------------------------------------------------------------------

/// Install the on-panic crash-report hook so a Rust panic leaves a durable
/// `<home>/crash-reports/<ts>.txt` (message, location, backtrace, build
/// version) instead of an empty log. The daemon has no observability RING, so
/// the report's ring section is simply absent. Best-effort; call once at boot.
pub fn install_crash_hook(home: &Path) {
    observability::install_crash_hook(observability::CrashContext::new(
        home,
        build_version(),
        // Socket-only daemon — no HTTP port to record.
        None,
        None,
    ));
}

/// Surface crash reports written since the last clean start (pruning old
/// reports and refreshing the marker), minus any already shipped at crash
/// time. Returned newest-first, the order
/// [`promote_previous_crash_evidence`] expects.
pub fn scan_previous_crashes(home: &Path) -> Vec<PathBuf> {
    observability::scan_and_warn_previous_crashes(home)
        .into_iter()
        .filter(|p| !crate::crash_telemetry::is_report_sent(p))
        .collect()
}

/// Promote previous-session crash evidence into Sentry: one event per surfaced
/// panic report, plus a single `unclean_exit` event (carrying the log tail)
/// when the previous session ended uncleanly with no panic report to explain
/// it. No-op when no Sentry client is bound. Returns the number of events sent.
pub fn promote_previous_crash_evidence(
    reports: &[PathBuf],
    start: Option<&StartSummary>,
    prev_log_tail: Option<&str>,
) -> usize {
    let sent = crate::crash_telemetry::report_crashes_with_log_tail(reports, start, prev_log_tail);
    if sent > 0 {
        tracing::info!(
            target: "lastdbd::crash_attribution",
            "promoted {sent} crash event(s) from the previous session to Sentry"
        );
    }
    sent
}

// ---------------------------------------------------------------------------
// Previous-daemon log tail
// ---------------------------------------------------------------------------

/// Read the tail of the previous daemon log (up to [`LOG_TAIL_READ_CAP`]
/// bytes), for attaching to an unclean-exit event. Called EARLY at boot, before
/// this session writes much, so the file still holds the dying session's last
/// lines. `None` when no log file can be resolved/read.
pub fn previous_log_tail() -> Option<String> {
    let path = resolve_log_path()?;
    read_tail(&path, LOG_TAIL_READ_CAP)
}

/// Resolve the daemon log file path, in priority order: the `LASTDBD_LOG_PATH`
/// override, the running stderr fd's file (where tracing goes), the stdout fd's
/// file (`println!` / launchd `StandardOutPath`), then the Homebrew default.
fn resolve_log_path() -> Option<PathBuf> {
    if let Some(p) = std::env::var(LOG_PATH_ENV)
        .ok()
        .filter(|p| !p.is_empty())
        .map(PathBuf::from)
        .filter(|p| p.is_file())
    {
        return Some(p);
    }
    for fd in [libc::STDERR_FILENO, libc::STDOUT_FILENO] {
        if let Some(p) = crate::log_rotation::fd_regular_file_path(fd) {
            return Some(p);
        }
    }
    let brew = PathBuf::from(BREW_LOG_PATH);
    brew.is_file().then_some(brew)
}

/// Read the last `cap` bytes of `path` as (lossy) UTF-8. Tail-biased: keeps the
/// END of the file (the last things the dying session logged).
fn read_tail(path: &Path, cap: usize) -> Option<String> {
    let data = std::fs::read(path).ok()?;
    let start = data.len().saturating_sub(cap);
    Some(String::from_utf8_lossy(&data[start..]).into_owned())
}

// ---------------------------------------------------------------------------
// `lastdb status` surface — the one-command answer
// ---------------------------------------------------------------------------

/// Human-readable "current uptime + why it last died" lines for `lastdb
/// status`. `running` comes from the caller's socket health probe — it
/// distinguishes a live session (report uptime + the restart before it) from a
/// stopped/crashed daemon (report how the last session itself ended).
pub fn status_lines(home: &Path, running: bool) -> Vec<String> {
    let records = session_ledger::read_all_records(home);
    let live = session_ledger::read_live_session(home);

    let mut out = Vec::new();

    // Current uptime — only meaningful when a session is actually live.
    match (running, &live) {
        (true, Some(live)) => {
            let uptime = fold_db::clock::unix_secs().saturating_sub(live.start_ts);
            out.push(format!(
                "Uptime: {} (pid {}, since {})",
                session_ledger::format_duration(uptime),
                live.pid,
                fmt_epoch(live.start_ts),
            ));
        }
        _ => out.push("Uptime: not running".to_string()),
    }

    out.push(last_death_line(&records, running));
    out
}

/// First-line daemon status when the caller has the health-probe error.
///
/// A failed owner-socket `/health` request is not enough to call the daemon
/// stopped: the process can still be alive while every UDS worker is pinned.
/// That is operationally different from both "not reachable" and the short
/// crash-recovery boot window.
pub fn daemon_status_line_with_health_error(
    home: &Path,
    running: bool,
    health_error: Option<&str>,
) -> String {
    daemon_status_line_with_health_error_and_pid_check(
        home,
        running,
        health_error,
        process_is_alive,
    )
}

/// Whether the session ledger points at a currently-alive `lastdbd` PID.
pub fn live_daemon_pid_alive(home: &Path) -> bool {
    live_daemon_pid_alive_with_pid_check(home, process_is_alive)
}

fn daemon_status_line_with_health_error_and_pid_check(
    home: &Path,
    running: bool,
    health_error: Option<&str>,
    pid_is_alive: impl Fn(u32) -> bool + Copy,
) -> String {
    if running {
        return "running".to_string();
    }

    recovering_after_unclean_start(home, pid_is_alive).map_or_else(
        || {
            let Some(error) = health_error else {
                return "not reachable".to_string();
            };
            live_daemon_status_with_pid_check(home, pid_is_alive).map_or_else(
                || "not reachable".to_string(),
                |live| format!("wedged - pid {}, /health failed: {error}", live.pid),
            )
        },
        |recovery| {
            format!(
                "recovering (unclean prior exit) - pid {}, booting {}",
                recovery.pid,
                session_ledger::format_duration(recovery.boot_secs)
            )
        },
    )
}

#[derive(Clone, Copy)]
struct LiveDaemonStatus {
    pid: u32,
}

struct RecoveryStatus {
    pid: u32,
    boot_secs: u64,
}

fn recovering_after_unclean_start(
    home: &Path,
    pid_is_alive: impl Fn(u32) -> bool + Copy,
) -> Option<RecoveryStatus> {
    let live = session_ledger::read_live_session(home)?;
    let newest = session_ledger::read_last_record(home)?;

    if newest.ended_clean()
        || newest.prev_session_clean
        || newest.pid != live.pid
        || newest.start_ts != live.start_ts
        || !pid_is_alive(live.pid)
    {
        return None;
    }

    Some(RecoveryStatus {
        pid: live.pid,
        boot_secs: fold_db::clock::unix_secs().saturating_sub(live.start_ts),
    })
}

fn live_daemon_pid_alive_with_pid_check(
    home: &Path,
    pid_is_alive: impl Fn(u32) -> bool + Copy,
) -> bool {
    live_daemon_status_with_pid_check(home, pid_is_alive).is_some()
}

fn live_daemon_status_with_pid_check(
    home: &Path,
    pid_is_alive: impl Fn(u32) -> bool + Copy,
) -> Option<LiveDaemonStatus> {
    let live = session_ledger::read_live_session(home)?;
    pid_is_alive(live.pid).then_some(LiveDaemonStatus { pid: live.pid })
}

fn process_is_alive(pid: u32) -> bool {
    let Ok(pid) = libc::pid_t::try_from(pid) else {
        return false;
    };
    // SAFETY: `kill(pid, 0)` does not send a signal; it asks the kernel whether
    // the PID currently exists and is signalable by this process.
    if unsafe { libc::kill(pid, 0) != 0 } {
        return false;
    }

    process_comm(pid).is_none_or(|comm| {
        let name = Path::new(comm.trim())
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or_else(|| comm.trim());
        name == "lastdbd"
    })
}

fn process_comm(pid: libc::pid_t) -> Option<String> {
    let output = std::process::Command::new("ps")
        .args(["-p", &pid.to_string(), "-o", "comm="])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let comm = String::from_utf8(output.stdout).ok()?;
    (!comm.trim().is_empty()).then_some(comm)
}

/// The "why did it last die / restart?" line.
///
/// - Running: the interesting event is the restart BEFORE the live session —
///   described by the live session's start record, which captured how the
///   previous session ended (clean+reason vs unclean) and the downtime.
/// - Not running: the newest record IS the last (now-ended) session; describe
///   how it itself ended.
fn last_death_line(records: &[SessionRecord], running: bool) -> String {
    let Some(newest) = records.last() else {
        return "Last exit: no sessions recorded".to_string();
    };

    if running {
        if records.len() < 2 {
            return "Last restart: first recorded session (no prior session)".to_string();
        }
        // The previous session's own exit reason (for a clean end) lives on its
        // record; the "clean vs unclean + downtime + cause" lives on this
        // session's start record. Reuse StartSummary's rendering.
        let prev = &records[records.len() - 2];
        let prev_exit_reason = prev.exit_reason.clone();
        let prev_exit = prev.exit.clone();
        let summary = StartSummary {
            had_previous: true,
            prev_session_clean: newest.prev_session_clean,
            prev_session_exit_recorded: newest.prev_session_exit_recorded,
            prev_downtime_secs: newest.prev_downtime_secs,
            prev_shutdown_cause: newest.prev_shutdown_cause.clone(),
            prev_exit_reason,
            prev_exit,
        };
        return match summary.log_line() {
            Some(line) => format!("Last restart: {line}"),
            None => "Last restart: first recorded session (no prior session)".to_string(),
        };
    }

    if newest.ended_clean() {
        let reason = newest
            .exit_reason
            .as_deref()
            .map(|r| format!(" ({r})"))
            .unwrap_or_default();
        format!(
            "Last exit: clean{reason} at {}",
            fmt_epoch(newest.end_ts.unwrap_or(0))
        )
    } else if newest.exit.as_deref() == Some(crate::session_ledger::EXIT_STARTUP_FAILED) {
        let reason = newest
            .exit_reason
            .as_deref()
            .map(|r| format!(": {r}"))
            .unwrap_or_default();
        format!(
            "Last exit: FAILED TO START{reason} at {}",
            fmt_epoch(newest.end_ts.unwrap_or(0))
        )
    } else if newest.ended_with_recorded_disposition() {
        let reason = newest
            .exit_reason
            .as_deref()
            .map(|r| format!(" ({r})"))
            .unwrap_or_default();
        format!(
            "Last exit: supervised shutdown started{reason}, clean drain incomplete at {}",
            fmt_epoch(newest.end_ts.unwrap_or(0))
        )
    } else {
        let alive = newest
            .last_heartbeat_ts
            .map_or_else(|| fmt_epoch(newest.start_ts), fmt_epoch);
        format!(
            "Last exit: UNCLEAN — no clean shutdown record (crash / kill / power loss); \
             last alive {alive}"
        )
    }
}

/// Format a Unix epoch-seconds timestamp as RFC3339 UTC (`chrono` is already a
/// daemon dependency). Falls back to the raw number if it can't be represented.
fn fmt_epoch(ts: u64) -> String {
    chrono::DateTime::from_timestamp(ts as i64, 0).map_or_else(
        || ts.to_string(),
        |dt| dt.format("%Y-%m-%dT%H:%M:%SZ").to_string(),
    )
}
