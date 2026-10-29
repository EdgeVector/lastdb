//! Ship crash evidence to Sentry on the launch AFTER a crash.
//!
//! Ported from `fold_db_node::crash_telemetry` into the `lastdbd` Mini daemon
//! (the fold_db_node desktop node was deleted in the 2026-07-12 Mini-only
//! cutover). The daemon-relevant surface is kept; the desktop-only explicit
//! per-crash consent path (`send_crashes_one_shot`, which reached into the
//! desktop's `telemetry_consent`/`install_id`) was dropped — a headless daemon
//! has no per-crash "send this report?" prompt.
//!
//! The node leaves two kinds of durable, local-only crash evidence:
//!
//! - `observability::crash` writes a `<home>/crash-reports/<ts>.txt` report on
//!   every Rust panic (message, location, backtrace, ring tail);
//! - [`crate::session_ledger`] records whether the *previous* session ended
//!   cleanly (a native crash — SIGSEGV/abort/kill/power-loss — leaves no panic
//!   report, but it does leave a ledger row with no clean-shutdown stamp).
//!
//! Neither ever left the machine, so a crash on a user's box was invisible.
//! This module closes that gap: at startup, once the Sentry client is
//! (DSN-gated) bound, it promotes the evidence into Sentry events.
//!
//! ## What is sent — deliberately minimal
//!
//! Events contain a fixed fault category and severity. The shared client's
//! final allowlist adds only the numeric package version, standard environment,
//! platform, current timestamp, and a fresh event ID. Panic text, source paths,
//! backtraces, startup errors, and log tails stay on the machine.
//!
//! ## Consent / gating
//!
//! [`report_crashes_with_log_tail`] captures through the process-global Sentry
//! client, which only exists when a DSN (`OBS_SENTRY_DSN`) was configured and
//! the daemon bound a client at boot — so "no DSN ⇒ no client ⇒ no-op" is
//! enforced by construction. A dev / ephemeral node with no DSN ships nothing.

use crate::session_ledger::StartSummary;
#[cfg(feature = "sentry-telemetry")]
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// Sibling marker written next to a report once it has been shipped at crash
/// time, so the next-launch scan neither re-sends nor re-offers it. Kept as a
/// separate file (rather than deleting/renaming the report) so `folddb
/// crash-reports` can still show the human-readable `.txt` locally.
const SENT_MARKER_SUFFIX: &str = ".sent";

/// Upper bound on panic reports promoted per launch. `scan_and_warn` already
/// caps retention at 20; a crash-looping binary should not turn a backlog
/// into an event flood on its first healthy start.
#[cfg(feature = "sentry-telemetry")]
const MAX_REPORTS_PER_LAUNCH: usize = 5;

/// Whether the previous session's ledger row indicates a NATIVE crash — an
/// unclean end with no panic report to explain it. Panic reports take
/// precedence: when one exists, the unclean ledger row is the same incident,
/// and one event per crash beats two.
pub fn unclean_exit_unexplained(start: Option<&StartSummary>, panic_report_count: usize) -> bool {
    panic_report_count == 0
        && start.is_some_and(|s| s.had_previous && !s.prev_session_exit_recorded)
}

/// Whether the previous session exited because its OWN startup/serve path
/// returned a fatal error and stamped the ledger for it.
///
/// This is the opposite of [`unclean_exit_unexplained`]: the process knew why
/// it was dying and said so. It still deserves an event — a node that cannot
/// open its store is a real failure — but it must be reported as the recorded
/// error it is, not as an unexplained native crash.
pub fn startup_failure_recorded(start: Option<&StartSummary>) -> bool {
    start.is_some_and(|s| {
        s.had_previous && s.prev_exit.as_deref() == Some(crate::session_ledger::EXIT_STARTUP_FAILED)
    })
}

/// Promote crash evidence into Sentry events through the process-global
/// client. No-ops (returns 0) when no client is bound — i.e. when consent or
/// a missing DSN kept the sink off. Returns the number of events captured.
///
/// `reports` is expected newest-first (the order `scan_and_warn` returns);
/// only the newest [`MAX_REPORTS_PER_LAUNCH`] are promoted.
/// The previous session's log tail stays local.
#[cfg(feature = "sentry-telemetry")]
pub fn report_crashes_with_log_tail(reports: &[PathBuf], start: Option<&StartSummary>) -> usize {
    if sentry::Hub::current().client().is_none() {
        return 0;
    }
    capture_events(reports, start)
}

#[cfg(not(feature = "sentry-telemetry"))]
pub fn report_crashes_with_log_tail(_reports: &[PathBuf], _start: Option<&StartSummary>) -> usize {
    0
}

/// The sent-marker path for a crash report (`<report>.txt.sent`).
fn sent_marker_path(report: &Path) -> PathBuf {
    let mut name = report.as_os_str().to_os_string();
    name.push(SENT_MARKER_SUFFIX);
    PathBuf::from(name)
}

/// Whether a report was already shipped at crash time (its sent-marker exists).
/// The next-launch path filters these out so a crash reported in-the-moment is
/// not reported a second time.
pub fn is_report_sent(report: &Path) -> bool {
    sent_marker_path(report).exists()
}

/// Build + capture the events for the given evidence. Caller has ensured a
/// client is bound.
#[cfg(feature = "sentry-telemetry")]
fn capture_events(reports: &[PathBuf], start: Option<&StartSummary>) -> usize {
    let mut captured = 0;
    for path in reports.iter().take(MAX_REPORTS_PER_LAUNCH) {
        if !path.is_file() {
            continue;
        }
        sentry::capture_event(panic_event());
        captured += 1;
    }
    if unclean_exit_unexplained(start, reports.len()) {
        sentry::capture_event(unclean_exit_event());
        captured += 1;
    } else if startup_failure_recorded(start) {
        sentry::capture_event(startup_failure_event());
        captured += 1;
    }
    captured
}

#[cfg(feature = "sentry-telemetry")]
fn panic_event() -> sentry::protocol::Event<'static> {
    sentry::protocol::Event {
        level: sentry::Level::Fatal,
        message: Some("previous session crashed (panic)".to_string()),
        fingerprint: vec!["crash_telemetry".into(), "panic".into()].into(),
        tags: BTreeMap::from([("crash_kind".to_string(), "panic".to_string())]),
        ..Default::default()
    }
}

/// The previous session returned a fatal error from its own startup/serve path
/// and recorded it. Reported separately from the native-crash event so the
/// error we already know is in the event instead of a guess about crashes,
/// kills, and power loss.
#[cfg(feature = "sentry-telemetry")]
fn startup_failure_event() -> sentry::protocol::Event<'static> {
    sentry::protocol::Event {
        level: sentry::Level::Error,
        message: Some(
            "previous session failed to start (node exited with a recorded startup error; \
             not a crash)"
                .to_string(),
        ),
        fingerprint: vec!["crash_telemetry".into(), "startup_failed".into()].into(),
        tags: BTreeMap::from([("crash_kind".to_string(), "startup_failed".to_string())]),
        ..Default::default()
    }
}

#[cfg(feature = "sentry-telemetry")]
fn unclean_exit_event() -> sentry::protocol::Event<'static> {
    sentry::protocol::Event {
        level: sentry::Level::Error,
        message: Some(
            "previous session ended uncleanly (no clean shutdown record; native crash, \
             kill, or power loss — no panic report found)"
                .to_string(),
        ),
        fingerprint: vec!["crash_telemetry".into(), "unclean_exit".into()].into(),
        tags: BTreeMap::from([("crash_kind".to_string(), "unclean_exit".to_string())]),
        ..Default::default()
    }
}
