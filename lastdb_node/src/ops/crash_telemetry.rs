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
//! For a panic report: the panic message, source location, build version,
//! report timestamp, and the backtrace — code-shaped data only. The
//! observability-ring tail embedded in the local report is NOT sent; it stays
//! on the machine. For an unclean exit: version, downtime, OS shutdown cause,
//! and the tail of the previous daemon log (code-shaped operational lines).
//!
//! ## Consent / gating
//!
//! [`report_crashes_with_log_tail`] captures through the process-global Sentry
//! client, which only exists when a DSN (`OBS_SENTRY_DSN`) was configured and
//! the daemon bound a client at boot — so "no DSN ⇒ no client ⇒ no-op" is
//! enforced by construction. A dev / ephemeral node with no DSN ships nothing.

#[cfg(feature = "sentry-telemetry")]
use crate::session_ledger::StartSummary;
#[cfg(feature = "sentry-telemetry")]
use std::collections::BTreeMap;
#[cfg(feature = "sentry-telemetry")]
use std::fs;
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

/// Upper bound on the backtrace text shipped per event. Sentry rejects
/// oversized events outright; a truncated backtrace still names the frames
/// that matter (the top), a rejected event names nothing.
const BACKTRACE_CAP_BYTES: usize = 32 * 1024;

/// Upper bound on the previous-session log tail attached to an unclean-exit
/// event. The tail is the app-lifecycle log (startup phases, supervisor and
/// shutdown lines — code-shaped operational data, same posture as the panic
/// fields above; the observability ring stays local). Capped so a chatty
/// session can't push the event over Sentry's size limit.
#[cfg(feature = "sentry-telemetry")]
const LOG_TAIL_CAP_BYTES: usize = 16 * 1024;

/// Section header that opens the local-only ring-tail portion of a crash
/// report (see `observability::crash::compose_report`). Parsing stops here —
/// everything above is code-shaped, everything below is log content.
const RING_SECTION_HEADER: &str = "observability ring";

/// The code-shaped fields parsed out of an on-disk panic report.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PanicReportSummary {
    /// Panic message (`message:` line). Placeholder when the report is
    /// malformed, so a truncated file still produces a traceable event.
    pub message: String,
    /// `file:line:col` of the panic site, when recorded.
    pub location: Option<String>,
    /// Build version that crashed (`version:` line).
    pub version: Option<String>,
    /// RFC3339 timestamp of the crash (`time:` line).
    pub time: Option<String>,
    /// Backtrace section, capped at [`BACKTRACE_CAP_BYTES`].
    pub backtrace: String,
}

/// Parse the code-shaped fields out of a crash-report body, stopping before
/// the observability-ring section (which never leaves the machine).
pub fn parse_panic_report(body: &str) -> PanicReportSummary {
    // Everything we send comes from ABOVE the ring header; slicing first
    // makes "the ring tail is never shipped" structural rather than a
    // per-field promise (a panic message can't smuggle ring lines in).
    let shipped = body
        .find(RING_SECTION_HEADER)
        .map_or(body, |idx| &body[..idx]);

    let field = |prefix: &str| -> Option<String> {
        shipped.lines().find_map(|line| {
            line.strip_prefix(prefix)
                .map(str::trim)
                .filter(|v| !v.is_empty() && *v != "<unknown>")
                .map(str::to_string)
        })
    };

    let backtrace = shipped
        .split_once("backtrace\n---------\n")
        .map(|(_, rest)| rest.trim_end())
        .unwrap_or_default();
    let backtrace = truncate_utf8(backtrace, BACKTRACE_CAP_BYTES).to_string();

    PanicReportSummary {
        message: field("message:").unwrap_or_else(|| "<unparseable crash report>".to_string()),
        location: field("location:"),
        version: field("version:"),
        time: field("time:"),
        backtrace,
    }
}

/// Truncate to at most `cap` bytes on a UTF-8 boundary.
fn truncate_utf8(s: &str, cap: usize) -> &str {
    if s.len() <= cap {
        return s;
    }
    let mut end = cap;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}

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
#[cfg(feature = "sentry-telemetry")]
pub fn report_crashes(reports: &[PathBuf], start: Option<&StartSummary>) -> usize {
    report_crashes_with_log_tail(reports, start, None)
}

/// [`report_crashes`] plus the tail of the PREVIOUS session's app log, attached
/// to the unclean-exit event so "what was it doing when it died" is answerable
/// from the Sentry event alone (2026-07-10 incident: the killed session's log
/// was truncated away and its in-memory ring died with it — the event carried
/// nothing).
#[cfg(feature = "sentry-telemetry")]
pub fn report_crashes_with_log_tail(
    reports: &[PathBuf],
    start: Option<&StartSummary>,
    prev_log_tail: Option<&str>,
) -> usize {
    if sentry::Hub::current().client().is_none() {
        return 0;
    }
    capture_events(reports, start, prev_log_tail)
}

#[cfg(not(feature = "sentry-telemetry"))]
pub fn report_crashes(_reports: &[PathBuf], _start: Option<&StartSummary>) -> usize {
    0
}

#[cfg(not(feature = "sentry-telemetry"))]
pub fn report_crashes_with_log_tail(
    _reports: &[PathBuf],
    _start: Option<&StartSummary>,
    _prev_log_tail: Option<&str>,
) -> usize {
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
fn capture_events(
    reports: &[PathBuf],
    start: Option<&StartSummary>,
    prev_log_tail: Option<&str>,
) -> usize {
    let mut captured = 0;
    for path in reports.iter().take(MAX_REPORTS_PER_LAUNCH) {
        let Ok(body) = fs::read_to_string(path) else {
            continue;
        };
        sentry::capture_event(panic_event(&parse_panic_report(&body)));
        captured += 1;
    }
    if unclean_exit_unexplained(start, reports.len()) {
        // `unclean_exit_unexplained` returned true, so start is Some.
        if let Some(summary) = start {
            sentry::capture_event(unclean_exit_event(summary, prev_log_tail));
            captured += 1;
        }
    } else if startup_failure_recorded(start) {
        // `startup_failure_recorded` returned true, so start is Some.
        if let Some(summary) = start {
            sentry::capture_event(startup_failure_event(summary, prev_log_tail));
            captured += 1;
        }
    }
    captured
}

#[cfg(feature = "sentry-telemetry")]
fn panic_event(report: &PanicReportSummary) -> sentry::protocol::Event<'static> {
    let mut extra = BTreeMap::new();
    if let Some(location) = &report.location {
        extra.insert("panic_location".to_string(), location.clone().into());
    }
    if let Some(version) = &report.version {
        extra.insert("crashed_version".to_string(), version.clone().into());
    }
    if let Some(time) = &report.time {
        extra.insert("crash_time".to_string(), time.clone().into());
    }
    if !report.backtrace.is_empty() {
        extra.insert("backtrace".to_string(), report.backtrace.clone().into());
    }
    sentry::protocol::Event {
        level: sentry::Level::Fatal,
        message: Some(format!(
            "previous session crashed (panic): {}",
            report.message
        )),
        // Group by panic site when known, else by message — the timestamp in
        // the default message would otherwise make every crash its own issue.
        fingerprint: vec![
            "crash_telemetry".into(),
            report
                .location
                .clone()
                .unwrap_or_else(|| report.message.clone())
                .into(),
        ]
        .into(),
        tags: BTreeMap::from([("crash_kind".to_string(), "panic".to_string())]),
        extra,
        ..Default::default()
    }
}

/// The context both previous-session events carry: how long the node was down,
/// what the OS said about the last shutdown, and the tail of the dead session's
/// log ("what was it doing when it died").
#[cfg(feature = "sentry-telemetry")]
fn previous_session_extra(
    summary: &StartSummary,
    prev_log_tail: Option<&str>,
) -> BTreeMap<String, sentry::protocol::Value> {
    let mut extra = BTreeMap::new();
    if let Some(secs) = summary.prev_downtime_secs {
        extra.insert("downtime_secs".to_string(), secs.into());
    }
    if let Some(cause) = &summary.prev_shutdown_cause {
        extra.insert("os_shutdown_cause".to_string(), cause.clone().into());
    }
    if let Some(tail) = prev_log_tail.map(str::trim).filter(|t| !t.is_empty()) {
        // Tail-biased truncation: when over the cap, the END of the log (the
        // last things the dying session did) matters more than the start.
        let tail = if tail.len() > LOG_TAIL_CAP_BYTES {
            let mut begin = tail.len() - LOG_TAIL_CAP_BYTES;
            while begin < tail.len() && !tail.is_char_boundary(begin) {
                begin += 1;
            }
            &tail[begin..]
        } else {
            tail
        };
        extra.insert(
            "previous_session_log_tail".to_string(),
            tail.to_string().into(),
        );
    }
    extra
}

/// The previous session returned a fatal error from its own startup/serve path
/// and recorded it. Reported separately from the native-crash event so the
/// error we already know is in the event instead of a guess about crashes,
/// kills, and power loss.
#[cfg(feature = "sentry-telemetry")]
fn startup_failure_event(
    summary: &StartSummary,
    prev_log_tail: Option<&str>,
) -> sentry::protocol::Event<'static> {
    let mut extra = previous_session_extra(summary, prev_log_tail);
    if let Some(reason) = &summary.prev_exit_reason {
        extra.insert("startup_error".to_string(), reason.clone().into());
    }
    sentry::protocol::Event {
        level: sentry::Level::Error,
        // Stable message + fingerprint: a launchd restart loop on one bad store
        // is ONE issue, and the varying error text rides in `startup_error`.
        message: Some(
            "previous session failed to start (node exited with a recorded startup error; \
             not a crash)"
                .to_string(),
        ),
        fingerprint: vec!["crash_telemetry".into(), "startup_failed".into()].into(),
        tags: BTreeMap::from([("crash_kind".to_string(), "startup_failed".to_string())]),
        extra,
        ..Default::default()
    }
}

#[cfg(feature = "sentry-telemetry")]
fn unclean_exit_event(
    summary: &StartSummary,
    prev_log_tail: Option<&str>,
) -> sentry::protocol::Event<'static> {
    let extra = previous_session_extra(summary, prev_log_tail);
    sentry::protocol::Event {
        level: sentry::Level::Error,
        message: Some(
            "previous session ended uncleanly (no clean shutdown record; native crash, \
             kill, or power loss — no panic report found)"
                .to_string(),
        ),
        fingerprint: vec!["crash_telemetry".into(), "unclean_exit".into()].into(),
        tags: BTreeMap::from([("crash_kind".to_string(), "unclean_exit".to_string())]),
        extra,
        ..Default::default()
    }
}
