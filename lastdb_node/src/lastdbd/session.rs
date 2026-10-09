//! Session ledger: start record, crash-evidence promotion, startup failures.

use std::path::Path;

use lastdb_node::session_ledger::Ledger;

/// Promote previous-session crash evidence to Sentry: surfaced panic reports
/// plus a single unclean_exit event (carrying the previous daemon log tail)
/// when the prior session died with no panic report to explain it. No-op
/// without a bound client.
fn promote_previous_crash(
    home: &Path,
    summary: Option<&lastdb_node::session_ledger::StartSummary>,
) {
    let reports = lastdb_node::crash_attribution::scan_previous_crashes(home);
    let tail = lastdb_node::crash_attribution::previous_log_tail();
    lastdb_node::crash_attribution::promote_previous_crash_evidence(
        &reports,
        summary,
        tail.as_deref(),
    );
}

/// Record this session's start (reading the prior session for clean/unclean +
/// downtime + OS shutdown cause). Best-effort; never blocks serving.
pub(crate) fn record_start(home: &Path, pid: u32) -> Option<Ledger> {
    match Ledger::record_start(home, pid, 0, None) {
        Ok((ledger, summary)) => {
            if let Some(line) = summary.log_line() {
                if summary.prev_session_clean {
                    tracing::info!(target: "lastdbd::session_ledger", "{line}");
                } else {
                    tracing::warn!(target: "lastdbd::session_ledger", "{line}");
                }
            }
            promote_previous_crash(home, Some(&summary));
            Some(ledger)
        }
        Err(e) => {
            tracing::warn!(
                target: "lastdbd::session_ledger",
                error = %e,
                "couldn't record session start; uptime ledger disabled for this run"
            );
            // Still surface panic reports even if the ledger write failed.
            promote_previous_crash(home, None);
            None
        }
    }
}

/// Every fatal error after the ledger start exits the process through
/// `main`'s `Err` return. That is a CONTROLLED exit, but it leaves the
/// session ledger line open, which the next boot reads as "no clean shutdown
/// record" => a phantom native crash. Stamp the real reason on the way out.
pub(crate) fn note_fatal(ledger_present: bool, home: &Path, pid: u32, err: String) -> String {
    if ledger_present {
        if let Err(e) = lastdb_node::session_ledger::mark_startup_failed(home, pid, Some(&err)) {
            tracing::warn!(
                target: "lastdbd::session_ledger",
                error = %e,
                "couldn't record the startup failure in the session ledger"
            );
        }
    }
    err
}
