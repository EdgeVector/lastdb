//! Text report of `lastdb status`, read from the home offline plus the daemon probe.

use super::*;

/// What the text report of `lastdb status` reads from the home, offline.
pub(super) struct StatusView<'a> {
    pub(super) home: &'a Path,
    pub(super) socket: &'a Path,
    pub(super) store_root: &'a Path,
    pub(super) session_live: bool,
}

impl StatusView<'_> {
    /// Report for a socket that did not answer `/health`: file-only facts.
    pub(super) fn print_unreachable(&self, reason: &str) {
        println!("{}", status_unreachable_line(reason));
        println!("Home:   {}", self.home.display());
        println!("Socket: {}", self.socket.display());
        // File-only session report stays after line 1 so operators
        // still see the home without treating the command as a live probe.
        self.print_offline_lines();
    }

    /// Report for a serving daemon: offline lines, then the daemon's own account.
    pub(super) fn print_serving(
        &self,
        running: bool,
        health_err: Option<&str>,
        probe_timeout: Duration,
    ) {
        println!(
            "lastdbd: {}",
            lastdb_node::crash_attribution::daemon_status_line_with_health_error(
                self.home, running, health_err
            )
        );
        println!("Home:   {}", self.home.display());
        println!("Socket: {}", self.socket.display());
        self.print_offline_lines();
        self.print_daemon_account(probe_timeout);
    }

    /// Session ledger and placement.
    /// Read straight from the store under `home`; no node required.
    fn print_offline_lines(&self) {
        for line in lastdb_node::crash_attribution::status_lines(self.home, self.session_live) {
            println!("{line}");
        }
        // A home left on full-key placement pays a full-collection sweep on every
        // HashRange partition read, and nothing else in `status` would show it.
        for line in layout_status_lines(self.store_root) {
            println!("{line}");
        }
    }

    /// Everything the daemon reports about itself, build identity first.
    fn print_daemon_account(&self, probe_timeout: Duration) {
        match probe_status(self.socket, probe_timeout) {
            Ok(snapshot) => {
                for line in lastdb_node::self_metrics::build_identity_lines(&snapshot.build) {
                    println!("{line}");
                }
                for line in lastdb_node::self_metrics::status_lines(&snapshot) {
                    println!("{line}");
                }
                for line in lastdb_node::self_metrics::phase_vocabulary_skew_lines(&snapshot.build)
                {
                    println!("{line}");
                }
                // Compact offender summary so `lastdb status` alone surfaces the top clients.
                for line in lastdb_node::self_metrics::request_ops_lines(&snapshot)
                    .into_iter()
                    .take(6)
                {
                    println!("{line}");
                }
            }
            Err(e) => println!("Sampler: unavailable ({e})"),
        }
    }
}
