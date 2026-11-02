use super::*;

#[derive(Args, Debug)]
pub(crate) struct OpsArgs {
    /// Show one compact row per app/client and request verb.
    #[arg(long)]
    pub(crate) by_app: bool,
    /// Include durable rollups for the trailing window (examples: 30m, 6h, 2d).
    #[arg(long)]
    pub(crate) since: Option<String>,
    /// Seconds to wait for the daemon's status read (default: the shared
    /// admin-scan budget, `LASTDB_UDS_ADMIN_TIMEOUT_SECS` / 600s). This is a
    /// diagnostic for an already-slow node, so it waits rather than giving up.
    #[arg(long)]
    pub(crate) timeout: Option<u64>,
}

#[derive(Args, Debug)]
pub(crate) struct AlertCheckArgs {
    /// Persisted checker state (default: <home>/monitoring/lastdbd-health-alert-state.json).
    #[arg(long)]
    pub(crate) state_file: Option<PathBuf>,
    /// Consecutive failed /health probes required before a down alert.
    #[arg(long, default_value_t = 3)]
    pub(crate) failures_before_alert: u64,
    /// Minimum seconds between repeated down alerts during one incident.
    #[arg(long, default_value_t = 900)]
    pub(crate) cooldown_secs: u64,
    /// Append notifications to this file instead of using macOS Notification Center.
    #[arg(long, conflicts_with = "no_notify")]
    pub(crate) notification_log: Option<PathBuf>,
    /// Disable notification delivery; useful only for dry-run diagnostics.
    #[arg(long)]
    pub(crate) no_notify: bool,
    /// Last Stack heartbeat helper path; when set, one routine-heartbeats line is appended.
    #[arg(long)]
    pub(crate) heartbeat_command: Option<PathBuf>,
    /// If this file exists, down alerts are suppressed for an acknowledged incident.
    #[arg(long)]
    pub(crate) acknowledged_incident_file: Option<PathBuf>,
    /// Routine name to write in heartbeat lines.
    #[arg(long, default_value = "lastdbd-mini-health-alert")]
    pub(crate) routine_name: String,
    /// Slowest-request budget in ms for the degradation probe. A slowest
    /// request at or above this is reported as a degradation.
    #[arg(long, default_value_t = 5000)]
    pub(crate) slowest_request_warn_ms: u64,
    /// Skip the `/api/status` degradation probe and report liveness only.
    /// Restores the pre-2026-10-04 behaviour, in which a node serving reads in
    /// 23 s answered `ok`.
    #[arg(long)]
    pub(crate) no_degradation_probe: bool,
}

#[derive(Subcommand, Debug)]
pub(crate) enum ServiceHomeCommand {
    /// Persist the home directory service starts should use.
    Set {
        /// Absolute path, or a path beginning with ~/; must not resolve to ~/.lastdb.
        home: PathBuf,
    },
    /// Show the persisted service home, if one is configured.
    Show,
    /// Clear the persisted service home.
    Clear,
}
