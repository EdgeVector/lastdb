//! Read-only Mini health checks through the owner socket.
//! Liveness, memory pressure, integrity faults and blocked V2 work stay distinct.
//! Unknown V2 freshness is never reported as healthy or as a missing cloud backup.
//! The checker saves its own transition state outside the database.

use std::fs;
use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

mod governor_arm;
mod reasons;

#[derive(Clone, Debug)]
pub struct Config {
    pub socket: PathBuf,
    pub state_file: PathBuf,
    pub failures_before_alert: u64,
    pub cooldown: Duration,
    pub notification: Notification,
    pub heartbeat_command: Option<PathBuf>,
    pub acknowledged_incident_file: Option<PathBuf>,
    pub routine_name: String,
    pub now: SystemTime,
    /// Slowest-request budget in ms. `Some(n)` enables the `/api/status`
    /// degradation probe and flags a slowest request at or above `n`; `None`
    /// disables the probe entirely and restores liveness-only behaviour.
    pub slowest_request_warn_ms: Option<u64>,
}

#[derive(Clone, Debug)]
pub enum Notification {
    MacOs,
    LogFile(PathBuf),
    Disabled,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    Healthy,
    /// Reachable, and the node reports at least one degradation of its own.
    /// `reasons` are the node's words, never this checker's inference.
    Degraded {
        reasons: Vec<String>,
    },
    /// Reachable, and the degradation probe could not be completed. Distinct
    /// from [`Self::Healthy`] on purpose: a status page that will not answer is
    /// evidence about the node, not an absence of evidence.
    DegradationUnknown {
        why: String,
    },
    PendingFailure {
        consecutive_failures: u64,
    },
    DownAlert {
        consecutive_failures: u64,
    },
    DownSuppressed {
        consecutive_failures: u64,
    },
    RecoveryAlert,
}

/// What the `/api/status` probe found. Three states, because "could not look"
/// must not collapse into "nothing to see".
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Degradation {
    Clean,
    Degraded(Vec<String>),
    Unknown(String),
}

impl Outcome {
    pub fn heartbeat_level(&self) -> &'static str {
        match self {
            Self::DownAlert { .. } => "error",
            Self::Degraded { .. } | Self::DegradationUnknown { .. } => "warn",
            _ => "ok",
        }
    }

    pub fn summary(&self) -> String {
        match self {
            Self::Healthy => "reachable".to_string(),
            Self::Degraded { reasons } => {
                format!("degraded reachable; {}", reasons.join("; "))
            }
            Self::DegradationUnknown { why } => {
                format!("reachable; degradation unknown — {why}")
            }
            Self::PendingFailure {
                consecutive_failures,
            } => format!("pending-failure consecutive_failures={consecutive_failures}"),
            Self::DownAlert {
                consecutive_failures,
            } => format!("alert-down consecutive_failures={consecutive_failures}"),
            Self::DownSuppressed {
                consecutive_failures,
            } => format!("down-cooldown consecutive_failures={consecutive_failures}"),
            Self::RecoveryAlert => "recovery-all-clear".to_string(),
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
struct State {
    version: u64,
    consecutive_failures: u64,
    observed_down: bool,
    alerted_down: bool,
    last_alert_at_epoch: Option<u64>,
    last_probe_at_epoch: Option<u64>,
    last_error: Option<String>,
}

pub fn default_state_file(home: &Path) -> PathBuf {
    home.join("monitoring")
        .join("lastdbd-health-alert-state.json")
}

pub fn run_once(config: &Config) -> Result<Outcome, String> {
    let health = probe_health(&config.socket);
    let mut state = read_state(&config.state_file)?;
    let mut outcome = evaluate(
        &mut state,
        health.as_ref().map(|_| ()).map_err(String::as_str),
        config.failures_before_alert,
        config.cooldown.as_secs(),
        epoch_secs(config.now)?,
    );

    if matches!(outcome, Outcome::DownAlert { .. }) && incident_acknowledged(config) {
        outcome = Outcome::DownSuppressed {
            consecutive_failures: state.consecutive_failures,
        };
    }

    // The liveness verdict above owns the down path, its failure counter and its
    // cooldown; the degradation probe only refines a would-be Healthy. A node
    // that is DOWN is not also "degraded", and RecoveryAlert must keep firing on
    // the tick the node comes back.
    if matches!(outcome, Outcome::Healthy) {
        if let Some(warn_ms) = config.slowest_request_warn_ms {
            outcome = match probe_degradation(&config.socket, warn_ms) {
                Degradation::Clean => Outcome::Healthy,
                Degradation::Degraded(reasons) => Outcome::Degraded { reasons },
                Degradation::Unknown(why) => Outcome::DegradationUnknown { why },
            };
        }
    }

    if matches!(outcome, Outcome::DownAlert { .. } | Outcome::RecoveryAlert) {
        deliver_notification(config, &outcome, health.err().as_deref())?;
    }

    write_state(&config.state_file, &state)?;
    append_heartbeat(config, &outcome)?;
    Ok(outcome)
}

mod probe;
pub use probe::*;
