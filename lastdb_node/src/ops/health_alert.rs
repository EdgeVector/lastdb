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

/// Read the node's own degradation signals off `/api/status`.
///
/// Reports only what the node already computed — `reasons` are its words. The
/// slowest-request arm is the one judgement this checker makes on a caller
/// budget (`warn_ms`); see `governor_arm` for the other.
///
/// Any failure to connect, read, or parse is [`Degradation::Unknown`], never
/// [`Degradation::Clean`]: the common cause of a status page that will not
/// answer is the very overload this probe exists to report.
pub fn probe_degradation(socket: &Path, warn_ms: u64) -> Degradation {
    let body = match fetch_status_body(socket) {
        Ok(body) => body,
        Err(e) => return Degradation::Unknown(e),
    };
    let value: serde_json::Value = match serde_json::from_str(&body) {
        Ok(v) => v,
        Err(e) => return Degradation::Unknown(format!("invalid status JSON: {e}")),
    };
    let Some(status) = value.get("status") else {
        return Degradation::Unknown("status response had no `status` object".to_string());
    };

    let Some(mem) = status.get("memory_budget") else {
        return Degradation::Unknown("status payload had no `memory_budget`".to_string());
    };
    let mut reasons = reasons::memory_budget_reasons(mem, warn_ms);
    reasons.extend(reasons::durability_reason(status));
    reasons.extend(reasons::integrity_reason(status));

    if reasons.is_empty()
        && status
            .get("durability")
            .and_then(|health| health.get("degraded"))
            .is_none_or(serde_json::Value::is_null)
    {
        Degradation::Unknown(
            "V2 backup freshness is not measured; check the selected coordinator".into(),
        )
    } else if reasons.is_empty() {
        Degradation::Clean
    } else {
        Degradation::Degraded(reasons)
    }
}

/// GET `/api/status` on the owner socket with the same short timeouts the
/// liveness probe uses.
fn fetch_status_body(socket: &Path) -> Result<String, String> {
    let mut stream = UnixStream::connect(socket)
        .map_err(|e| format!("failed to connect to daemon socket: {e}"))?;
    stream
        .set_read_timeout(Some(Duration::from_secs(3)))
        .map_err(|e| format!("failed to set socket read timeout: {e}"))?;
    stream
        .set_write_timeout(Some(Duration::from_secs(3)))
        .map_err(|e| format!("failed to set socket write timeout: {e}"))?;
    stream
        .write_all(
            b"GET /api/status HTTP/1.1\r\nHost: localhost\r\nX-LastDB-Client: lastdb-health-alert\r\nConnection: close\r\n\r\n",
        )
        .map_err(|e| format!("failed to write status request: {e}"))?;
    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .map_err(|e| format!("failed to read status response: {e}"))?;
    if !response.starts_with("HTTP/1.1 200 ") {
        let line = response.lines().next().unwrap_or("<empty response>");
        return Err(format!("status probe returned {line}"));
    }
    response
        .split_once("\r\n\r\n")
        .map(|(_, body)| body.to_string())
        .ok_or_else(|| "status response had no body".to_string())
}

pub fn probe_health(socket: &Path) -> Result<(), String> {
    let mut stream = UnixStream::connect(socket)
        .map_err(|e| format!("failed to connect to daemon socket: {e}"))?;
    stream
        .set_read_timeout(Some(Duration::from_secs(3)))
        .map_err(|e| format!("failed to set socket read timeout: {e}"))?;
    stream
        .set_write_timeout(Some(Duration::from_secs(3)))
        .map_err(|e| format!("failed to set socket write timeout: {e}"))?;
    stream
        .write_all(b"GET /health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .map_err(|e| format!("failed to write health request: {e}"))?;

    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .map_err(|e| format!("failed to read health response: {e}"))?;
    if response.starts_with("HTTP/1.1 200 ") {
        Ok(())
    } else {
        let status = response.lines().next().unwrap_or("<empty response>");
        Err(format!("health probe returned {status}"))
    }
}

fn evaluate(
    state: &mut State,
    health: Result<(), &str>,
    failures_before_alert: u64,
    cooldown_secs: u64,
    now_secs: u64,
) -> Outcome {
    state.version = 1;
    state.last_probe_at_epoch = Some(now_secs);
    let threshold = failures_before_alert.max(1);

    match health {
        Ok(()) => {
            let should_alert_recovery = state.observed_down || state.alerted_down;
            state.consecutive_failures = 0;
            state.observed_down = false;
            state.alerted_down = false;
            state.last_error = None;
            if should_alert_recovery {
                Outcome::RecoveryAlert
            } else {
                Outcome::Healthy
            }
        }
        Err(error) => {
            state.consecutive_failures = state.consecutive_failures.saturating_add(1);
            state.observed_down = true;
            state.last_error = Some(error.to_string());

            if state.consecutive_failures < threshold {
                return Outcome::PendingFailure {
                    consecutive_failures: state.consecutive_failures,
                };
            }

            let cooldown_elapsed = state
                .last_alert_at_epoch
                .is_none_or(|last| now_secs.saturating_sub(last) >= cooldown_secs);
            if !state.alerted_down || cooldown_elapsed {
                state.alerted_down = true;
                state.last_alert_at_epoch = Some(now_secs);
                Outcome::DownAlert {
                    consecutive_failures: state.consecutive_failures,
                }
            } else {
                Outcome::DownSuppressed {
                    consecutive_failures: state.consecutive_failures,
                }
            }
        }
    }
}

fn deliver_notification(
    config: &Config,
    outcome: &Outcome,
    error: Option<&str>,
) -> Result<(), String> {
    let (title, body) = match outcome {
        Outcome::DownAlert {
            consecutive_failures,
        } => (
            "LastDB Mini health alert".to_string(),
            format!(
                "lastdbd is not reachable after {consecutive_failures} consecutive checks. Socket: {}. {}",
                config.socket.display(),
                error.unwrap_or("No probe error captured.")
            ),
        ),
        Outcome::RecoveryAlert => (
            "LastDB Mini recovered".to_string(),
            format!("lastdbd /health is reachable again. Socket: {}", config.socket.display()),
        ),
        _ => return Ok(()),
    };

    match &config.notification {
        Notification::MacOs => deliver_macos_notification(&title, &body),
        Notification::LogFile(path) => {
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent)
                    .map_err(|e| format!("failed to create {}: {e}", parent.display()))?;
            }
            let line = format!("{}\t{}\t{}\n", iso_time(config.now), title, body);
            fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
                .and_then(|mut f| f.write_all(line.as_bytes()))
                .map_err(|e| format!("failed to append notification log {}: {e}", path.display()))
        }
        Notification::Disabled => Ok(()),
    }
}

fn deliver_macos_notification(title: &str, body: &str) -> Result<(), String> {
    let script = format!(
        "display notification \"{}\" with title \"{}\"",
        applescript_escape(body),
        applescript_escape(title)
    );
    let output = Command::new("osascript")
        .arg("-e")
        .arg(script)
        .output()
        .map_err(|e| format!("failed to run osascript notification: {e}"))?;
    if output.status.success() {
        Ok(())
    } else {
        Err(format!(
            "osascript notification failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ))
    }
}

fn append_heartbeat(config: &Config, outcome: &Outcome) -> Result<(), String> {
    let Some(command) = &config.heartbeat_command else {
        return Ok(());
    };
    let line = format!(
        "{} {} {} {}",
        config.routine_name,
        iso_time(config.now),
        outcome.heartbeat_level(),
        outcome.summary()
    );
    let output = Command::new(command)
        .arg("--line")
        .arg(&line)
        .output()
        .map_err(|e| format!("failed to run heartbeat helper {}: {e}", command.display()))?;
    if output.status.success() {
        Ok(())
    } else {
        Err(format!(
            "heartbeat helper failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ))
    }
}

fn incident_acknowledged(config: &Config) -> bool {
    config
        .acknowledged_incident_file
        .as_ref()
        .is_some_and(|path| path.exists())
}

fn read_state(path: &Path) -> Result<State, String> {
    if !path.exists() {
        return Ok(State::default());
    }
    let raw = fs::read(path).map_err(|e| format!("failed to read {}: {e}", path.display()))?;
    serde_json::from_slice(&raw)
        .map_err(|e| format!("{} is not valid alert state: {e}", path.display()))
}

fn write_state(path: &Path, state: &State) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .map_err(|e| format!("failed to create {}: {e}", parent.display()))?;
    }
    let raw = serde_json::to_vec_pretty(state)
        .map_err(|e| format!("failed to encode alert state: {e}"))?;
    let tmp = path.with_extension("json.tmp");
    fs::write(&tmp, raw).map_err(|e| format!("failed to write {}: {e}", tmp.display()))?;
    fs::rename(&tmp, path).map_err(|e| format!("failed to replace {}: {e}", path.display()))
}

fn epoch_secs(now: SystemTime) -> Result<u64, String> {
    now.duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .map_err(|e| format!("system time is before unix epoch: {e}"))
}

fn iso_time(now: SystemTime) -> String {
    let epoch = epoch_secs(now).unwrap_or(0);
    chrono::DateTime::<chrono::Utc>::from_timestamp(epoch as i64, 0)
        .unwrap_or(chrono::DateTime::<chrono::Utc>::UNIX_EPOCH)
        .to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

fn applescript_escape(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}
