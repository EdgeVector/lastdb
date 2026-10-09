//! One-shot health checker for proactive `lastdbd` Mini down alerts.
//!
//! The checker is intentionally read-only against the brain: it probes the owner
//! socket `/health` and `/api/status`, persists its own transition state outside
//! the DB, and optionally calls the Last Stack heartbeat helper so routine fleet
//! monitoring can notice if the checker itself stops running. Neither probe
//! reads a DB record, so "read-only against the brain" still holds.
//!
//! **Why there are two probes.** `/health` is deliberately answerable without
//! node state, so it stays truthful when the internals are wedged — that is
//! exactly why it carries no health fields. Measured 2026-10-04 on the primary:
//! `alert-check` answered `ok` in 0.08 s while a real read took 23.5 s, because
//! liveness was its only input. Meanwhile the node already computed
//! `memory_budget.runtime_degraded`, `memory_budget.slowest_request_ms` (17826)
//! and `durability.degraded=true` with `reasons=["backup_age_over_threshold"]`
//! — a backup 3.5 days old against a 24 h threshold. The evidence was one GET
//! away and nothing read it
//! (papercut-lastdb-alert-check-reports-ok-on-a-node-serving-reads-in-23s-because-it-only-probes-liveness-20261004).
//!
//! `/api/status` is the node's own cheap health path (process vitals, sync,
//! backup, QoS, scalars only — no forensic ring, no walks). Measured at 10-18 ms
//! for 37 KB, so the check stays cheaper than what it checks.
//!
//! **What the degradation probe READS, which is not the same as what the
//! payload carries.** The node publishes five independent "I am degraded"
//! verdicts and [`probe_degradation`] consumes all five:
//! `memory_budget` (`runtime_degraded`, `footprint_over_limit`,
//! `deferred_lane_refuse_entries`, `slowest_request_ms` against the
//! caller's budget, and a stale `governor_state` — see `governor_arm`),
//! `durability.degraded`, `sync.sync_degraded`, and
//! `integrity.unresolved_atom_skips`. The first shipped round read only the
//! first two: the sentence above named `sync` as part of the payload and read
//! as a statement about this checker, while nothing here touched it, so a
//! 16.1 h mutation-log recovery point and 96 reads already served short were
//! both invisible to the alarm and both printed on `lastdb status`
//! (measured on the primary 2026-10-05T14:4xZ). Keep this list and the arms in
//! [`probe_degradation`] in step; `tests::every_node_degradation_verdict_is_read`
//! fails if a verdict goes unread.
//!
//! **A failed degradation probe is never `ok`.** Absent and healthy are
//! different answers: if `/api/status` cannot be read or parsed, the verdict is
//! [`Outcome::DegradationUnknown`], not [`Outcome::Healthy`]. A node too busy to
//! answer its own status page is the case this checker exists to catch.

use std::fs;
use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

mod governor_arm;
use governor_arm::stale_purge_failed_reason;

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

    let mut reasons = Vec::new();

    let mem = status.get("memory_budget");
    if mem.is_none() {
        return Degradation::Unknown("status payload had no `memory_budget`".to_string());
    }
    if let Some(mem) = mem {
        if mem
            .get("runtime_degraded")
            .and_then(serde_json::Value::as_bool)
            == Some(true)
        {
            reasons.push("memory_budget.runtime_degraded=true".to_string());
        }
        if mem
            .get("footprint_over_limit")
            .and_then(serde_json::Value::as_bool)
            == Some(true)
        {
            reasons.push("memory_budget.footprint_over_limit=true".to_string());
        }
        if let Some(refused) = mem
            .get("deferred_lane_refuse_entries")
            .and_then(serde_json::Value::as_u64)
        {
            if refused > 0 {
                reasons.push(format!("deferred_lane_refuse_entries={refused}"));
            }
        }
        if let Some(slowest) = mem
            .get("slowest_request_ms")
            .and_then(serde_json::Value::as_u64)
        {
            if slowest >= warn_ms {
                reasons.push(format!("slowest_request_ms={slowest} (budget {warn_ms})"));
            }
        }
        if let Some(reason) = stale_purge_failed_reason(mem) {
            reasons.push(reason);
        }
    }

    // Durability carries its own verdict AND its own reason list. Pass them
    // through rather than restating them: a 3.5-day-old backup against a 24 h
    // threshold is the node's finding, not this checker's.
    if let Some(dur) = status.get("durability") {
        if dur.get("degraded").and_then(serde_json::Value::as_bool) == Some(true) {
            let why = dur
                .get("reasons")
                .and_then(|v| v.as_array())
                .map(|rs| {
                    rs.iter()
                        .filter_map(|r| r.as_str())
                        .collect::<Vec<_>>()
                        .join(",")
                })
                .filter(|j| !j.is_empty())
                .unwrap_or_else(|| "no reason given".to_string());
            reasons.push(format!("durability.degraded=true ({why})"));
        }
    }

    // Sync carries the node's OWN aggregated verdict plus its own reason list,
    // exactly like durability, and it is a DIFFERENT exposure: durability asks
    // "how old is the last committed backup manifest", sync asks "has the
    // mutation log published recently". The two are independent paths on
    // independent schedules, so a home with a fresh backup and a mutation log
    // that has not published for hours reads `durability.degraded=false` and
    // was reported healthy here. Measured on the primary 2026-10-05T14:4xZ:
    // `sync_degraded=true`, `degraded_reasons=["mutation_log_lag"]`,
    // `mutation_log_recovery_point_age_secs=58021` — a 16.1 h recovery point
    // that `lastdb status` printed on its `Sync:` line while this probe,
    // reading the same payload, said nothing about it.
    //
    // `sync_degraded` is false when sync is switched OFF (the case
    // `crate::backup_durability` exists to cover), so passing it through does
    // not fire on a deliberately disabled engine.
    if let Some(sync) = status.get("sync") {
        if sync
            .get("sync_degraded")
            .and_then(serde_json::Value::as_bool)
            == Some(true)
        {
            let why = sync
                .get("degraded_reasons")
                .and_then(|v| v.as_array())
                .map(|rs| {
                    rs.iter()
                        .filter_map(|r| r.as_str())
                        .collect::<Vec<_>>()
                        .join(",")
                })
                .filter(|j| !j.is_empty())
                .unwrap_or_else(|| "no reason given".to_string());
            // The recovery point is the number an operator acts on, and it is
            // the node's own field, so it rides along as context rather than
            // as a second judgement by this checker.
            let rpo = sync
                .get("mutation_log_recovery_point_age_secs")
                .and_then(serde_json::Value::as_u64)
                .map(|secs| format!("; rpo_secs={secs}"))
                .unwrap_or_default();
            reasons.push(format!("sync.sync_degraded=true ({why}){rpo}"));
        }
    }

    // Read integrity means callers were already served SHORT: a tip pointed at
    // a missing atom, so an affected partition returns 200 with fewer rows than
    // its index claims. This mirrors `integrity_status_line`'s predicate in
    // `self_metrics` exactly -- DEGRADED iff a MEASURED skip count is non-zero
    // -- so the alarm and the `lastdb status` line cannot disagree. An absent
    // or unmeasurable count adds no reason here; whole-payload absence is
    // already `Degradation::Unknown` above.
    if let Some(integrity) = status.get("integrity") {
        if let Some(skips) = integrity
            .get("unresolved_atom_skips")
            .and_then(serde_json::Value::as_u64)
        {
            if skips > 0 {
                let rows = integrity
                    .get("unresolved_atom_rows")
                    .and_then(serde_json::Value::as_u64)
                    .map_or_else(|| "unknown".to_string(), |r| r.to_string());
                reasons.push(format!(
                    "integrity.unresolved_atom_rows={rows} ({skips} read(s) served short)"
                ));
            }
        }
    }

    if reasons.is_empty() {
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
