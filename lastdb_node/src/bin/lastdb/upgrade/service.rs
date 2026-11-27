//! The service backends: a launchd LaunchAgent on macOS and a systemd user unit on
//! Linux. The files are contract section 11.

use std::path::{Path, PathBuf};
use std::time::Duration;

use super::config::valid_label;
use super::procs;
pub(super) use super::service_jobfile::{
    backup_definition, restore_definition, rewrite_definition,
};

const PLIST_BUDDY: &str = "/usr/libexec/PlistBuddy";
const TOOL_LIMIT: Duration = Duration::from_secs(60);

pub(super) fn is_mac() -> bool {
    cfg!(target_os = "macos")
}

fn user_home() -> Result<PathBuf, String> {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .ok_or_else(|| "HOME is required".to_string())
}

fn domain() -> String {
    format!("gui/{}", rustix::process::getuid().as_raw())
}

pub(super) fn plist_path(label: &str) -> Result<PathBuf, String> {
    Ok(user_home()?
        .join("Library/LaunchAgents")
        .join(format!("{label}.plist")))
}

/// A systemd unit name always ends in `.service` here.
pub(super) fn unit_name(label: &str) -> String {
    if label.ends_with(".service") {
        label.to_owned()
    } else {
        format!("{label}.service")
    }
}

pub(super) fn unit_path(label: &str) -> Result<PathBuf, String> {
    Ok(user_home()?
        .join(".config/systemd/user")
        .join(unit_name(label)))
}

/// The file that defines the job.
pub(super) fn definition_path(label: &str) -> Result<PathBuf, String> {
    if is_mac() {
        plist_path(label)
    } else {
        unit_path(label)
    }
}

pub(super) fn ctl(program: &str, args: &[&str]) -> Result<procs::Captured, String> {
    procs::run(Path::new(program), args, TOOL_LIMIT)
}

/// The pid of the service's process, `None` when the job is not running.
pub(super) fn service_pid(label: &str) -> Result<Option<u32>, String> {
    if is_mac() {
        let target = format!("{}/{label}", domain());
        let out = ctl("launchctl", &["print", &target])?;
        if !out.ok {
            return Ok(None);
        }
        let text = out.stdout_text();
        return Ok(text
            .lines()
            .filter_map(|l| l.trim().strip_prefix("pid = "))
            .find_map(|rest| rest.trim().parse::<u32>().ok()));
    }
    let unit = unit_name(label);
    let out = ctl(
        "systemctl",
        &["--user", "show", "-p", "MainPID", "--value", &unit],
    )?;
    Ok(out
        .stdout_text()
        .trim()
        .parse::<u32>()
        .ok()
        .filter(|pid| *pid != 0))
}

/// Whether launchd (or systemd) knows the job.
pub(super) fn is_loaded(label: &str) -> Result<bool, String> {
    if is_mac() {
        let target = format!("{}/{label}", domain());
        return Ok(ctl("launchctl", &["print", &target])?.ok);
    }
    let unit = unit_name(label);
    let out = ctl("systemctl", &["--user", "is-active", &unit])?;
    Ok(out.stdout_text().trim() == "active")
}

/// The time a stop may take beyond the service manager's own stop window.
const STOP_MARGIN_SECS: u64 = 120;

/// How long the service manager lets the daemon exit after a stop before it would end it
/// by force: `ExitTimeOut` on macOS (launchd's default is 20 s), `TimeoutStopSec` on Linux
/// (systemd's default is 90 s). `None` when it never forces (`infinity`).
fn force_window(label: &str) -> Result<Option<u64>, String> {
    let path = definition_path(label)?;
    if is_mac() {
        let out = plist_buddy(&path, "Print :ExitTimeOut")?;
        let have = if out.ok {
            out.stdout_text().trim().parse::<u64>().unwrap_or(20)
        } else {
            20
        };
        return Ok(Some(have));
    }
    let text = std::fs::read_to_string(&path)
        .map_err(|e| format!("cannot read {}: {e}", path.display()))?;
    let value = text
        .lines()
        .find_map(|l| l.trim().strip_prefix("TimeoutStopSec="))
        .map_or("90", str::trim);
    if value == "infinity" {
        return Ok(None);
    }
    Ok(Some(if let Some(min) = value.strip_suffix("min") {
        min.trim().parse::<u64>().map_or(0, |m| m * 60)
    } else {
        value
            .trim_end_matches('s')
            .trim()
            .parse::<u64>()
            .unwrap_or(0)
    }))
}

/// The bound on one stop: the longer of `stop_timeout_secs` and the service manager's own
/// stop window, plus a margin. After this the old process has exited, or the service
/// manager never stopped it. Derived from the config and the job file, never a constant.
pub(super) fn stop_wait_secs(label: &str, stop_timeout_secs: u64) -> u64 {
    let window = force_window(label)
        .ok()
        .flatten()
        .unwrap_or(stop_timeout_secs);
    window
        .max(stop_timeout_secs)
        .saturating_add(STOP_MARGIN_SECS)
}

/// Stop the job. launchd sends SIGTERM and waits `ExitTimeOut` before it would send
/// SIGKILL, so [`stop_time_ok`] runs first. `bootout` and `systemctl stop` wait for the
/// process to exit, so `limit` must cover the whole stop window. An error here does not
/// mean the stop did not start: the caller waits for the process either way.
pub(super) fn stop(label: &str, limit: Duration) -> Result<(), String> {
    let out = if is_mac() {
        let target = format!("{}/{label}", domain());
        procs::run(Path::new("launchctl"), &["bootout", &target], limit)?
    } else {
        let unit = unit_name(label);
        procs::run(Path::new("systemctl"), &["--user", "stop", &unit], limit)?
    };
    if out.ok {
        Ok(())
    } else {
        Err(format!("the service did not stop: {}", out.stderr.trim()))
    }
}

/// Queue a start that runs once the stopping process has exited. systemd holds the start
/// job until the stop job ends; launchd has no such queue.
pub(super) fn start_queued(label: &str) -> Result<(), String> {
    if is_mac() {
        return Err("launchd cannot queue a start while the job is still stopping".into());
    }
    let unit = unit_name(label);
    let out = ctl("systemctl", &["--user", "start", "--no-block", &unit])?;
    if out.ok {
        Ok(())
    } else {
        Err(format!("cannot queue a start: {}", out.stderr.trim()))
    }
}

/// The command a person runs to start the job.
pub(super) fn start_command(label: &str) -> String {
    if is_mac() {
        let plist = plist_path(label)
            .map_or_else(|_| format!("<{label}.plist>"), |p| p.display().to_string());
        format!("launchctl bootstrap {} {plist}", domain())
    } else {
        format!("systemctl --user start {}", unit_name(label))
    }
}

pub(super) fn start(label: &str) -> Result<(), String> {
    let out = if is_mac() {
        let plist = plist_path(label)?;
        let text = plist.to_str().ok_or("plist path is not UTF-8")?;
        ctl("launchctl", &["bootstrap", &domain(), text])?
    } else {
        let unit = unit_name(label);
        ctl("systemctl", &["--user", "start", &unit])?
    };
    if out.ok {
        Ok(())
    } else {
        Err(format!("the service did not start: {}", out.stderr.trim()))
    }
}

pub(super) fn plist_buddy(plist: &Path, command: &str) -> Result<procs::Captured, String> {
    let text = plist.to_str().ok_or("plist path is not UTF-8")?;
    procs::run(Path::new(PLIST_BUDDY), &["-c", command, text], TOOL_LIMIT)
}

/// The first program argument the job runs.
pub(super) fn program_of(label: &str) -> Result<PathBuf, String> {
    let path = definition_path(label)?;
    if is_mac() {
        let out = plist_buddy(&path, "Print :ProgramArguments:0")?;
        if !out.ok {
            return Err(format!("cannot read {}", path.display()));
        }
        return Ok(PathBuf::from(out.stdout_text().trim()));
    }
    let text = std::fs::read_to_string(&path)
        .map_err(|e| format!("cannot read {}: {e}", path.display()))?;
    text.lines()
        .find_map(|l| l.trim().strip_prefix("ExecStart="))
        .and_then(|rest| rest.split_whitespace().next())
        .map(PathBuf::from)
        .ok_or_else(|| format!("{} has no ExecStart", path.display()))
}

/// The environment `LASTDB_HOME` of the job, when it sets one.
pub(super) fn home_of(label: &str) -> Option<PathBuf> {
    let path = definition_path(label).ok()?;
    if is_mac() {
        let out = plist_buddy(&path, "Print :EnvironmentVariables:LASTDB_HOME").ok()?;
        return out.ok.then(|| PathBuf::from(out.stdout_text().trim()));
    }
    let text = std::fs::read_to_string(&path).ok()?;
    text.lines()
        .find_map(|l| l.trim().strip_prefix("Environment=LASTDB_HOME="))
        .map(|rest| PathBuf::from(rest.trim_matches('"')))
}

/// The service waits at least `need` seconds for the daemon to exit before it stops it
/// by force. A shorter wait could end a clean shutdown with SIGKILL, so it is refused.
pub(super) fn stop_time_ok(label: &str, need: u64) -> Result<(), String> {
    let path = definition_path(label)?;
    let Some(have) = force_window(label)?.filter(|have| *have < need) else {
        return Ok(());
    };
    Err(if is_mac() {
        format!(
            "the job {label} lets the daemon exit for only {have} seconds (ExitTimeOut) and then sends SIGKILL; set ExitTimeOut to at least {need} in {} and reload the job, or set stop_timeout_secs to at most {have} in the home's upgrade/config.toml",
            path.display()
        )
    } else {
        format!(
            "the unit {label} lets the daemon stop for only {have}s (TimeoutStopSec); set it to at least {need}s in {} and reload the unit, or set stop_timeout_secs to at most {have} in the home's upgrade/config.toml",
            path.display()
        )
    })
}

fn xml(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

/// The LaunchAgent of contract section 11.
pub(super) fn render_plist(label: &str, home: &Path, exit_timeout: u64) -> String {
    let h = xml(&home.display().to_string());
    let label = xml(label);
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">\n<plist version=\"1.0\"><dict>\n<key>Label</key><string>{label}</string>\n<key>ProgramArguments</key><array><string>{h}/current/lastdbd</string></array>\n<key>EnvironmentVariables</key><dict><key>LASTDB_HOME</key><string>{h}</string></dict>\n<key>KeepAlive</key><true/>\n<key>RunAtLoad</key><true/>\n<key>ExitTimeOut</key><integer>{exit_timeout}</integer>\n<key>StandardOutPath</key><string>{h}/logs/lastdbd.out.log</string>\n<key>StandardErrorPath</key><string>{h}/logs/lastdbd.err.log</string>\n</dict></plist>\n"
    )
}

/// The systemd user unit of contract section 11.
pub(super) fn render_unit(home: &Path, stop_secs: u64) -> Result<String, String> {
    let h = home.display().to_string();
    if h.contains(|c: char| c.is_whitespace() || c == '%' || c == '"' || c == '\\') {
        return Err("a systemd unit cannot hold this home path; use a path without spaces".into());
    }
    Ok(format!(
        "[Unit]\nDescription=LastDB node\n\n[Service]\nExecStart={h}/current/lastdbd\nEnvironment=LASTDB_HOME={h}\nRestart=always\nTimeoutStopSec={stop_secs}\nStandardOutput=append:{h}/logs/lastdbd.out.log\nStandardError=append:{h}/logs/lastdbd.err.log\n\n[Install]\nWantedBy=default.target\n"
    ))
}

/// Write the job file for a new service and make the init system read it.
pub(super) fn write_definition(label: &str, home: &Path, stop_secs: u64) -> Result<(), String> {
    if !valid_label(label) {
        return Err("invalid service label".into());
    }
    let path = definition_path(label)?;
    let text = if is_mac() {
        render_plist(label, home, stop_secs + 60)
    } else {
        render_unit(home, stop_secs + 60)?
    };
    std::fs::create_dir_all(home.join("logs"))
        .map_err(|e| format!("cannot create the logs folder: {e}"))?;
    super::state::atomic_write(&path, text.as_bytes(), 0o644)?;
    if !is_mac() {
        ctl("systemctl", &["--user", "daemon-reload"])?;
    }
    Ok(())
}
