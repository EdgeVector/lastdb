//! Backup connection setup, snapshot requests, and recovery-home commands.

use super::*;
use serde_json::Value;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use zeroize::Zeroizing;

#[path = "cloud_cmds/account.rs"]
mod account;
#[path = "cloud_cmds/dev_keys.rs"]
mod dev_keys;
#[path = "cloud_cmds/enrollment_home.rs"]
mod enrollment_home;
#[path = "cloud_cmds/restore.rs"]
mod restore;
#[path = "cloud_cmds/setup.rs"]
mod setup;

pub(super) use account::cloud_account_command;
pub(crate) use dev_keys::with_publishing_credential;

/// The longest a `--wait` snapshot or sweep waits. A large store uploads for a while.
const SNAPSHOT_WAIT: Duration = Duration::from_secs(1800);

/// One kind of request that `/api/backup/snapshot` queues for the sync loop.
struct Queued {
    /// True for a sweep (`"reclaim": true` in the body), false for a snapshot.
    reclaim: bool,
    /// The query parameter that carries the nonce when the state of the request is asked.
    nonce: &'static str,
    /// The name of the command in an error.
    label: &'static str,
    /// What is printed when the command does not wait.
    queued: &'static str,
    /// The error when the wait ends first.
    late: &'static str,
}

const SNAPSHOT: Queued = Queued {
    reclaim: false,
    nonce: "nonce",
    label: "backup snapshot",
    queued: "Queued snapshot of the personal database",
    late: "the snapshot did not reach the relay in time",
};

const RECLAIM: Queued = Queued {
    reclaim: true,
    nonce: "reclaim",
    label: "backup reclaim",
    queued: "Queued a sweep of the relay storage of the personal database; `lastdb status` shows what the relay holds after it",
    late: "the sweep did not finish in time",
};

pub(super) fn backup_command(
    data_dir: Option<PathBuf>,
    action: BackupCommand,
) -> Result<(), String> {
    match action {
        BackupCommand::Snapshot { database, wait } => {
            queue(data_dir, database.as_deref(), wait, &SNAPSHOT)
        }
        BackupCommand::Reclaim { database, wait } => {
            queue(data_dir, database.as_deref(), wait, &RECLAIM)
        }
        BackupCommand::Connect(a) => setup::run_connect(data_dir, &a),
        BackupCommand::Restore(args) => {
            let expanded = data_dir
                .map(lastdb_node::profile::paths::expand_tilde_path)
                .transpose()?;
            restore::run(expanded.as_deref(), args)
        }
    }
}

fn queue(
    data_dir: Option<PathBuf>,
    database: Option<&str>,
    wait: bool,
    kind: &Queued,
) -> Result<(), String> {
    let (_home, socket) = resolve_client_home_and_socket(data_dir)?;
    require_reachable(&socket)?;
    let mut body = serde_json::json!({ "database": database });
    if kind.reclaim {
        body["reclaim"] = Value::Bool(true);
    }
    let raw = post_json_admin(&socket, "/api/backup/snapshot", &body)?;
    let reply = parse_json_or_error(&raw, kind.label)?;
    let nonce = reply["nonce"]
        .as_str()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| format!("{} response missing nonce", kind.label))?;
    if !wait {
        println!("{}", kind.queued);
        return Ok(());
    }
    wait_for(&socket, nonce, kind)
}

/// Wait until the daemon answered the request `nonce`, then print its report as JSON.
fn wait_for(socket: &Path, nonce: &str, kind: &Queued) -> Result<(), String> {
    let path = format!("/api/backup/snapshot?{}={nonce}", kind.nonce);
    let status = format!("{} status", kind.label);
    let deadline = Instant::now() + SNAPSHOT_WAIT;
    loop {
        if let Ok(raw) = get_path_retry(socket, &path, Duration::from_secs(10)) {
            if let Ok(reply) = parse_json_response(&raw, &status) {
                if reply["ready"].as_bool() == Some(true) {
                    println!(
                        "{}",
                        serde_json::to_string(&reply["report"]).map_err(|_| "bad reply")?
                    );
                    return Ok(());
                }
                if let Some(error) = reply["error"].as_str().filter(|s| !s.is_empty()) {
                    return Err(error.to_owned());
                }
            }
        }
        if Instant::now() >= deadline {
            return Err(kind.late.into());
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}

pub(super) fn admin_backup_command(
    data_dir: Option<PathBuf>,
    action: AdminBackupCommand,
) -> Result<(), String> {
    match action {
        AdminBackupCommand::Init(a) => setup::run_init(data_dir.as_deref(), &a),
        AdminBackupCommand::RecoverCredentialsHome(args) => {
            let expanded = data_dir
                .map(lastdb_node::profile::paths::expand_tilde_path)
                .transpose()?;
            restore::recover_credentials_home(expanded.as_deref(), args)
        }
    }
}

pub(super) fn print_backup(value: &Value, json: bool) -> Result<(), String> {
    if !json {
        println!("Signed backup");
    }
    println!(
        "{}",
        serde_json::to_string_pretty(value).map_err(|_| "cannot encode backup result")?
    );
    Ok(())
}

pub(super) fn read_stdin_no_control(
    limit: usize,
    label: &str,
) -> Result<Zeroizing<String>, String> {
    let text = read_stdin_secret(limit, label)?;
    (!text.chars().any(char::is_control))
        .then_some(text)
        .ok_or_else(|| format!("{label} is invalid"))
}

pub(super) fn read_stdin_secret(limit: usize, label: &str) -> Result<Zeroizing<String>, String> {
    let mut bytes = Zeroizing::new(Vec::new());
    std::io::stdin()
        .lock()
        .take((limit + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|_| format!("cannot read {label}"))?;
    if bytes.len() > limit {
        return Err(format!("{label} exceeds its byte limit"));
    }
    let text = std::str::from_utf8(&bytes)
        .map_err(|_| format!("{label} must be UTF-8"))?
        .trim();
    (!text.is_empty())
        .then(|| Zeroizing::new(text.to_owned()))
        .ok_or_else(|| format!("{label} is empty"))
}
