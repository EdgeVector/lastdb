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

/// The longest a `--wait` snapshot waits. A large store uploads for a while.
const SNAPSHOT_WAIT: Duration = Duration::from_secs(1800);

pub(super) fn backup_command(
    data_dir: Option<PathBuf>,
    db: Option<String>,
    action: BackupCommand,
) -> Result<(), String> {
    match action {
        BackupCommand::Snapshot { database, wait } => {
            queue_snapshot(data_dir, database.or(db).as_deref(), wait)
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

fn queue_snapshot(
    data_dir: Option<PathBuf>,
    database: Option<&str>,
    wait: bool,
) -> Result<(), String> {
    let (_home, socket) = resolve_client_home_and_socket(data_dir)?;
    require_reachable(&socket)?;
    let raw = post_json_admin(
        &socket,
        "/api/backup/snapshot",
        &serde_json::json!({ "database": database }),
    )?;
    let reply = parse_json_or_error(&raw, "backup snapshot")?;
    let nonce = reply["nonce"]
        .as_str()
        .filter(|s| !s.is_empty())
        .ok_or("backup snapshot response missing nonce")?;
    if !wait {
        println!("Queued snapshot of the personal database");
        return Ok(());
    }
    wait_for_snapshot(&socket, nonce)
}

fn wait_for_snapshot(socket: &Path, nonce: &str) -> Result<(), String> {
    let path = format!("/api/backup/snapshot?nonce={nonce}");
    let deadline = Instant::now() + SNAPSHOT_WAIT;
    loop {
        if let Ok(raw) = get_path_retry(socket, &path, Duration::from_secs(10)) {
            if let Ok(reply) = parse_json_response(&raw, "backup snapshot status") {
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
            return Err("the snapshot did not reach the relay in time".into());
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
