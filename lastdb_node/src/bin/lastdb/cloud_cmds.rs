//! Backup connection setup and recovery-home commands (no snapshots).

use super::*;
use serde_json::Value;
use std::io::Read;
use std::path::PathBuf;
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

pub(super) fn backup_command(
    data_dir: Option<PathBuf>,
    _db: Option<String>,
    action: BackupCommand,
) -> Result<(), String> {
    match action {
        BackupCommand::Connect(a) => setup::run_connect(data_dir, &a),
        BackupCommand::Restore(args) => {
            let expanded = data_dir
                .map(lastdb_node::profile::paths::expand_tilde_path)
                .transpose()?;
            if expanded.as_deref().is_none_or(|h| !h.is_absolute()) {
                return Err(
                    "signed backup restore requires an explicit absolute --data-dir home".into(),
                );
            }
            restore::run(&lastdb_node::host::resolve_home(expanded)?, args)
        }
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
