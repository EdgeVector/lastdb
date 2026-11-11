//! Explicit V2 backup and credentials-only recovery-home commands.
use super::*;
use cloud_backup_protocol::{AccountId, Id};
use lastdb_node::byte_cloud_backup::{ByteBackupRequest, SensitiveConnectionRequest};
use std::io::Read;
use zeroize::Zeroizing;

#[path = "cloud_cmds/byte_backup/offline.rs"]
mod offline;
#[path = "cloud_cmds/recovery_home.rs"]
mod recovery_home;
#[path = "cloud_cmds/byte_backup/setup.rs"]
mod setup;
#[path = "cloud_cmds/shared_restore.rs"]
mod shared_restore;
#[path = "cloud_cmds/shared_writer.rs"]
mod shared_writer;

pub(super) use recovery_home::files::FreshHome;

pub(super) fn cloud_command(data_dir: Option<PathBuf>, action: CloudCommand) -> Result<(), String> {
    let action = match action {
        CloudCommand::SharedWriter(selection) => {
            return shared_writer::run(data_dir.as_deref(), selection);
        }
        CloudCommand::RestoreShared(selection) => {
            return shared_restore::run(data_dir.as_deref(), selection);
        }
        CloudCommand::ResumeSharedRestore(ResumeSharedRestoreArgs { workspace, json }) => {
            return shared_restore::resume(data_dir.as_deref(), &workspace, json);
        }
        CloudCommand::BackupV2 { action } if setup::is_setup(&action) => {
            return setup::setup(data_dir.as_deref(), action);
        }
        action => action,
    };
    if recovery_home::handles(&action) {
        return recovery_home::command(data_dir.as_deref(), action);
    }
    if matches!(action, CloudCommand::BackupV2 { .. })
        && !data_dir.as_ref().is_some_and(|home| home.is_absolute())
    {
        return Err("V2 backup requires an explicit absolute --data-dir home".into());
    }
    let home = lastdb_node::host::resolve_home(data_dir)?;
    match action {
        CloudCommand::BackupV2 { action } => run_byte_backup(&home, action),
        CloudCommand::PrepareRecoveryHome(_)
        | CloudCommand::RecoverCredentialsHome(_)
        | CloudCommand::RecoverDevCredentialsHome { .. }
        | CloudCommand::SharedWriter(_)
        | CloudCommand::RestoreShared(_)
        | CloudCommand::ResumeSharedRestore(_) => {
            unreachable!("handled before home resolution")
        }
    }
}

fn run_byte_backup(home: &Path, action: ByteBackupCommand) -> Result<(), String> {
    let (request, json) = match action {
        ByteBackupCommand::Start(BackupStartArgs { request_id, json }) => {
            let request_id =
                Id::try_from(request_id.unwrap_or_else(|| uuid::Uuid::new_v4().to_string()))
                    .map_err(|_| "V2 request ID must be a canonical UUID")?;
            eprintln!("V2 backup request ID: {}", request_id.as_str());
            (ByteBackupRequest::Start { request_id }, json)
        }
        ByteBackupCommand::Resume(BackupJsonArgs { json }) => (ByteBackupRequest::Resume, json),
        ByteBackupCommand::Status(BackupJsonArgs { json }) => (ByteBackupRequest::Status, json),
        ByteBackupCommand::Close(BackupJsonArgs { json }) => (ByteBackupRequest::Close, json),
        ByteBackupCommand::EnableAutomatic(BackupEnableArgs {
            log_interval_seconds,
            snapshot_interval_seconds,
            json,
        }) => (
            ByteBackupRequest::EnableAutomatic {
                log_interval_seconds,
                snapshot_interval_seconds,
            },
            json,
        ),
        ByteBackupCommand::DisableAutomatic(BackupJsonArgs { json }) => {
            (ByteBackupRequest::DisableAutomatic, json)
        }
        action => return offline::run(home, action),
    };
    let body = serde_json::to_value(request).map_err(|_| "cannot encode V2 owner request")?;
    let response = super::socket_client::post_json_admin(
        &home.join("data/folddb.sock"),
        "/api/sync/byte-backup-v2",
        &body,
    )?;
    print_backup(&parse_json_response(&response, "V2 backup")?, json)
}

fn run_existing(
    home: &Path,
    environment: &str,
    expected: Option<&str>,
    json: bool,
) -> Result<(), String> {
    if !home.is_absolute() || !matches!(environment, "dev" | "prod") {
        return Err(
            "existing connection requires an absolute home and dev or prod environment".into(),
        );
    }
    let expected_user_hash = expected
        .map(|value| AccountId::try_from(value.to_owned()))
        .transpose()
        .map_err(|_| "invalid expected account identity")?;
    let mut bytes = Zeroizing::new(Vec::new());
    std::io::stdin()
        .take(4097)
        .read_to_end(&mut bytes)
        .map_err(|_| "cannot read existing account API key")?;
    if bytes.len() > 4096 {
        return Err("existing account API key exceeds its byte limit".into());
    }
    let api_key = std::str::from_utf8(&bytes)
        .map_err(|_| "API key must be UTF-8")?
        .trim();
    if api_key.is_empty() || api_key.chars().any(char::is_control) {
        return Err("existing account API key is empty or invalid".into());
    }
    let request = SensitiveConnectionRequest {
        environment: environment.to_owned(),
        expected_user_hash,
        api_key: api_key.to_owned(),
    };
    let body = Zeroizing::new(
        serde_json::to_vec(&request).map_err(|_| "cannot encode protected connection request")?,
    );
    let mut wire = Zeroizing::new(
        format!(
            "POST /api/sync/byte-backup-v2/connect HTTP/1.1\r\nHost: localhost\r\n{}Content-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            super::socket_client::client_headers(),
            body.len(),
        )
        .into_bytes(),
    );
    wire.extend_from_slice(&body);
    let response = super::socket_client::request_with_timeout(
        &home.join("data/folddb.sock"),
        &wire,
        admin_scan_client_timeout(),
    )?;
    print_backup(
        &parse_json_response(&response, "V2 existing account connection")?,
        json,
    )
}

fn print_backup(value: &serde_json::Value, json: bool) -> Result<(), String> {
    if !json {
        println!(
            "{}",
            if value.get("backup_kind").and_then(serde_json::Value::as_str) == Some("shared") {
                "Cloud sync"
            } else {
                "V2 backup"
            }
        );
    }
    println!(
        "{}",
        serde_json::to_string_pretty(value).map_err(|_| "cannot encode V2 result")?
    );
    Ok(())
}

fn read_recovery_phrase() -> Result<Zeroizing<String>, String> {
    let mut bytes = Zeroizing::new(Vec::new());
    std::io::stdin()
        .lock()
        .take(4097)
        .read_to_end(&mut bytes)
        .map_err(|_| "cannot read recovery phrase")?;
    if bytes.len() > 4096 {
        return Err("recovery phrase exceeds its byte limit".into());
    }
    let text = std::str::from_utf8(&bytes)
        .map_err(|_| "recovery phrase must be UTF8")?
        .trim();
    if text.is_empty() {
        return Err("recovery phrase is empty".into());
    }
    Ok(Zeroizing::new(text.to_owned()))
}
