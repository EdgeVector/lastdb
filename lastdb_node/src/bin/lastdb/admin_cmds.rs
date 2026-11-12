use super::*;
use fold_db::storage::disk_pack::{NativeCleanupPhase, NativeCleanupStatus};

pub(super) fn run(data_dir: Option<PathBuf>, action: AdminCommand) -> Result<(), String> {
    match action {
        AdminCommand::Storage(AdminStorageCommand::Cleanup {
            status,
            wait,
            timeout,
            json,
        }) => {
            let (_, socket) = resolve_client_home_and_socket(data_dir)?;
            if probe_health(&socket).is_err() {
                return Err(format!(
                    "lastdbd not reachable at {} — start the daemon first",
                    socket.display()
                ));
            }
            db_cleanup(&socket, status, wait, timeout, json)
        }
        AdminCommand::Backup(action) => cloud_command(data_dir, backup_action(action)?),
    }
}

fn backup_action(action: AdminBackupCommand) -> Result<CloudCommand, String> {
    Ok(match action {
        AdminBackupCommand::Init(a) => CloudCommand::BackupV2 {
            action: ByteBackupCommand::Init(a),
        },
        AdminBackupCommand::Resume(a) => CloudCommand::BackupV2 {
            action: ByteBackupCommand::Resume(a),
        },
        AdminBackupCommand::Close(a) => CloudCommand::BackupV2 {
            action: ByteBackupCommand::Close(a),
        },
        AdminBackupCommand::InspectDependencies(a) => CloudCommand::BackupV2 {
            action: ByteBackupCommand::InspectDependencies(a),
        },
        AdminBackupCommand::RecoverCredentialsHome { args, existing_dev } => {
            if existing_dev {
                if args.env != "dev" {
                    return Err("--existing-dev requires --env dev".into());
                }
                CloudCommand::RecoverDevCredentialsHome {
                    into: args.into,
                    expected_user_hash: args.expected_user_hash,
                    identity_seed_stdin: args.identity_seed_stdin,
                    json: args.json,
                }
            } else {
                CloudCommand::RecoverCredentialsHome(args)
            }
        }
        AdminBackupCommand::PrepareRecoveryHome(a) => CloudCommand::PrepareRecoveryHome(a),
    })
}

fn decode_cleanup(value: &serde_json::Value) -> Result<NativeCleanupStatus, String> {
    let report = value
        .get("cleanup")
        .cloned()
        .ok_or("response missing cleanup")?;
    let status: NativeCleanupStatus =
        serde_json::from_value(report).map_err(|e| format!("invalid cleanup response: {e}"))?;
    (status.complete == (status.phase == NativeCleanupPhase::Complete))
        .then_some(status)
        .ok_or_else(|| "cleanup response has inconsistent completion fields".into())
}

fn read_cleanup_status(socket: &Path, timeout: Duration) -> Result<NativeCleanupStatus, String> {
    decode_cleanup(&parse_json_response(
        &get_path(socket, "/api/db/cleanup", timeout)?,
        "cleanup status",
    )?)
}

fn db_cleanup(
    socket: &Path,
    status_only: bool,
    wait: bool,
    timeout: u64,
    json: bool,
) -> Result<(), String> {
    let mut status = if status_only {
        read_cleanup_status(socket, Duration::from_secs(30))?
    } else {
        let raw = post_json_admin(socket, "/api/db/cleanup", &serde_json::json!({}))?;
        decode_cleanup(&parse_json_response(&raw, "/api/db/cleanup")?)?
    };
    let id = status.request_id.clone();
    let (started, limit) = (Instant::now(), Duration::from_secs(timeout));
    while wait && !status.complete {
        if status.phase == NativeCleanupPhase::RecoveryRequired || status.request_id.is_none() {
            print_cleanup_status(&status, json)?;
            return Err(status
                .last_error
                .unwrap_or_else(|| "no cleanup request is active".into()));
        }
        let Some(left) = limit.checked_sub(started.elapsed()) else {
            print_cleanup_status(&status, json)?;
            return Err(
                "cleanup remains incomplete; its durable request continues after this client timeout".into(),
            );
        };
        std::thread::sleep(Duration::from_millis(250).min(left));
        if let Some(left) = limit.checked_sub(started.elapsed()) {
            status = read_cleanup_status(socket, Duration::from_secs(30).min(left))?;
            if status.request_id != id {
                return Err("the latest cleanup request changed while this client waited".into());
            }
        }
    }
    print_cleanup_status(&status, json)
}

fn print_cleanup_status(status: &NativeCleanupStatus, json: bool) -> Result<(), String> {
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(status).map_err(|e| e.to_string())?
        );
        return Ok(());
    }
    println!("Cleanup: {:?}", status.phase);
    if let Some(id) = &status.request_id {
        println!("  request: {id}");
    }
    for (label, s) in [("main", &status.main), ("file blobs", &status.cas)] {
        println!(
            "  {label}: {} tiered, {} merge inputs, {} held bytes",
            s.tiered_runs, s.merging_runs, s.held_retired_bytes
        );
    }
    if let Some(b) = status.completed_through {
        println!(
            "  completed through: main {}, file blobs {}",
            b.main_frontier, b.cas_frontier
        );
    }
    if let Some(error) = &status.last_error {
        println!("  error: {error}");
    }
    println!(
        "  Scope: data already deleted from the active stores. Backup copies remain separate."
    );
    Ok(())
}
