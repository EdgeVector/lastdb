//! Administrative storage cleanup and backup-home commands (`lastdb admin`).

use super::*;
use fold_db::storage::disk_pack::{NativeCleanupPhase, NativeCleanupStatus};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

pub(super) fn run_admin(data_dir: Option<PathBuf>, action: AdminCommand) -> Result<(), String> {
    match action {
        AdminCommand::Storage(AdminStorageCommand::Cleanup {
            status,
            wait,
            timeout,
            json,
        }) => {
            let (_, socket) = resolve_client_home_and_socket(data_dir)?;
            require_reachable(&socket)?;
            db_cleanup(&socket, status, wait, timeout, json)
        }
        AdminCommand::Backup(AdminBackupCommand::Init(a)) => cloud_command(
            data_dir,
            CloudCommand::BackupV2 {
                action: ByteBackupCommand::Init(a),
            },
        ),
        AdminCommand::Backup(AdminBackupCommand::PrepareRecoveryHome(a)) => {
            cloud_command(data_dir, CloudCommand::PrepareRecoveryHome(a))
        }
        AdminCommand::Backup(AdminBackupCommand::RecoverCredentialsHome { args, existing_dev }) => {
            if !existing_dev {
                return cloud_command(data_dir, CloudCommand::RecoverCredentialsHome(args));
            }
            if args.env != "dev" {
                return Err("--existing-dev requires --env dev".into());
            }
            let cmd = CloudCommand::RecoverDevCredentialsHome {
                into: args.into,
                expected_user_hash: args.expected_user_hash,
                identity_seed_stdin: args.identity_seed_stdin,
                json: args.json,
            };
            cloud_command(data_dir, cmd)
        }
    }
}

fn decode_cleanup(raw: &str, label: &str) -> Result<NativeCleanupStatus, String> {
    let v = parse_json_response(raw, label)?
        .get("cleanup")
        .cloned()
        .ok_or("response missing cleanup")?;
    let s: NativeCleanupStatus =
        serde_json::from_value(v).map_err(|e| format!("invalid cleanup response: {e}"))?;
    if s.complete != (s.phase == NativeCleanupPhase::Complete) {
        return Err("cleanup response has inconsistent completion fields".into());
    }
    Ok(s)
}

fn db_cleanup(
    socket: &Path,
    status_only: bool,
    wait: bool,
    timeout: u64,
    json: bool,
) -> Result<(), String> {
    let (raw, label) = if status_only {
        (
            get_path(socket, "/api/db/cleanup", Duration::from_secs(30))?,
            "cleanup status",
        )
    } else {
        (
            post_json_admin(socket, "/api/db/cleanup", &serde_json::json!({}))?,
            "/api/db/cleanup",
        )
    };
    let mut s = decode_cleanup(&raw, label)?;
    let id = s.request_id.clone();
    let (started, limit) = (Instant::now(), Duration::from_secs(timeout));
    while wait && !s.complete {
        if s.phase == NativeCleanupPhase::RecoveryRequired || s.request_id.is_none() {
            print_cleanup_status(&s, json);
            return Err(s
                .last_error
                .unwrap_or_else(|| "no cleanup request is active".into()));
        }
        let Some(left) = limit.checked_sub(started.elapsed()) else {
            print_cleanup_status(&s, json);
            return Err("cleanup remains incomplete; its durable request continues after this client timeout".into());
        };
        std::thread::sleep(Duration::from_millis(250).min(left));
        if let Some(left) = limit.checked_sub(started.elapsed()) {
            s = decode_cleanup(
                &get_path(socket, "/api/db/cleanup", Duration::from_secs(30).min(left))?,
                "cleanup status",
            )?;
            if s.request_id != id {
                return Err("the latest cleanup request changed while this client waited".into());
            }
        }
    }
    print_cleanup_status(&s, json);
    Ok(())
}

fn print_cleanup_status(s: &NativeCleanupStatus, json: bool) {
    if json {
        print_pretty_json(s);
        return;
    }
    println!("Cleanup: {:?}", s.phase);
    if let Some(id) = &s.request_id {
        println!("  request: {id}");
    }
    for (label, m) in [("main", &s.main), ("file blobs", &s.cas)] {
        println!(
            "  {label}: {} tiered, {} merge inputs, {} held bytes",
            m.tiered_runs, m.merging_runs, m.held_retired_bytes
        );
    }
    if let Some(b) = s.completed_through {
        println!(
            "  completed through: main {}, file blobs {}",
            b.main_frontier, b.cas_frontier
        );
    }
    if let Some(err) = &s.last_error {
        println!("  error: {err}");
    }
    println!(
        "  Scope: data already deleted from the active stores. Backup copies remain separate."
    );
}
