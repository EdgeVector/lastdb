//! Explicit cleanup request, read-only status and optional bounded wait.

use super::*;
use fold_db::storage::disk_pack::{NativeCleanupPhase, NativeCleanupStatus};

pub(crate) fn db_cleanup(
    socket: &Path,
    status_only: bool,
    wait: bool,
    timeout: u64,
    json: bool,
) -> Result<(), String> {
    let mut status = if status_only {
        read_status(socket, Duration::from_secs(30))?
    } else {
        let value = db_post_json(socket, "/api/db/cleanup", &serde_json::json!({}))?;
        decode(&value)?
    };
    let id = status.request_id.clone();
    let started = std::time::Instant::now();
    let limit = Duration::from_secs(timeout);
    while wait && !status.complete {
        if status.phase == NativeCleanupPhase::RecoveryRequired || status.request_id.is_none() {
            print_status(&status, json)?;
            return Err(status
                .last_error
                .unwrap_or_else(|| "no cleanup request is active".into()));
        }
        let Some(left) = limit.checked_sub(started.elapsed()) else {
            print_status(&status, json)?;
            return Err("cleanup remains incomplete; its durable request continues after this client timeout".into());
        };
        std::thread::sleep(Duration::from_millis(250).min(left));
        let Some(left) = limit.checked_sub(started.elapsed()) else {
            continue;
        };
        status = read_status(socket, Duration::from_secs(30).min(left))?;
        if status.request_id != id {
            return Err("the latest cleanup request changed while this client waited".into());
        }
    }
    print_status(&status, json)
}

fn read_status(socket: &Path, timeout: Duration) -> Result<NativeCleanupStatus, String> {
    let response = get_path(socket, "/api/db/cleanup", timeout)?;
    decode(&parse_json_response(&response, "cleanup status")?)
}

fn decode(value: &serde_json::Value) -> Result<NativeCleanupStatus, String> {
    let report = value
        .get("cleanup")
        .cloned()
        .ok_or("response missing cleanup")?;
    let status: NativeCleanupStatus = serde_json::from_value(report)
        .map_err(|error| format!("invalid cleanup response: {error}"))?;
    if status.complete != (status.phase == NativeCleanupPhase::Complete) {
        return Err("cleanup response has inconsistent completion fields".into());
    }
    Ok(status)
}

fn print_status(status: &NativeCleanupStatus, json: bool) -> Result<(), String> {
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(status).map_err(|error| error.to_string())?
        );
    } else {
        println!("Cleanup: {:?}", status.phase);
        if let Some(id) = &status.request_id {
            println!("  request: {id}");
        }
        println!(
            "  main: {} tiered, {} merge inputs, {} held bytes",
            status.main.tiered_runs, status.main.merging_runs, status.main.held_retired_bytes
        );
        println!(
            "  file blobs: {} tiered, {} merge inputs, {} held bytes",
            status.cas.tiered_runs, status.cas.merging_runs, status.cas.held_retired_bytes
        );
        if let Some(boundary) = status.completed_through {
            println!(
                "  completed through: main {}, file blobs {}",
                boundary.main_frontier, boundary.cas_frontier
            );
        }
        if let Some(error) = &status.last_error {
            println!("  error: {error}");
        }
        println!(
            "  Scope: data already deleted from the active stores. Backup copies remain separate."
        );
    }
    Ok(())
}
