//! Storage inspection, space reclamation, and delete-ledger commands.

use super::*;
use fold_db::storage::disk_pack::{NativeCleanupPhase, NativeCleanupStatus};

pub(crate) fn db_storage_command(
    data_dir: Option<PathBuf>,
    action: AdminStorageCommand,
) -> Result<(), String> {
    let (_, socket) = resolve_client_home_and_socket(data_dir)?;
    if probe_health(&socket).is_err() {
        return Err(format!(
            "lastdbd not reachable at {} — start the daemon first",
            socket.display()
        ));
    }
    match action {
        AdminStorageCommand::Inventory {
            by_schema: _,
            limit,
            json,
            out,
            timeout,
        } => db_schemas(&socket, json, out.as_deref(), timeout, limit.unwrap_or(50)),
        AdminStorageCommand::Reclaim(DbReclaimCommand::Cleanup {
            status,
            wait,
            timeout,
            json,
        }) => db_cleanup(&socket, status, wait, timeout, json),
        AdminStorageCommand::Reclaim(DbReclaimCommand::DeleteLedger { limit, json }) => {
            db_delete_ledger(&socket, limit, json)
        }
    }
}

fn decode_cleanup(value: &serde_json::Value) -> Result<NativeCleanupStatus, String> {
    let report = value
        .get("cleanup")
        .cloned()
        .ok_or("response missing cleanup")?;
    let status: NativeCleanupStatus =
        serde_json::from_value(report).map_err(|e| format!("invalid cleanup response: {e}"))?;
    if status.complete != (status.phase == NativeCleanupPhase::Complete) {
        return Err("cleanup response has inconsistent completion fields".into());
    }
    Ok(status)
}

fn read_cleanup_status(socket: &Path, timeout: Duration) -> Result<NativeCleanupStatus, String> {
    let response = get_path(socket, "/api/db/cleanup", timeout)?;
    decode_cleanup(&parse_json_response(&response, "cleanup status")?)
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
        decode_cleanup(&db_post_json(
            socket,
            "/api/db/cleanup",
            &serde_json::json!({}),
        )?)?
    };
    let id = status.request_id.clone();
    let started = Instant::now();
    let limit = Duration::from_secs(timeout);
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
        let Some(left) = limit.checked_sub(started.elapsed()) else {
            continue;
        };
        status = read_cleanup_status(socket, Duration::from_secs(30).min(left))?;
        if status.request_id != id {
            return Err("the latest cleanup request changed while this client waited".into());
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
    println!(
        "  main: {} tiered, {} merge inputs, {} held bytes",
        status.main.tiered_runs, status.main.merging_runs, status.main.held_retired_bytes
    );
    println!(
        "  file blobs: {} tiered, {} merge inputs, {} held bytes",
        status.cas.tiered_runs, status.cas.merging_runs, status.cas.held_retired_bytes
    );
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

fn db_delete_ledger(socket: &Path, limit: u64, json_only: bool) -> Result<(), String> {
    let target = if limit == 0 {
        "/api/db/delete-ledger".to_string()
    } else {
        format!("/api/db/delete-ledger?limit={limit}")
    };
    let response = get_path(socket, &target, admin_scan_client_timeout())?;
    let ledger = parse_json_response(&response, "delete-ledger")?
        .get("delete_ledger")
        .cloned()
        .ok_or("response missing delete_ledger field")?;
    if json_only {
        println!(
            "{}",
            serde_json::to_string_pretty(&ledger).map_err(|e| format!("serialize: {e}"))?
        );
        return Ok(());
    }
    let entries = ledger
        .get("entries")
        .and_then(serde_json::Value::as_array)
        .cloned()
        .unwrap_or_default();
    if entries.is_empty() {
        println!("Record delete ledger: EMPTY — no delete or purge batch has run here.");
        println!("  A missing record on this store was not removed by a delete path.");
        return Ok(());
    }
    println!(
        "Record delete ledger — {} row(s), oldest first",
        entries.len()
    );
    for e in &entries {
        let s = |k: &str| e.get(k).and_then(serde_json::Value::as_str).unwrap_or("");
        let n = |k: &str| e.get(k).and_then(serde_json::Value::as_u64).unwrap_or(0);
        let mut line = format!(
            "  {}  {:<15} records={}",
            s("at"),
            s("verb"),
            n("records_erased")
        );
        if let Some(schema) = e.get("schema").and_then(serde_json::Value::as_str) {
            line.push_str(&format!(" schema={schema}"));
        }
        if let Some(fp) = e.get("key_fingerprint").and_then(serde_json::Value::as_str) {
            line.push_str(&format!(
                " key_fp={}",
                observability::truncate::truncate_on_char_boundary(fp, 12)
            ));
        }
        println!("{line}");
    }
    Ok(())
}

fn db_schemas(
    socket: &Path,
    json_only: bool,
    out: Option<&Path>,
    timeout_secs: Option<u64>,
    limit: usize,
) -> Result<(), String> {
    let deadline = inventory_client_timeout(timeout_secs);
    if !json_only {
        eprintln!(
            "lastdb admin storage inventory --by-schema: heavy record walk (client deadline {}s; raise with --timeout or LASTDB_UDS_ADMIN_TIMEOUT_SECS). This is not a cheap status gauge.",
            deadline.as_secs().max(1)
        );
    }
    let response = get_path(socket, "/api/db/schemas", deadline)?;
    let report = parse_json_response(&response, "schema storage")?
        .get("schema_storage")
        .cloned()
        .ok_or("response missing schema_storage field")?;
    let pretty = serde_json::to_string_pretty(&report)
        .map_err(|e| format!("serialize schema storage: {e}"))?;
    if let Some(path) = out {
        write_product_file_atomic(path, pretty.as_bytes())?;
        if !json_only {
            eprintln!("Wrote {}", path.display());
        }
    }
    if json_only {
        println!("{pretty}");
        return Ok(());
    }
    let measured = report
        .get("measured_at")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("?");
    let u64_of =
        |r: &serde_json::Value, k: &str| r.get(k).and_then(serde_json::Value::as_u64).unwrap_or(0);
    println!("Per-schema record storage  measured_at={measured}  heavy=true");
    println!(
        "  {:>10}  {:>8} records  {} schemas\n\n  {:>10}  {:>8}  {:<32}  SCHEMA",
        format_bytes(u64_of(&report, "total_logical_bytes")),
        u64_of(&report, "total_records"),
        u64_of(&report, "schema_count"),
        "SIZE",
        "RECORDS",
        "NAME"
    );
    let rows = report
        .get("per_schema")
        .and_then(serde_json::Value::as_array)
        .cloned()
        .unwrap_or_default();
    let shown = if limit == 0 { rows.len() } else { limit };
    for (i, row) in rows.iter().enumerate() {
        if i >= shown {
            println!(
                "  … {} more schemas (pass --limit 0 for all)",
                rows.len() - shown
            );
            break;
        }
        let identity = row
            .get("schema_name")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("?");
        let name = row
            .get("display_name")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("-");
        println!(
            "  {:>10}  {:>8}  {:<32}  {identity}",
            format_bytes(u64_of(row, "bytes")),
            u64_of(row, "record_count"),
            truncate_label(name, 32),
        );
    }
    Ok(())
}
