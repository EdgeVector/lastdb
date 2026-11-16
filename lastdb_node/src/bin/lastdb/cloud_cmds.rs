//! Backup connection setup, snapshot queueing, log maintenance, and recovery-home commands.

use super::*;
use cloud_backup_protocol::Digest;
use lastdb_node::log_sync::{index_history, reclaim_snapshot_log, IndexHistoryReport};
use serde_json::Value;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::thread;
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

const SNAPSHOT_WAIT: Duration = Duration::from_secs(70);

enum Poll {
    Pending,
    Ready(u64, u64),
    Failed(String),
}

pub(super) fn backup_command(
    data_dir: Option<PathBuf>,
    db: Option<String>,
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
        BackupCommand::Reclaim(args) => {
            run_reclaim(&lastdb_node::host::resolve_home(data_dir)?, &args)
        }
        BackupCommand::IndexLogHistory(args) => {
            run_index_history(&lastdb_node::host::resolve_home(data_dir)?, &args)
        }
        BackupCommand::Snapshot { database, wait } => {
            let db = database.or(db).unwrap_or_else(|| "personal".to_owned());
            queue_snapshot(data_dir, &db, wait)
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

fn queue_snapshot(data_dir: Option<PathBuf>, database: &str, wait: bool) -> Result<(), String> {
    let (_home, socket) = resolve_client_home_and_socket(data_dir)?;
    require_reachable(&socket)?;
    let raw = post_json_admin(
        &socket,
        "/api/backup/snapshot",
        &serde_json::json!({ "database": database }),
    )?;
    let reply = parse_json_or_error(&raw, "backup snapshot")?;
    let id = reply["database_id"]
        .as_str()
        .filter(|s| !s.is_empty())
        .ok_or("backup snapshot response missing database_id")?;
    let nonce = reply["nonce"]
        .as_str()
        .filter(|s| !s.is_empty())
        .ok_or("backup snapshot response missing nonce")?;
    if wait {
        return wait_until_head(&socket, id, nonce);
    }
    println!("Queued signed snapshot for {id}");
    Ok(())
}

fn run_index_history(home: &Path, args: &BackupIndexLogHistoryArgs) -> Result<(), String> {
    if args.status == args.batches.is_some() {
        return Err("choose --status or --batches".into());
    }
    let report = block_on(
        "backup index-log-history",
        index_history(home, &args.database, args.batches, progress),
    )?;
    if args.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&report).map_err(|_| "cannot encode index status")?
        );
        return Ok(());
    }
    println!(
        "Database {} ({})",
        report.database_id.as_str(),
        report.environment
    );
    println!("Cloud history indexing enabled: {}", report.index_enabled);
    println!("Database history eligible: {}", report.eligible);
    println!("First indexed delivery: {}", first(&report));
    if args.batches.is_some() {
        println!(
            "Confirmed {} batches and {} indexed entries",
            report.batches,
            report.indexed_entries.get()
        );
    }
    Ok(())
}

fn progress(report: &IndexHistoryReport) {
    eprintln!(
        "Confirmed {} batches and {} indexed entries. First indexed delivery is {}",
        report.batches,
        report.indexed_entries.get(),
        first(report),
    );
}

fn first(report: &IndexHistoryReport) -> String {
    report
        .first_indexed_delivery
        .map_or_else(|| "none".into(), |value| value.get().to_string())
}

fn run_reclaim(home: &Path, args: &BackupReclaimArgs) -> Result<(), String> {
    if args.status == args.allow_required_client_upgrade {
        return Err("choose --status or --allow-required-client-upgrade".into());
    }
    let batches = args
        .allow_required_client_upgrade
        .then(|| args.batches.unwrap_or(16));
    let expected = args
        .expect_snapshot
        .clone()
        .map(Digest::try_from)
        .transpose()
        .map_err(|_| "--expect-snapshot requires a lowercase SHA-256 digest")?;
    let report = block_on(
        "backup reclaim",
        reclaim_snapshot_log(
            home,
            &args.database,
            batches,
            expected.as_ref(),
            args.steps_per_request,
        ),
    )?;
    if args.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&report).map_err(|_| "cannot encode reclaim status")?
        );
        return Ok(());
    }
    let snapshot = report.snapshot_id.as_ref().map_or("none", |id| id.as_str());
    println!(
        "Database {} ({})",
        report.database_id.as_str(),
        report.environment
    );
    println!("Cloud reclamation enabled: {}", report.reclaim_enabled);
    println!("Database history eligible: {}", report.eligible);
    println!("Recovery snapshot: {snapshot}");
    println!("Snapshot covers through {}", report.snapshot_through.get());
    println!(
        "Cloud log reclaimed through {}",
        report.reclaimed_through.get()
    );
    if args.allow_required_client_upgrade {
        println!("Reclaim requests confirmed: {}", report.batches);
        if let Some(state) = report.provider_cleanup {
            println!("Retired snapshot cleanup: {state}");
        }
    }
    Ok(())
}

pub(super) fn print_backup(value: &Value, json: bool) -> Result<(), String> {
    if !json {
        if let (Some(through), Some(replayed)) = (
            value["snapshot_through"].as_u64(),
            value["replayed"].as_u64(),
        ) {
            println!("snapshot_through={through} replayed={replayed}");
        }
        println!("Signed backup");
    }
    println!(
        "{}",
        serde_json::to_string_pretty(value).map_err(|_| "cannot encode backup result")?
    );
    Ok(())
}

fn wait_until_head(socket: &Path, database: &str, nonce: &str) -> Result<(), String> {
    let path = format!("/api/backup/snapshot?database={database}&nonce={nonce}");
    let deadline = Instant::now() + SNAPSHOT_WAIT;
    loop {
        match poll_status(socket, &path) {
            Poll::Ready(through, entries) => {
                println!("snapshot_through={through}");
                println!("log_head={through}");
                println!("log_entries={entries}");
                return Ok(());
            }
            Poll::Failed(err) => return Err(err),
            Poll::Pending if Instant::now() >= deadline => {
                return Err("signed snapshot did not reach the log head".into());
            }
            Poll::Pending => thread::sleep(Duration::from_millis(200)),
        }
    }
}

fn poll_status(socket: &Path, path: &str) -> Poll {
    let Ok(raw) = get_path_retry(socket, path, Duration::from_secs(10)) else {
        return Poll::Pending;
    };
    let Ok(reply) = parse_json_response(&raw, "backup snapshot status") else {
        return Poll::Pending;
    };
    if reply["ready"].as_bool() == Some(true) {
        if let (Some(through), Some(entries)) = (
            reply["snapshot_through"].as_u64(),
            reply["log_entries"].as_u64(),
        ) {
            return Poll::Ready(through, entries);
        }
    }
    if let Some(err) = reply["error"].as_str().filter(|s| !s.is_empty()) {
        return Poll::Failed(err.to_owned());
    }
    Poll::Pending
}

fn read_stdin_no_control(limit: usize, label: &str) -> Result<Zeroizing<String>, String> {
    let text = read_stdin_secret(limit, label)?;
    (!text.chars().any(char::is_control))
        .then_some(text)
        .ok_or_else(|| format!("{label} is invalid"))
}

fn read_stdin_secret(limit: usize, label: &str) -> Result<Zeroizing<String>, String> {
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
