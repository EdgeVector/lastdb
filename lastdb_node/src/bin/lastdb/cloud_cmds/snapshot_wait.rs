//! Wait until the daemon's signed snapshot read matches the log head.

use serde_json::Value;

use std::path::Path;
use std::thread;
use std::time::{Duration, Instant};

const WAIT: Duration = Duration::from_secs(70);

const AWAIT_INCOMING: &str = "snapshot awaits incoming signed updates";
const AWAIT_LOCAL_UPLOAD: &str = "snapshot awaits publication of local signed writes";

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

pub(super) fn parse_reply(raw: &str, label: &str) -> Result<Value, String> {
    crate::parse_json_response(raw, label).map_err(|fallback| {
        raw.split_once("\r\n\r\n")
            .and_then(|(_, body)| serde_json::from_str::<Value>(body.trim()).ok())
            .and_then(|v| v.get("error").and_then(Value::as_str).map(str::to_owned))
            .unwrap_or(fallback)
    })
}

pub(super) fn until_head(
    home: &Path,
    socket: &Path,
    database: &str,
    nonce: &str,
) -> Result<(), String> {
    let path = format!("/api/backup/snapshot?database={database}&nonce={nonce}");
    let err_path = home.join("log-sync").join(database).join("snapshot-error");
    let deadline = Instant::now() + WAIT;
    loop {
        if let Some((through, entries)) = poll_match(socket, &path) {
            println!("snapshot_through={through}");
            println!("log_head={through}");
            println!("log_entries={entries}");
            return Ok(());
        }
        if let Some(err) = read_error(&err_path) {
            return Err(err);
        }
        if Instant::now() >= deadline {
            return Err("signed snapshot did not reach the log head".into());
        }
        thread::sleep(Duration::from_millis(200));
    }
}

fn read_error(path: &Path) -> Option<String> {
    let text = std::fs::read_to_string(path).ok()?;
    let trimmed = text.trim();
    if trimmed.is_empty() || trimmed == AWAIT_INCOMING || trimmed == AWAIT_LOCAL_UPLOAD {
        return None;
    }
    Some(trimmed.to_owned())
}

fn poll_match(socket: &Path, path: &str) -> Option<(u64, u64)> {
    let raw = crate::get_path_retry(socket, path, Duration::from_secs(10)).ok()?;
    let reply = crate::parse_json_response(&raw, "backup snapshot status").ok()?;
    if reply["ready"].as_bool() != Some(true) {
        return None;
    }
    Some((
        reply["snapshot_through"].as_u64()?,
        reply["log_entries"].as_u64()?,
    ))
}
