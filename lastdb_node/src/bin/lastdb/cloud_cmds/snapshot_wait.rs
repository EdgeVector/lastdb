//! Wait until the daemon's signed snapshot read matches the log head.

use serde_json::Value;

use std::path::Path;
use std::thread;
use std::time::{Duration, Instant};

const WAIT: Duration = Duration::from_secs(70);

pub(super) fn print_backup(value: &Value, json: bool) -> Result<(), String> {
    if let (Some(through), Some(replayed)) = (
        value["snapshot_through"].as_u64(),
        value["replayed"].as_u64(),
    ) {
        println!("snapshot_through={through} replayed={replayed}");
    }
    if !json {
        println!("V2 backup");
    }
    println!(
        "{}",
        serde_json::to_string_pretty(value).map_err(|_| "cannot encode V2 result")?
    );
    Ok(())
}

pub(super) fn parse_reply(raw: &str, label: &str) -> Result<Value, String> {
    match crate::parse_json_response(raw, label) {
        Ok(value) => Ok(value),
        Err(fallback) => {
            let msg = raw
                .split_once("\r\n\r\n")
                .and_then(|(_, body)| serde_json::from_str::<Value>(body.trim()).ok())
                .and_then(|v| v.get("error").and_then(Value::as_str).map(str::to_owned));
            Err(msg.unwrap_or(fallback))
        }
    }
}

pub(super) fn until_head(socket: &Path, database: &str, nonce: &str) -> Result<(), String> {
    let path = format!("/api/backup/snapshot?database={database}&nonce={nonce}");
    let deadline = Instant::now() + WAIT;
    loop {
        if let Some((through, entries)) = poll_match(socket, &path) {
            println!("snapshot_through={through}");
            println!("log_head={through}");
            println!("log_entries={entries}");
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err("signed snapshot did not reach the log head".into());
        }
        thread::sleep(Duration::from_millis(200));
    }
}

fn poll_match(socket: &Path, path: &str) -> Option<(u64, u64)> {
    let raw = crate::get_path_retry(socket, path, Duration::from_secs(10)).ok()?;
    let reply = crate::parse_json_response(&raw, "backup snapshot status").ok()?;
    (reply["ready"].as_bool() == Some(true))
        .then(|| {
            Some((
                reply["snapshot_through"].as_u64()?,
                reply["log_entries"].as_u64()?,
            ))
        })
        .flatten()
}
