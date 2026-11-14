//! Wait until the daemon's signed snapshot read matches the log head.

use serde_json::Value;

use std::path::Path;
use std::thread;
use std::time::{Duration, Instant};

const WAIT: Duration = Duration::from_secs(70);

enum Poll {
    Pending,
    Ready(u64, u64),
    Failed(String),
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

pub(super) fn parse_reply(raw: &str, label: &str) -> Result<Value, String> {
    crate::parse_json_response(raw, label).map_err(|fallback| {
        raw.split_once("\r\n\r\n")
            .and_then(|(_, body)| serde_json::from_str::<Value>(body.trim()).ok())
            .and_then(|v| v.get("error").and_then(Value::as_str).map(str::to_owned))
            .unwrap_or(fallback)
    })
}

pub(super) fn until_head(socket: &Path, database: &str, nonce: &str) -> Result<(), String> {
    let path = format!("/api/backup/snapshot?database={database}&nonce={nonce}");
    let deadline = Instant::now() + WAIT;
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
    let Ok(raw) = crate::get_path_retry(socket, path, Duration::from_secs(10)) else {
        return Poll::Pending;
    };
    let Ok(reply) = crate::parse_json_response(&raw, "backup snapshot status") else {
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
