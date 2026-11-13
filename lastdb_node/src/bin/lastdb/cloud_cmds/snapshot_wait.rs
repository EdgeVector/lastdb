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

pub(super) fn until_head(home: &Path, database: &str) -> Result<(), String> {
    let path = home.join("log-sync").join(database).join("snapshot-result");
    let deadline = Instant::now() + WAIT;
    loop {
        if let Some((through, entries)) = read_match(&path) {
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

fn read_match(path: &Path) -> Option<(u64, u64)> {
    let text = std::fs::read_to_string(path).ok()?;
    let mut through = None;
    let mut head = None;
    let mut entries = None;
    for line in text.lines() {
        if let Some(value) = line.strip_prefix("through=") {
            through = value.parse().ok();
        } else if let Some(value) = line.strip_prefix("head=") {
            head = value.parse().ok();
        } else if let Some(value) = line.strip_prefix("log_entries=") {
            entries = value.parse().ok();
        } else if !line.is_empty() {
            return None;
        }
    }
    (through == head).then_some((through?, entries?))
}
