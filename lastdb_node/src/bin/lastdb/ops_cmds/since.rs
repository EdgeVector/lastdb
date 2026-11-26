//! `lastdb ops --since <duration>`: the requests of a recent window, read from the ops history
//! files in the node home. The daemon does not need to run. The window sums across restarts.

use std::io::{self, Write};
use std::path::PathBuf;

use serde_json::Value;

use super::{format_ns, print_tables};
use crate::uds_client::resolve_client_home_and_socket;

pub(super) fn ops_since(
    data_dir: Option<PathBuf>,
    window: &str,
    by_count: bool,
    json: bool,
) -> Result<(), String> {
    let since_secs = parse_duration(window)?;
    let (home, _) = resolve_client_home_and_socket(data_dir)?;
    let now = fold_db::clock::unix_secs();
    let value = lastdb_node::jobs::ops_history::window(&home, since_secs, now)?;
    if json {
        let mut out =
            serde_json::to_vec(&value).map_err(|error| format!("ops history: {error}"))?;
        out.push(b'\n');
        return io::stdout()
            .write_all(&out)
            .map_err(|error| format!("ops: {error}"));
    }
    let lines = number(&value["window"], "lines");
    print_window(&value["window"], window, lines);
    if lines == 0 {
        return Ok(());
    }
    println!();
    print_tables(&value, by_count, true)?;
    if let Some(log) = value.get("commit_log").filter(|log| log.is_object()) {
        println!();
        print_commit_log(log);
    }
    Ok(())
}

/// A count and a unit: `s`, `m`, `h` or `d`.
fn parse_duration(text: &str) -> Result<u64, String> {
    let bad = || format!("--since takes a duration such as 10m, 24h or 7d, not {text:?}");
    let unit_at = text
        .find(|ch: char| !ch.is_ascii_digit())
        .unwrap_or(text.len());
    let (digits, unit) = text.split_at(unit_at);
    let count: u64 = digits.parse().map_err(|_| bad())?;
    let scale: u64 = match unit {
        "s" => 1,
        "m" => 60,
        "h" => 3_600,
        "d" => 86_400,
        _ => return Err(bad()),
    };
    count
        .checked_mul(scale)
        .filter(|secs| *secs > 0)
        .ok_or_else(bad)
}

fn print_window(window: &Value, asked: &str, lines: u64) {
    if lines == 0 {
        println!(
            "no ops history in the last {asked}. A daemon writes one line a minute to the ops-history folder of its home."
        );
        return;
    }
    let from = time_text(number(window, "from_ts"));
    let to = time_text(number(window, "to_ts"));
    let daemons = number(window, "daemons");
    println!("window  last {asked}: {from} to {to}");
    println!("lines   {lines} one-minute lines from {daemons} daemon run(s)");
    let builds: Vec<&str> = window
        .get("builds")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .collect();
    if !builds.is_empty() {
        println!("builds  {}", builds.join(", "));
    }
    let skipped = number(window, "skipped_lines") + number(window, "skipped_files");
    if skipped > 0 {
        println!("skipped {skipped} unreadable line(s) or file(s)");
    }
    println!("note    mean is total / count; slowest is the slowest single request in the window");
}

fn print_commit_log(log: &Value) {
    let flushes = number(log, "flushes");
    let micros = number(log, "flush_micros");
    let mean = micros.checked_div(flushes).unwrap_or(0);
    println!(
        "commit log  {flushes} flushes, mean {}, total {}; memory layer peak {} commits, {} bytes",
        format_ns(mean.saturating_mul(1_000)),
        format_ns(micros.saturating_mul(1_000)),
        number(log, "max_memory_commits"),
        number(log, "max_memory_bytes"),
    );
}

fn time_text(ts: u64) -> String {
    i64::try_from(ts)
        .ok()
        .and_then(|secs| chrono::DateTime::from_timestamp(secs, 0))
        .map_or_else(
            || ts.to_string(),
            |time| time.format("%Y-%m-%d %H:%M:%SZ").to_string(),
        )
}

fn number(value: &Value, key: &str) -> u64 {
    value.get(key).and_then(Value::as_u64).unwrap_or(0)
}
