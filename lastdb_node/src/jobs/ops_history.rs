//! The node's request history. Once a minute the daemon appends one JSON line to a file in
//! `<home>/ops-history/`: the requests completed since the line before, in the shape of
//! `GET /api/ops`. `lastdb ops --since` reads the files and sums a window.
//!
//! A line holds `ts` (unix seconds), `pid`, `start_ts` (the daemon's start), `uptime_secs`,
//! `interval_secs` (the time since the previous line, or since the start for the first line
//! of a daemon), `build`, and the changes: `calls`, `schemas`, `write_phases` (null when no
//! write committed) and `commit_log` (null when the store has no commit log). Only rows with
//! a new request appear. `slowest_ns` is the slowest single value in that interval.
//!
//! The files are diagnostic, not data. They are written without a flush to the drive, so a
//! power cut may lose the last minutes, and the requests of the last minute before a crash
//! are not written. They are not LastDB records, and they lie outside `data/`: a snapshot
//! lists only the store's own files, so no backup, snapshot or upload holds them. `store`
//! names the caps.

mod store;
mod window;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{json, Value};
use tokio::task::JoinHandle;
use tracing::Instrument;

use crate::host::Host;

pub use window::window;

/// The folder of the history files in the node home.
pub const DIR: &str = "ops-history";
const INTERVAL: Duration = Duration::from_secs(60);

/// Append one line a minute until `shutdown`, then one last line for the time since.
pub fn spawn_periodic(host: Arc<Host>, shutdown: Arc<AtomicBool>) -> JoinHandle<()> {
    tokio::spawn(
        async move {
            let mut writer = store::Writer::new(host.home.join(DIR));
            let mut marks = Marks::default();
            loop {
                let stopping = wait(&shutdown).await;
                let (now, line) = sample_line(&host, &mut marks);
                let dir = host.home.join(DIR);
                writer = tokio::task::spawn_blocking(move || {
                    writer.append(&line, now);
                    writer
                })
                .await
                .unwrap_or_else(|_| store::Writer::new(dir));
                if stopping {
                    break;
                }
            }
        }
        .in_current_span(),
    )
}

/// Wait one interval. True when the node is stopping.
async fn wait(shutdown: &AtomicBool) -> bool {
    let until = Instant::now() + INTERVAL;
    while Instant::now() < until {
        if shutdown.load(Ordering::Acquire) {
            return true;
        }
        let rest = until.saturating_duration_since(Instant::now());
        tokio::time::sleep(Duration::from_millis(250).min(rest)).await;
    }
    shutdown.load(Ordering::Acquire)
}

/// The uptime and the commit log totals at the previous line.
#[derive(Default)]
struct Marks {
    uptime_secs: u64,
    flushes: u64,
    flush_micros: u64,
}

fn sample_line(host: &Host, marks: &mut Marks) -> (u64, Vec<u8>) {
    let now = fold_db::clock::unix_secs();
    let uptime_secs = now.saturating_sub(host.process_start_ts);
    let ops = host.ops.sample();
    let commit_log =
        host.db
            .db_ops()
            .native_runtime()
            .commit_log_stats()
            .map_or(Value::Null, |stats| {
                // A log opened again counts from zero.
                if stats.flushes < marks.flushes {
                    (marks.flushes, marks.flush_micros) = (0, 0);
                }
                let value = json!({
                    "flushes": stats.flushes - marks.flushes,
                    "flush_micros": stats.total_flush_micros.saturating_sub(marks.flush_micros),
                    "memory_commits": stats.memory_commits,
                    "memory_bytes": stats.memory_bytes,
                });
                (marks.flushes, marks.flush_micros) = (stats.flushes, stats.total_flush_micros);
                value
            });
    let value = json!({
        "ts": now,
        "pid": std::process::id(),
        "start_ts": host.process_start_ts,
        "uptime_secs": uptime_secs,
        "interval_secs": uptime_secs.saturating_sub(marks.uptime_secs),
        "build": crate::version::build_version(),
        "calls": ops.get("calls"),
        "schemas": ops.get("schemas"),
        "write_phases": ops.get("write_phases"),
        "commit_log": commit_log,
    });
    marks.uptime_secs = uptime_secs;
    let mut bytes = serde_json::to_vec(&value).unwrap_or_default();
    bytes.push(b'\n');
    (now, bytes)
}
