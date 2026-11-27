//! The commit log line of `lastdb status`, from `/api/status`, and the warm set
//! line, from `/api/db/native-status`.

use std::path::Path;
use std::time::Duration;

use serde_json::{json, Value};

use super::snapshot_line::size;
use super::uds_client::{get_path_retry, parse_json_response};

pub(crate) fn commit_log_line(log: &Value) -> Option<String> {
    let n = |k: &str| log[k].as_u64();
    Some(format!(
        "Commit log: {} flushes, mean {} µs, slowest {} µs; {} commits ({} bytes) not yet in a run",
        n("flushes")?,
        n("mean_flush_micros")?,
        n("slowest_flush_micros")?,
        n("memory_commits")?,
        n("memory_bytes")?
    ))
}

/// The warm set counters from `/api/db/native-status`. `hits` and `misses`
/// count record fetches since the daemon started: a hit is answered from
/// memory, a miss from disk. `None` when the daemon is not serving.
pub(crate) fn warm_set(running: bool, socket: &Path, budget: Duration) -> Option<Value> {
    if !running {
        return None;
    }
    let raw = get_path_retry(socket, "/api/db/native-status", budget).ok()?;
    let native = parse_json_response(&raw, "native status").ok()?;
    let disk = native.get("native_disk")?;
    let n = |k: &str| disk[k].as_u64();
    let bytes = |k: &str| disk["core_warm_bytes"][k].as_u64();
    Some(json!({
        "records": n("warm_records")?,
        "used_bytes": bytes("used_bytes")?,
        "limit_bytes": bytes("limit_bytes")?,
        "hits": n("core_warm_hits")?,
        "misses": n("core_warm_misses")?,
        "purged": n("core_warm_purged")?,
    }))
}

pub(crate) fn warm_set_line(warm: &Value) -> Option<String> {
    let n = |k: &str| warm[k].as_u64();
    let (hits, misses) = (n("hits")?, n("misses")?);
    let fetches = hits.saturating_add(misses);
    let share = if fetches == 0 {
        String::from("no fetches yet")
    } else {
        format!("{:.1}% from memory", hits as f64 * 100.0 / fetches as f64)
    };
    Some(format!(
        "Warm set: {} records, {} of {}; {hits} hits, {misses} misses ({share}); {} purged",
        n("records")?,
        size(n("used_bytes")?),
        size(n("limit_bytes")?),
        n("purged")?
    ))
}
