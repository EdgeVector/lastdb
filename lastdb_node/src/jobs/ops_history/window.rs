//! Sum the history lines of one window. The lines hold changes, not running totals, so the
//! sum crosses daemon restarts: each daemon's first line counts from its start.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use serde_json::{json, Map, Value};

use super::{store, DIR};

/// A history file larger than this is skipped. The writer starts a new file at 4 MiB.
const READ_LIMIT: usize = 16 << 20;

/// The requests of the lines written in the `since_secs` seconds up to `now`, from the history
/// files in `home`, in the shape of `GET /api/ops`. A line covers the minute before it, so the
/// window starts at the start of its first line. `slowest_ns` is the slowest single value in
/// the window's lines; `mean_ns` is `total_ns / count`. `window` says what the lines cover.
pub fn window(home: &Path, since_secs: u64, now: u64) -> Result<Value, String> {
    let dir = home.join(DIR);
    let found = store::files(&dir).map_err(|error| {
        format!(
            "cannot read the ops history in {}: {error} (a daemon writes it once a minute)",
            dir.display()
        )
    })?;
    let cutoff = now.saturating_sub(since_secs);
    let mut sum = Sum::default();
    for file in &found {
        // A file holds only lines from its own UTC day, so every line in it is before `day_end`.
        let day_end = (file.start / store::DAY_SECS)
            .saturating_add(1)
            .saturating_mul(store::DAY_SECS);
        if day_end <= cutoff {
            continue;
        }
        let Ok(Some(bytes)) =
            crate::profile::private_fs::read_file_limited(&file.path, READ_LIMIT, false)
        else {
            sum.skipped_files += 1;
            continue;
        };
        for line in bytes
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
        {
            let Ok(value) = serde_json::from_slice::<Value>(line) else {
                sum.skipped_lines += 1;
                continue;
            };
            let ts = number(&value, "ts");
            if ts > cutoff && ts <= now {
                sum.add(&value, ts);
            }
        }
    }
    Ok(sum.value(since_secs))
}

#[derive(Default)]
struct CallSum {
    count: u64,
    items: u64,
    total_ns: u64,
    slowest_ns: u64,
    refused: BTreeMap<String, u64>,
}

#[derive(Default)]
struct Sum {
    calls: BTreeMap<(String, String, String), CallSum>,
    schemas: BTreeMap<(String, String, String, String), (u64, u64)>,
    writes: u64,
    phases: Vec<(String, u64, u64)>,
    commit_log: Option<[u64; 4]>,
    lines: u64,
    from_ts: Option<u64>,
    to_ts: u64,
    daemons: BTreeSet<(u64, u64)>,
    builds: BTreeSet<String>,
    skipped_lines: u64,
    skipped_files: u64,
}

impl Sum {
    fn add(&mut self, line: &Value, ts: u64) {
        self.lines += 1;
        let from = ts.saturating_sub(number(line, "interval_secs"));
        self.from_ts = Some(self.from_ts.map_or(from, |old| old.min(from)));
        self.to_ts = self.to_ts.max(ts);
        self.daemons
            .insert((number(line, "start_ts"), number(line, "pid")));
        if let Some(build) = line.get("build").and_then(Value::as_str) {
            self.builds.insert(build.to_owned());
        }
        for row in rows(line, "calls") {
            let key = (text(row, "caller"), text(row, "client"), text(row, "call"));
            let call = self.calls.entry(key).or_default();
            call.count += number(row, "count");
            call.items += number(row, "items");
            call.total_ns += number(row, "total_ns");
            call.slowest_ns = call.slowest_ns.max(number(row, "slowest_ns"));
            for (code, count) in row
                .get("refused")
                .and_then(Value::as_object)
                .into_iter()
                .flatten()
            {
                *call.refused.entry(code.clone()).or_default() += count.as_u64().unwrap_or(0);
            }
        }
        for row in rows(line, "schemas") {
            let key = (
                text(row, "caller"),
                text(row, "client"),
                text(row, "schema"),
                text(row, "op"),
            );
            let schema = self.schemas.entry(key).or_default();
            schema.0 += number(row, "items");
            schema.1 += number(row, "total_ns");
        }
        if let Some(phases) = line.get("write_phases").filter(|value| value.is_object()) {
            self.writes += number(phases, "writes");
            for phase in rows(phases, "phases") {
                let name = text(phase, "phase");
                let index = if let Some(index) = self.phases.iter().position(|p| p.0 == name) {
                    index
                } else {
                    self.phases.push((name, 0, 0));
                    self.phases.len() - 1
                };
                let entry = &mut self.phases[index];
                entry.1 += number(phase, "total_ns");
                entry.2 = entry.2.max(number(phase, "slowest_ns"));
            }
        }
        if let Some(log) = line.get("commit_log").filter(|value| value.is_object()) {
            let sum = self.commit_log.get_or_insert([0; 4]);
            sum[0] += number(log, "flushes");
            sum[1] += number(log, "flush_micros");
            sum[2] = sum[2].max(number(log, "memory_commits"));
            sum[3] = sum[3].max(number(log, "memory_bytes"));
        }
    }

    fn value(self, since_secs: u64) -> Value {
        let mut calls: Vec<Value> = self
            .calls
            .into_iter()
            .map(|((caller, client, call), sum)| {
                json!({
                    "caller": caller,
                    "client": text_or_null(client),
                    "call": call,
                    "count": sum.count,
                    "items": sum.items,
                    "total_ns": sum.total_ns,
                    "mean_ns": sum.total_ns.checked_div(sum.count).unwrap_or(0),
                    "slowest_ns": sum.slowest_ns,
                    "refused": sum
                        .refused
                        .into_iter()
                        .map(|(code, count)| (code, json!(count)))
                        .collect::<Map<String, Value>>(),
                })
            })
            .collect();
        calls.sort_by(|left, right| {
            number(right, "total_ns")
                .cmp(&number(left, "total_ns"))
                .then(number(right, "count").cmp(&number(left, "count")))
                .then(str_field(left, "caller").cmp(str_field(right, "caller")))
                .then(str_field(left, "client").cmp(str_field(right, "client")))
                .then(str_field(left, "call").cmp(str_field(right, "call")))
        });
        let mut schemas: Vec<Value> = self
            .schemas
            .into_iter()
            .map(|((caller, client, schema, op), (items, total_ns))| {
                json!({
                    "caller": caller,
                    "client": text_or_null(client),
                    "schema": schema,
                    "op": op,
                    "items": items,
                    "total_ns": total_ns,
                })
            })
            .collect();
        schemas.sort_by(|left, right| {
            number(right, "total_ns")
                .cmp(&number(left, "total_ns"))
                .then(number(right, "items").cmp(&number(left, "items")))
                .then(str_field(left, "caller").cmp(str_field(right, "caller")))
                .then(str_field(left, "client").cmp(str_field(right, "client")))
                .then(str_field(left, "schema").cmp(str_field(right, "schema")))
                .then(str_field(left, "op").cmp(str_field(right, "op")))
        });
        let phases: Vec<Value> = self
            .phases
            .into_iter()
            .map(|(phase, total_ns, slowest_ns)| {
                json!({ "phase": phase, "total_ns": total_ns, "slowest_ns": slowest_ns })
            })
            .collect();
        let commit_log = self.commit_log.map(|[flushes, micros, commits, bytes]| {
            json!({
                "flushes": flushes,
                "flush_micros": micros,
                "mean_flush_micros": micros.checked_div(flushes).unwrap_or(0),
                "max_memory_commits": commits,
                "max_memory_bytes": bytes,
            })
        });
        json!({
            "window": {
                "since_secs": since_secs,
                "from_ts": self.from_ts,
                "to_ts": self.from_ts.map(|_| self.to_ts),
                "lines": self.lines,
                "daemons": self.daemons.len(),
                "builds": self.builds,
                "skipped_lines": self.skipped_lines,
                "skipped_files": self.skipped_files,
            },
            "calls": calls,
            "schemas": schemas,
            "write_phases": { "writes": self.writes, "phases": phases },
            "commit_log": commit_log,
        })
    }
}

fn rows<'a>(value: &'a Value, key: &str) -> impl Iterator<Item = &'a Value> {
    value
        .get(key)
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
}

fn number(value: &Value, key: &str) -> u64 {
    value.get(key).and_then(Value::as_u64).unwrap_or(0)
}

fn str_field<'a>(value: &'a Value, key: &str) -> &'a str {
    value.get(key).and_then(Value::as_str).unwrap_or("")
}

fn text(value: &Value, key: &str) -> String {
    str_field(value, key).to_owned()
}

fn text_or_null(value: String) -> Value {
    if value.is_empty() {
        Value::Null
    } else {
        Value::String(value)
    }
}
