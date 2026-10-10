//! Print the events of a reap run, as text or as JSON lines.
//!
//! The count line of each collection has exactly these fields:
//! `collection groups keys_scanned matched_keys matched_bytes expect_keys
//! expect_bytes ok`. Every other JSON line has an `event` field.

use laststore::{CollectionApplied, CollectionCount, ReapError, ReapEvent, ReapOutcome};
use serde_json::{json, Value};

pub(super) struct Printer {
    pub json: bool,
}

fn count_json(count: &CollectionCount) -> Value {
    json!({
        "collection": count.collection,
        "groups": count.groups,
        "keys_scanned": count.keys_scanned,
        "matched_keys": count.matched_keys,
        "matched_bytes": count.matched_bytes,
        "expect_keys": count.expect_keys,
        "expect_bytes": count.expect_bytes,
        "ok": count.ok,
    })
}

fn optional(value: Option<u64>) -> String {
    value.map_or_else(|| "-".to_string(), |value| value.to_string())
}

fn count_text(count: &CollectionCount) -> String {
    format!(
        "{}: groups={} keys_scanned={} matched_keys={} matched_bytes={} expect_keys={} expect_bytes={} ok={}",
        count.collection,
        count.groups,
        count.keys_scanned,
        count.matched_keys,
        count.matched_bytes,
        optional(count.expect_keys),
        optional(count.expect_bytes),
        count.ok
    )
}

fn applied_json(done: &CollectionApplied) -> Value {
    json!({
        "event": "collection_done",
        "collection": done.collection,
        "groups_rewritten": done.groups_rewritten,
        "dropped_keys": done.dropped_keys,
        "dropped_bytes": done.dropped_bytes,
        "bytes_before": done.bytes_before,
        "bytes_after": done.bytes_after,
    })
}

fn applied_text(done: &CollectionApplied) -> String {
    format!(
        "{}: rewrote {} groups, dropped {} keys / {} bytes",
        done.collection, done.groups_rewritten, done.dropped_keys, done.dropped_bytes
    )
}

impl Printer {
    /// Print one event while the run goes on.
    pub(super) fn event(&self, event: ReapEvent<'_>) {
        match event {
            ReapEvent::Counted(count) if self.json => println!("{}", count_json(count)),
            ReapEvent::Counted(count) => println!("{}", count_text(count)),
            ReapEvent::Applied(done) if self.json => println!("{}", applied_json(done)),
            ReapEvent::Applied(done) => println!("{}", applied_text(done)),
            ReapEvent::GroupRewritten {
                collection,
                shard,
                group,
                dropped_keys,
                bytes_before,
                bytes_after,
            } => {
                let group = group.map_or_else(|| "-".to_string(), |value| format!("{value:03x}"));
                eprintln!(
                    "reap {collection} shard={shard} group={group} drop={dropped_keys} before={bytes_before} after={bytes_after}"
                );
            }
        }
    }

    /// Print the final line of a run that finished.
    pub(super) fn summary(&self, outcome: &ReapOutcome) {
        let keys = outcome.total_keys();
        let bytes = outcome.total_bytes();
        if self.json {
            let mut line = json!({
                "event": "summary",
                "executed": outcome.executed,
                "already_applied": outcome.already_applied,
                "gate_problems": outcome.gate_problems,
            });
            let fields = line.as_object_mut().expect("object");
            if outcome.executed {
                fields.insert("dropped_keys".into(), json!(keys));
                fields.insert("dropped_bytes".into(), json!(bytes));
                fields.insert("bytes_before".into(), json!(outcome.bytes_before()));
                fields.insert("bytes_after".into(), json!(outcome.bytes_after()));
            } else {
                fields.insert("would_drop_keys".into(), json!(keys));
                fields.insert("would_drop_bytes".into(), json!(bytes));
            }
            println!("{line}");
        } else if !outcome.executed {
            println!("would drop {keys} keys / {bytes} bytes");
        } else if outcome.already_applied {
            println!("already applied: dropped 0 keys / 0 bytes");
        } else {
            println!(
                "dropped {keys} keys / {bytes} bytes, bytes before {}, after {}",
                outcome.bytes_before(),
                outcome.bytes_after()
            );
        }
    }

    /// Print an error. The text goes to stderr. A JSON run also prints a line.
    pub(super) fn error(&self, error: &ReapError) {
        eprintln!("reap failed: {error}");
        if self.json {
            let line = json!({
                "event": "error",
                "exit_code": error.exit_code(),
                "message": error.to_string(),
            });
            println!("{line}");
        }
    }
}
