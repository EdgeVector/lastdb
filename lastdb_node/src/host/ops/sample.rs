//! The change in the counts since the previous history sample. Each row keeps a mark: its
//! counts at the last sample, and the slowest request since then. A sample reports the rows
//! that changed and moves their marks forward, so one sample holds only the requests that
//! completed after the sample before it.

use std::sync::PoisonError;

use serde_json::{json, Map, Value};

use super::{view, CallRow, Ops, SchemaRow, CODES};

/// The counts of one call row at the last sample, and its slowest request since then.
#[derive(Default)]
pub(super) struct CallMark {
    count: u64,
    items: u64,
    total_ns: u64,
    refused: [u64; 8],
    pub(super) recent_slowest_ns: u64,
}

impl Ops {
    /// The requests completed since the previous call, in the shape of `GET /api/ops`. Only
    /// rows with a new request appear. `slowest_ns` of a call row and of a write phase is the
    /// slowest single value since the previous call. `write_phases` is null when no write
    /// committed since then.
    pub(crate) fn sample(&self) -> Value {
        let mut tables = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        let calls: Vec<Value> = tables.calls.iter_mut().filter_map(call_change).collect();
        let schemas: Vec<Value> = tables
            .schemas
            .iter_mut()
            .filter_map(schema_change)
            .collect();
        drop(tables);
        json!({
            "calls": calls,
            "schemas": schemas,
            "write_phases": self.writes.sample(),
        })
    }
}

fn call_change(row: &mut CallRow) -> Option<Value> {
    let count = row.count.saturating_sub(row.mark.count);
    if count == 0 {
        return None;
    }
    let mut refused = Map::new();
    for (index, (now, then)) in row.refused.iter().zip(row.mark.refused).enumerate() {
        let added = now.saturating_sub(then);
        if added > 0 {
            refused.insert(CODES[index].to_owned(), json!(added));
        }
    }
    let value = json!({
        "caller": row.caller.as_str(),
        "client": view::text_or_null(row.client.as_str()),
        "call": row.call.as_str(),
        "count": count,
        "total_ns": row.total_ns.saturating_sub(row.mark.total_ns),
        "slowest_ns": row.mark.recent_slowest_ns,
        "items": row.items.saturating_sub(row.mark.items),
        "refused": Value::Object(refused),
    });
    row.mark = CallMark {
        count: row.count,
        items: row.items,
        total_ns: row.total_ns,
        refused: row.refused,
        recent_slowest_ns: 0,
    };
    Some(value)
}

fn schema_change(row: &mut SchemaRow) -> Option<Value> {
    let items = row.items.saturating_sub(row.mark.0);
    if items == 0 {
        return None;
    }
    let value = json!({
        "caller": row.caller.as_str(),
        "client": view::text_or_null(row.client.as_str()),
        "schema": row.schema.as_str(),
        "op": row.op.as_str(),
        "items": items,
        "total_ns": row.total_ns.saturating_sub(row.mark.1),
    });
    row.mark = (row.items, row.total_ns);
    Some(value)
}
