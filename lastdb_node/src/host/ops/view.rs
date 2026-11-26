//! JSON for `GET /api/ops`. Built from a copy of the rows, then sorted.

use serde_json::{json, Map, Value};

use super::{CallRow, SchemaRow, CODES};

pub(super) fn call_value(row: &CallRow) -> Value {
    let mut refused = Map::new();
    for (index, count) in row.refused.iter().copied().enumerate() {
        if count > 0 {
            refused.insert(CODES[index].to_owned(), json!(count));
        }
    }
    json!({
        "caller": row.caller.as_str(),
        "client": text_or_null(row.client.as_str()),
        "call": row.call.as_str(),
        "count": row.count,
        "total_ns": row.total_ns,
        "slowest_ns": row.slowest_ns,
        "items": row.items,
        "refused": Value::Object(refused),
    })
}

pub(super) fn schema_value(row: &SchemaRow) -> Value {
    json!({
        "caller": row.caller.as_str(),
        "client": text_or_null(row.client.as_str()),
        "schema": row.schema.as_str(),
        "op": row.op.as_str(),
        "items": row.items,
        "total_ns": row.total_ns,
    })
}

pub(super) fn sort_calls(calls: &mut [Value]) {
    calls.sort_by(|left, right| {
        number(right, "total_ns")
            .cmp(&number(left, "total_ns"))
            .then(number(right, "count").cmp(&number(left, "count")))
            .then(text(left, "caller").cmp(text(right, "caller")))
            .then(text(left, "client").cmp(text(right, "client")))
            .then(text(left, "call").cmp(text(right, "call")))
    });
}

pub(super) fn sort_schemas(schemas: &mut [Value]) {
    schemas.sort_by(|left, right| {
        number(right, "total_ns")
            .cmp(&number(left, "total_ns"))
            .then(number(right, "items").cmp(&number(left, "items")))
            .then(text(left, "caller").cmp(text(right, "caller")))
            .then(text(left, "client").cmp(text(right, "client")))
            .then(text(left, "schema").cmp(text(right, "schema")))
            .then(text(left, "op").cmp(text(right, "op")))
    });
}

pub(super) fn text_or_null(value: &str) -> Value {
    if value.is_empty() {
        Value::Null
    } else {
        Value::String(value.to_owned())
    }
}

fn number(value: &Value, key: &str) -> u64 {
    value.get(key).and_then(Value::as_u64).unwrap_or(0)
}

fn text<'a>(value: &'a Value, key: &str) -> &'a str {
    value.get(key).and_then(Value::as_str).unwrap_or("")
}
