//! Name the mutation a refused `/v1/write` failed on.
//!
//! The engine's `cas_conflict` carries the expectation and the revision it
//! found. A create expects `<absent>` and conflicts only when a revision is
//! there (`exists`). A replace or a delete expects a revision: no revision
//! means `absent`, and a different revision means `revision`. The batch holds
//! each record once, so the schema and key select the mutation. No extra read
//! is added to tell those apart.

use fold_db::schema::types::KeyValue;
use serde_json::Value;

use crate::exec::reply::HostError;

const EXPECTED_ABSENT: &str = "<absent>";

pub(super) const EXISTS: &str = "exists";
pub(super) const ABSENT: &str = "absent";
pub(super) const REVISION: &str = "revision";

/// The schema and storage key of one mutation, in request order.
pub(super) struct WrittenKey {
    pub schema: String,
    pub key: String,
}

/// Replace an engine compare-and-set refusal with the app API's conflict.
/// Any other error is returned as it arrived.
pub(super) fn name_conflict(error: HostError, written: &[WrittenKey]) -> HostError {
    if error.status != 409 {
        return error;
    }
    let Ok(body) = serde_json::from_str::<Value>(&error.message) else {
        return error;
    };
    if body.get("error").and_then(Value::as_str) != Some("cas_conflict") {
        return error;
    }
    let Some(reason) = conflict_reason(&body) else {
        return HostError::new(409, "the condition for this write does not hold");
    };
    match conflict_index(&body, written) {
        Some(index) => conflict_error(index, reason),
        None => HostError::new(409, sentence(reason)).with_reason(reason),
    }
}

pub(super) fn conflict_error(index: usize, reason: &'static str) -> HostError {
    let sentence = sentence(reason);
    HostError::new(409, format!("mutation {index}: {sentence}"))
        .at(index)
        .with_reason(reason)
}

fn sentence(reason: &str) -> &'static str {
    match reason {
        EXISTS => "a record already holds the key",
        ABSENT => "no record holds the key",
        _ => "the record is at another revision",
    }
}

/// `expected` and `actual` are enough. `<absent>` with a revision is a create
/// that found a record. A null `actual` is a replace or delete that found
/// none. Any other found revision is a different revision.
fn conflict_reason(body: &Value) -> Option<&'static str> {
    let expected = body.get("expected").and_then(Value::as_str)?;
    match body.get("actual")? {
        Value::String(_) if expected == EXPECTED_ABSENT => Some(EXISTS),
        Value::Null if expected != EXPECTED_ABSENT => Some(ABSENT),
        Value::String(found) if expected != EXPECTED_ABSENT && found != expected => Some(REVISION),
        _ => None,
    }
}

fn conflict_index(body: &Value, written: &[WrittenKey]) -> Option<usize> {
    let schema = body.get("schema").and_then(Value::as_str)?;
    let key = body.get("key").and_then(Value::as_str)?;
    written
        .iter()
        .position(|item| item.schema == schema && item.key == key)
        .or_else(|| unique_hash(schema, key, written))
}

/// The engine stores the canonical range. When that string differs from the
/// request and only one mutation in the schema has this hash, it is that one.
fn unique_hash(schema: &str, key: &str, written: &[WrittenKey]) -> Option<usize> {
    let parsed: KeyValue = serde_json::from_str(key).ok()?;
    let mut found = None;
    for (index, item) in written.iter().enumerate() {
        if item.schema != schema {
            continue;
        }
        let Ok(item_key) = serde_json::from_str::<KeyValue>(&item.key) else {
            continue;
        };
        if item_key.hash != parsed.hash {
            continue;
        }
        if found.is_some() {
            return None;
        }
        found = Some(index);
    }
    found
}
