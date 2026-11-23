//! `POST /api/databases/{id}/change`: the proposal document read as one
//! `/v1/write` item, for the direct path.

use fold_db::schema::types::KeyValue;
use serde_json::{json, Value};

use super::write::{run, Alternates, FileInput, Op, WriteItem};
use super::{bad, require_write};
use crate::exec::reply::HostError;
use crate::host::Host;

/// One change to one record, parsed from a proposal document for the direct path.
pub(in crate::exec) struct Change(WriteItem);

/// Read the proposal document of `POST /api/databases/{id}/change` as one
/// `/v1/write` item of `database`. This is the direct path's reading of the
/// document: `expected` becomes the revision a replace or delete names, and a
/// create expects the key absent. A file is refused, as the proposal path does.
pub(in crate::exec) fn parse_change(body: &[u8], database: &str) -> Result<Change, HostError> {
    let Ok(Value::Object(mut doc)) = serde_json::from_slice::<Value>(body) else {
        return Err(bad("the change document must be a JSON object"));
    };
    if doc.get("file").is_some_and(|file| !file.is_null()) {
        return Err(bad("the change call does not take a file"));
    }
    let mutation_type = doc.get("mutation_type").and_then(Value::as_str);
    let op = match mutation_type {
        Some("create") => Op::Create,
        Some("replace") => Op::Replace,
        Some("delete") => Op::Delete,
        _ => return Err(bad("mutation_type must be create, replace or delete")),
    };
    let schema = match doc.remove("schema") {
        Some(Value::String(schema)) if !schema.is_empty() => schema,
        _ => return Err(bad("schema is required")),
    };
    let key: KeyValue = serde_json::from_value(doc.remove("key_value").unwrap_or(Value::Null))
        .map_err(|_| bad("key_value must be an object with hash and range"))?;
    let expected = doc.remove("expected").unwrap_or(Value::Null);
    let revision = match (op, expected.get("type").and_then(Value::as_str)) {
        (Op::Create, Some("absent")) => None,
        (Op::Replace | Op::Delete, Some("revision")) => Some(
            expected
                .get("revision")
                .and_then(Value::as_str)
                .ok_or_else(|| bad("expected.revision must be a string"))?
                .to_owned(),
        ),
        _ => {
            return Err(bad(
                "a create expects absent; a replace or delete expects a revision",
            ))
        }
    };
    let record = match (op, doc.remove("record")) {
        (Op::Delete, _) => None,
        (_, Some(Value::Object(map))) => Some(map.into_iter().collect()),
        _ => return Err(bad("a create or replace carries a record")),
    };
    // A delete names no alternates: the node reads the live record's addresses.
    let alternates = if op == Op::Delete {
        // The proposal path refuses alternates on a delete, so the direct path does too.
        let named = doc.get("alternates");
        if named.is_some_and(|v| !v.is_null() && v.as_object().is_none_or(|m| !m.is_empty())) {
            return Err(bad("a delete names no alternates"));
        }
        Alternates::new()
    } else {
        serde_json::from_value(doc.remove("alternates").unwrap_or(json!({})))
            .map_err(|_| bad("alternates must map a name to a key or null"))?
    };
    Ok(Change(WriteItem {
        database: Some(database.to_owned()),
        schema,
        op,
        key,
        record,
        alternates,
        revision,
        file: FileInput::Absent,
    }))
}

/// On the app socket the calling app must hold a write grant for the schema a
/// change document names, whichever path the node takes. A document the node
/// cannot read is left to the path that runs, which refuses it.
pub(in crate::exec) fn require_change_grant(body: &[u8]) -> Result<(), HostError> {
    match serde_json::from_slice::<Value>(body) {
        Ok(doc) => match doc.get("schema").and_then(Value::as_str) {
            Some(schema) => require_write(schema),
            None => Ok(()),
        },
        Err(_) => Ok(()),
    }
}

/// Commit one parsed change through the same path as `/v1/write`.
pub(in crate::exec) async fn apply_change(host: &Host, change: Change) -> Result<(), HostError> {
    run(host, vec![change.0]).await.map(|_| ())
}
