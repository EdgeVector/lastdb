//! `POST /api/databases/{id}/change`: the proposal document read as one
//! `/v1/write` item, for the direct path.

use std::collections::HashMap;

use fold_db::schema::types::KeyValue;
use serde::Deserialize;
use serde_json::Value;

use super::write::{run, Alternates, FileInput, Op, WriteItem};
use super::{bad, require_write};
use crate::exec::reply::HostError;
use crate::host::Host;

/// One change to one record, parsed from a proposal document for the direct path.
pub(in crate::exec) struct Change(WriteItem);

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ChangeDoc {
    #[serde(rename = "type")]
    kind: String,
    schema: String,
    key_value: KeyValue,
    mutation_type: String,
    expected: ExpectedDoc,
    #[serde(default)]
    record: Option<HashMap<String, Value>>,
    #[serde(default)]
    alternates: Option<Alternates>,
    #[serde(default)]
    file: Option<Value>,
    #[serde(default)]
    definition_id: Option<String>,
    #[serde(default)]
    convergence: Option<String>,
    #[serde(default)]
    durability: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ExpectedDoc {
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    revision: Option<String>,
}

/// Read the proposal document of `POST /api/databases/{id}/change` as one
/// `/v1/write` item of `database`. This is the direct path's reading of the
/// document: `expected` becomes the revision a replace or delete names, and a
/// create expects the key absent. A file is refused, as the proposal path does.
pub(in crate::exec) fn parse_change(body: &[u8], database: &str) -> Result<Change, HostError> {
    let doc: ChangeDoc = serde_json::from_slice(body)
        .map_err(|error| bad(format!("invalid change document: {error}")))?;
    if doc.kind != "mutation" {
        return Err(bad("type must be mutation"));
    }
    if doc.file.is_some() {
        return Err(bad("the change call does not take a file"));
    }
    if doc.definition_id.as_deref().is_some_and(str::is_empty) {
        return Err(bad("definition_id must not be empty"));
    }
    if doc
        .convergence
        .as_deref()
        .is_some_and(|c| !matches!(c, "sync" | "async"))
    {
        return Err(bad("convergence must be sync or async"));
    }
    if doc
        .durability
        .as_deref()
        .is_some_and(|d| !matches!(d, "queued" | "durable"))
    {
        return Err(bad("durability must be queued or durable"));
    }
    let op = match doc.mutation_type.as_str() {
        "create" => Op::Create,
        "replace" => Op::Replace,
        "delete" => Op::Delete,
        _ => return Err(bad("mutation_type must be create, replace or delete")),
    };
    if doc.schema.is_empty() || doc.schema.len() > 128 {
        return Err(bad("schema is required"));
    }
    if doc.key_value.hash.as_deref().is_none_or(str::is_empty) {
        return Err(bad("key_value.hash is required"));
    }
    let revision = parse_expected(op, doc.expected)?;
    let record = match (op, doc.record) {
        (Op::Delete, _) => None,
        (_, Some(map)) => Some(map),
        _ => return Err(bad("a create or replace carries a record")),
    };
    // A delete names no alternates: the node reads the live record's addresses.
    let alternates = if op == Op::Delete {
        if doc
            .alternates
            .as_ref()
            .is_some_and(|named| !named.is_empty())
        {
            return Err(bad("a delete names no alternates"));
        }
        Alternates::new()
    } else {
        doc.alternates.unwrap_or_default()
    };
    Ok(Change(WriteItem {
        database: Some(database.to_owned()),
        schema: doc.schema,
        op,
        key: doc.key_value,
        record,
        alternates,
        revision,
        file: FileInput::Absent,
    }))
}

fn parse_expected(op: Op, expected: ExpectedDoc) -> Result<Option<String>, HostError> {
    match (op, expected.kind.as_str(), expected.revision) {
        (Op::Create, "absent", None) => Ok(None),
        (Op::Replace | Op::Delete, "revision", Some(revision)) => Ok(Some(revision)),
        (Op::Replace | Op::Delete, "revision", None) => {
            Err(bad("expected.revision must be a string"))
        }
        _ => Err(bad(
            "a create expects absent; a replace or delete expects a revision",
        )),
    }
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
