//! Record mutation submission (`lastdb mutate`).
//!
//! The request file is one conditional mutation: a create that expects the
//! key to be absent, or a replace or delete at a revision. A file, when
//! present, is a local path. The node commits the mutation on `POST /v1/write`.
//! A received database gets that route's refusal, `a received database is read
//! only`. This command does not submit a share proposal. A proposal is
//! `POST /api/databases/{id}/submissions` on the owner socket.

use super::v1_records;
use super::*;
use lastdb_node::lastdb_uds::handler_timeout;
use serde::Deserialize;
use serde_json::{Map, Value};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::Duration;
use uuid::Uuid;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RecordInput {
    mutation: Value,
    file: InputFile,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct InputFile {
    source: PathBuf,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    media_type: Option<String>,
    #[serde(default)]
    local: Option<bool>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MutationDoc {
    #[serde(rename = "type")]
    kind: String,
    schema: String,
    record: Map<String, Value>,
    key_value: Value,
    #[serde(default)]
    alternates: Map<String, Value>,
    mutation_type: String,
    expected: Expected,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Expected {
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    revision: Option<String>,
}

pub(super) fn mutate_command(
    target: &DatabaseTarget,
    request_file: &Path,
    durable: bool,
    mutation_id: Option<Uuid>,
) -> Result<(), String> {
    if mutation_id.is_some() && target.selector.is_none() {
        return Err("--mutation-id requires --db".into());
    }
    let body = read_mutation_json(request_file)?;
    let (doc, link) = take_input(request_file, body)?;
    let item = v1_records::record_write(&doc, v1_records::database(target), link.as_ref())?;
    let timeout = if durable {
        handler_timeout().saturating_add(Duration::from_secs(20))
    } else {
        Duration::from_secs(10)
    };
    let reply = v1_records::write_mutations(target, &item, timeout).map_err(|error| {
        mutation_id.map_or(error.clone(), |id| format!("{error} (mutation ID {id})"))
    })?;
    println!("{}", v1_records::receipt(&reply, link.is_some()));
    Ok(())
}

fn read_mutation_json(request_file: &Path) -> Result<Value, String> {
    let text = if request_file == Path::new("-") {
        let mut s = String::new();
        std::io::stdin()
            .lock()
            .read_to_string(&mut s)
            .map_err(|e| format!("read mutation: {e}"))?;
        s
    } else {
        std::fs::read_to_string(request_file).map_err(|e| format!("read mutation: {e}"))?
    };
    serde_json::from_str(&text).map_err(|e| format!("invalid mutation JSON: {e}"))
}

fn take_input(
    request_file: &Path,
    body: Value,
) -> Result<(Value, Option<v1_records::LinkedFile>), String> {
    if body.get("mutation").is_none() {
        let doc: MutationDoc =
            serde_json::from_value(body).map_err(|e| format!("invalid complete mutation: {e}"))?;
        return Ok((doc_value(&doc)?, None));
    }
    let input: RecordInput =
        serde_json::from_value(body).map_err(|e| format!("invalid record input: {e}"))?;
    if input.file.local == Some(false) {
        return Err("a file is named by a path".into());
    }
    let doc: MutationDoc = serde_json::from_value(input.mutation)
        .map_err(|e| format!("invalid complete mutation: {e}"))?;
    let base = request_file.parent().unwrap_or(Path::new(""));
    let source = base.join(&input.file.source);
    let canonical =
        std::fs::canonicalize(&source).map_err(|e| format!("open {}: {e}", source.display()))?;
    let path = canonical
        .to_str()
        .ok_or_else(|| format!("open {}: path is not utf-8", source.display()))?
        .to_owned();
    let link = v1_records::LinkedFile {
        path,
        name: input.file.name,
        media_type: input.file.media_type,
    };
    Ok((doc_value(&doc)?, Some(link)))
}

fn doc_value(doc: &MutationDoc) -> Result<Value, String> {
    if doc.kind != "mutation" {
        return Err("invalid complete mutation".into());
    }
    Ok(serde_json::json!({
        "type": doc.kind,
        "schema": doc.schema,
        "record": doc.record,
        "key_value": doc.key_value,
        "alternates": doc.alternates,
        "mutation_type": doc.mutation_type,
        "expected": { "type": doc.expected.kind, "revision": doc.expected.revision },
    }))
}
