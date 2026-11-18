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

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RecordInput {
    mutation: Value,
    /// Absent sends no file. JSON null clears a replace. An object links a path.
    #[serde(default, deserialize_with = "input_file")]
    file: InputFile,
}

#[derive(Default)]
enum InputFile {
    #[default]
    Absent,
    Clear,
    Link(FileBody),
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FileBody {
    source: PathBuf,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    media_type: Option<String>,
}

fn input_file<'de, D>(deserializer: D) -> Result<InputFile, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Ok(match Option::<FileBody>::deserialize(deserializer)? {
        None => InputFile::Clear,
        Some(body) => InputFile::Link(body),
    })
}

enum TakenFile {
    Absent,
    Clear,
    Link(v1_records::LinkedFile),
}

impl TakenFile {
    fn written(&self) -> v1_records::WrittenFile<'_> {
        match self {
            Self::Absent => v1_records::WrittenFile::Absent,
            Self::Clear => v1_records::WrittenFile::Clear,
            Self::Link(link) => v1_records::WrittenFile::Link(link),
        }
    }

    fn linked(&self) -> bool {
        matches!(self, Self::Link(_))
    }
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
) -> Result<(), String> {
    let body = read_mutation_json(request_file)?;
    let (doc, file) = take_input(request_file, body)?;
    let written = file.written();
    let item = v1_records::record_write(&doc, v1_records::database(target), &written)?;
    let timeout = if durable {
        handler_timeout().saturating_add(Duration::from_secs(20))
    } else {
        Duration::from_secs(10)
    };
    let reply = v1_records::write_mutations(target, &item, timeout)?;
    println!("{}", v1_records::receipt(&reply, file.linked()));
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

fn take_input(request_file: &Path, body: Value) -> Result<(Value, TakenFile), String> {
    if body.get("mutation").is_none() {
        let doc: MutationDoc =
            serde_json::from_value(body).map_err(|e| format!("invalid complete mutation: {e}"))?;
        return Ok((doc_value(&doc)?, TakenFile::Absent));
    }
    let input: RecordInput =
        serde_json::from_value(body).map_err(|e| format!("invalid record input: {e}"))?;
    let doc: MutationDoc = serde_json::from_value(input.mutation)
        .map_err(|e| format!("invalid complete mutation: {e}"))?;
    let file = match input.file {
        InputFile::Absent => TakenFile::Absent,
        InputFile::Clear => TakenFile::Clear,
        InputFile::Link(file) => TakenFile::Link(linked_file(request_file, file)?),
    };
    Ok((doc_value(&doc)?, file))
}

fn linked_file(request_file: &Path, file: FileBody) -> Result<v1_records::LinkedFile, String> {
    let base = request_file.parent().unwrap_or_else(|| Path::new(""));
    let source = base.join(&file.source);
    let canonical =
        std::fs::canonicalize(&source).map_err(|e| format!("open {}: {e}", source.display()))?;
    let path = canonical
        .to_str()
        .ok_or_else(|| format!("open {}: path is not utf-8", source.display()))?
        .to_owned();
    Ok(v1_records::LinkedFile {
        path,
        name: file.name,
        media_type: file.media_type,
    })
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
