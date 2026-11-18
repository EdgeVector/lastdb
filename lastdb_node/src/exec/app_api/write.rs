//! `POST /v1/write`: a list of conditional mutations, committed all or nothing.
//!
//! A `create` writes only when no record holds the key. A `replace` and a
//! `delete` name the revision the caller read and write only when the record
//! is still at it. A mutation whose condition does not hold refuses the list
//! and names that mutation.

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;

use fold_db::access::AccessContext;
use fold_db::record_replacement::RecordTarget;
use fold_db::record_value::FileMarker;
use fold_db::schema::types::{CasExpectation, FileWrite, KeyValue, Mutation, MutationType, Query};
use schema_types::record_keys::{RecordKeyDefinition, RecordKeyKind};
use schema_types::FieldValueType;
use serde::Deserialize;
use serde_json::{json, Value};

use super::refusal::{conflict_error, name_conflict, WrittenKey, ABSENT};
use super::{bad, fail, key_filter, ok, require_exact_name, require_write};
use crate::exec::mutation::commit_mutations;
use crate::exec::query::{execute_query, QueryResponseFormat};
use crate::exec::reply::HostError;
use crate::exec::schema_routes::resolve_schema_name;
use crate::exec::wire::QueryRequest;
use crate::host::Host;
use crate::lastdb_uds::uds_http::{UdsRequest, UdsResponse};

type Alternates = BTreeMap<String, Option<KeyValue>>;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WriteBody {
    mutations: Vec<WriteItem>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WriteItem {
    #[serde(default)]
    database: Option<String>,
    schema: String,
    op: Op,
    key: KeyValue,
    #[serde(default)]
    record: Option<HashMap<String, Value>>,
    #[serde(default)]
    alternates: Alternates,
    #[serde(default)]
    revision: Option<String>,
    #[serde(default)]
    file: Option<FileLink>,
}

/// A link to a file on disk that the caller can read. No bytes pass through
/// the request, and LastDB keeps a reference in place of a copy.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FileLink {
    path: String,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    media_type: Option<String>,
}

/// A file reference to remember once its record is committed.
struct LinkedFile {
    schema: String,
    key: KeyValue,
    path: PathBuf,
    marker: FileMarker,
}

#[derive(Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum Op {
    Create,
    Replace,
    Delete,
}

pub(in crate::exec) async fn write_route(
    req: &UdsRequest,
    _ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    match write(host, &req.body).await {
        Ok(body) => ok(&body),
        Err(error) => fail(&error),
    }
}

async fn write(host: &Host, body: &[u8]) -> Result<Value, HostError> {
    let items = parse(body)?;
    for (index, item) in items.iter().enumerate() {
        require_write(&item.schema).map_err(|error| error.at(index))?;
    }
    let database = items.first().and_then(|item| item.database.clone());
    super::database::write_in(database.as_deref(), host, async {
        for (index, item) in items.iter().enumerate() {
            require_exact_name(host, &item.schema).map_err(|error| error.at(index))?;
        }
        let located = located_keys(host, &items)?;
        let prepared = prepare(host, items).await?;
        let receipt = commit_mutations(host, prepared.mutations)
            .await
            .map_err(|error| name_conflict(error, &located))?;
        for file in &prepared.linked {
            host.records()
                .remember_local_file(&file.schema, &file.key, &file.path, &file.marker)
                .await?;
        }
        let revisions: Vec<Value> = receipt
            .mutation_ids
            .iter()
            .map(|id| json!(receipt.record_revisions.get(id)))
            .collect();
        Ok(json!({ "written": revisions.len(), "revisions": revisions }))
    })
    .await
}

struct Prepared {
    mutations: Vec<Mutation>,
    linked: Vec<LinkedFile>,
}

async fn prepare(host: &Host, items: Vec<WriteItem>) -> Result<Prepared, HostError> {
    let mut mutations = Vec::with_capacity(items.len());
    let mut linked = Vec::new();
    for (index, mut item) in items.into_iter().enumerate() {
        // A delete names every declared alternate. The node reads the live
        // record's addresses. When no record is there, a shape-valid stand-in
        // lets the commit report that absence with the other conditions.
        let alternates = if item.op == Op::Delete {
            match stored_alternates(host, &item).await {
                Ok(found) => found,
                Err(error) if error.reason == Some(ABSENT) => {
                    return Err(conflict_error(index, ABSENT));
                }
                Err(error) => return Err(error.at(index)),
            }
        } else {
            std::mem::take(&mut item.alternates)
        };
        let link = item.file.take();
        let mut mutation = build(item, alternates);
        if let Some(link) = link {
            linked.push(
                link_file(host, &mut mutation, link)
                    .await
                    .map_err(|error| error.at(index))?,
            );
        }
        mutations.push(mutation);
    }
    Ok(Prepared { mutations, linked })
}

fn located_keys(host: &Host, items: &[WriteItem]) -> Result<Vec<WrittenKey>, HostError> {
    let mut located = Vec::with_capacity(items.len());
    for (index, item) in items.iter().enumerate() {
        let schema = resolve_schema_name(host, &item.schema)
            .map_err(|error| error.at(index))?
            .unwrap_or_else(|| item.schema.clone());
        located.push(WrittenKey {
            schema,
            key: item.key.to_storage_key(),
        });
    }
    Ok(located)
}

fn parse(body: &[u8]) -> Result<Vec<WriteItem>, HostError> {
    let parsed: WriteBody = serde_json::from_slice(body)
        .map_err(|error| bad(format!("invalid /v1/write body: {error}")))?;
    let Some(first) = parsed.mutations.first() else {
        return Err(bad("mutations must hold at least one mutation"));
    };
    for (index, item) in parsed.mutations.iter().enumerate() {
        if item.database != first.database {
            return Err(bad("every mutation in one write names the same database").at(index));
        }
        let (record, revision) = (item.record.is_some(), item.revision.is_some());
        let shape = match item.op {
            Op::Create if record && !revision => Ok(()),
            Op::Create => Err("a create carries a record and no revision"),
            Op::Replace if record && revision => Ok(()),
            Op::Replace => Err("a replace carries a record and the revision it replaces"),
            Op::Delete if item.file.is_some() => Err("a delete carries no file"),
            Op::Delete if !record && revision && item.alternates.is_empty() => Ok(()),
            Op::Delete => Err("a delete carries the revision it deletes and no record"),
        };
        if let Err(message) = shape {
            return Err(bad(message).at(index));
        }
    }
    Ok(parsed.mutations)
}

async fn stored_alternates(host: &Host, item: &WriteItem) -> Result<Alternates, HostError> {
    let filter = key_filter(&item.key)?;
    let request = QueryRequest {
        query: Query::new_with_filter(item.schema.clone(), Some(filter)),
        limit: Some(2),
        offset: None,
        cursor: None,
        response_format: QueryResponseFormat::Values,
    };
    let page = execute_query(host, request).await?;
    let rows = page["results"]
        .as_array()
        .ok_or_else(|| HostError::internal("write read is incomplete"))?;
    match rows.as_slice() {
        [] => absent_addresses(host, item),
        [row] => serde_json::from_value(row["alternates"].clone())
            .map_err(|_| HostError::internal("stored alternates are invalid")),
        _ => Err(bad(
            "the key names more than one record; a hash and range schema needs key.range",
        )),
    }
}

/// No live record at this key. The engine requires every declared alternate
/// name before it checks the condition, and it compares those addresses only
/// when a record is live. A shape-valid stand-in, taken from the schema the
/// write already loads, lets the commit report `absent`. When no stand-in
/// fits, this keyed read has already shown that the record is absent.
fn absent_addresses(host: &Host, item: &WriteItem) -> Result<Alternates, HostError> {
    let Some(name) = resolve_schema_name(host, &item.schema)? else {
        return Ok(Alternates::new());
    };
    let Some(schema) = host.records().schema_manager().get_schema_metadata(&name)? else {
        return Ok(Alternates::new());
    };
    let Some(keys) = schema.record_keys.as_ref() else {
        return Ok(Alternates::new());
    };
    let mut alternates = Alternates::new();
    for (alternate, definition) in &keys.alternates {
        let Some(address) = stand_in_address(definition, &item.key) else {
            return Err(HostError::new(409, "no record holds the key").with_reason(ABSENT));
        };
        alternates.insert(alternate.clone(), address);
    }
    Ok(alternates)
}

/// `Some(None)` is an alternate the schema allows to be missing.
/// `None` means no address fits the declared shape.
fn stand_in_address(
    definition: &RecordKeyDefinition,
    primary: &KeyValue,
) -> Option<Option<KeyValue>> {
    if definition.allow_absent {
        return Some(None);
    }
    let hash = primary.hash.clone().filter(|hash| !hash.is_empty())?;
    stand_in_ranges(definition, primary)
        .into_iter()
        .find(|range| {
            definition
                .validate_coordinates(Some(hash.as_str()), range.as_deref())
                .is_ok()
        })
        .map(|range| {
            Some(KeyValue {
                hash: Some(hash),
                range,
            })
        })
}

fn stand_in_ranges(definition: &RecordKeyDefinition, primary: &KeyValue) -> Vec<Option<String>> {
    if definition.kind == RecordKeyKind::Hash {
        return vec![None];
    }
    let mut ranges = Vec::new();
    if let Some(sample) = sample_range(definition.range_type.as_ref()) {
        ranges.push(Some(sample));
    }
    if let Some(range) = primary.range.clone().filter(|range| !range.is_empty()) {
        ranges.push(Some(range));
    }
    if let Some(hash) = primary.hash.clone().filter(|hash| !hash.is_empty()) {
        ranges.push(Some(hash));
    }
    ranges
}

fn sample_range(range_type: Option<&FieldValueType>) -> Option<String> {
    Some(
        match range_type? {
            FieldValueType::String => "-",
            FieldValueType::Integer => "0",
            FieldValueType::Boolean => "false",
            FieldValueType::Date => "1970-01-01",
            FieldValueType::DateTime => "1970-01-01T00:00:00Z",
            _ => return None,
        }
        .to_owned(),
    )
}

/// Describe the linked file, set it on the mutation, and return what to
/// remember after the commit. The file stays where it is.
/// One answer for every file that cannot be linked, so a caller does not
/// learn from the refusal whether a path exists.
const UNUSABLE_FILE: &str =
    "the file cannot be linked: it must be a regular file outside the LastDB home";

async fn link_file(
    host: &Host,
    mutation: &mut Mutation,
    link: FileLink,
) -> Result<LinkedFile, HostError> {
    // The whole LastDB home is refused, through any symlink: its key file and
    // every database in it, not only the data folder of the selected one.
    let real = std::fs::canonicalize(&link.path).map_err(|_| bad(UNUSABLE_FILE))?;
    let home = std::fs::canonicalize(&host.home).unwrap_or_else(|_| host.home.clone());
    if real.starts_with(&home) {
        return Err(bad(UNUSABLE_FILE));
    }
    let records = host.records();
    let write = records
        .db_ops()
        .records()
        .reference_local_file(&real, link.name, link.media_type)
        .await
        .map_err(|_| bad(UNUSABLE_FILE))?;
    let FileWrite::LocalPath(reference) = &write else {
        return Err(HostError::internal(
            "a local file reference is a local path",
        ));
    };
    let (marker, path) = (reference.marker().clone(), reference.path().to_path_buf());
    let schema = resolve_schema_name(host, &mutation.schema_name)?
        .unwrap_or_else(|| mutation.schema_name.clone());
    mutation.file = Some(marker.clone());
    mutation.file_write = Some(write);
    Ok(LinkedFile {
        schema,
        key: mutation.key_value.clone(),
        path,
        marker,
    })
}

fn build(item: WriteItem, alternates: Alternates) -> Mutation {
    let kind = match item.op {
        Op::Create => MutationType::Create,
        Op::Replace => MutationType::Replace,
        Op::Delete => MutationType::Delete,
    };
    let expected = item.revision.map_or(CasExpectation::Absent, |revision| {
        CasExpectation::Revision { revision }
    });
    let target = RecordTarget {
        schema: item.schema,
        key: item.key,
        alternates,
    };
    Mutation::with_uuid(
        String::new(),
        target,
        item.record.unwrap_or_default(),
        kind,
        expected,
    )
}
