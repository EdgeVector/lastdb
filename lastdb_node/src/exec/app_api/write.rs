//! `POST /v1/write`: a list of conditional mutations, committed all or nothing.
//!
//! A `create` writes only when no record holds the key. A `replace` and a
//! `delete` name the revision the caller read and write only when the record
//! is still at it. A mutation whose condition does not hold refuses the list.

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};

use fold_db::access::AccessContext;
use fold_db::record_replacement::RecordTarget;
use fold_db::record_value::FileMarker;
use fold_db::schema::types::{CasExpectation, FileWrite, KeyValue, Mutation, MutationType, Query};
use serde::Deserialize;
use serde_json::{json, Value};

use super::{bad, fail, in_database, key_filter, ok};
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
    let database = items.first().and_then(|item| item.database.clone());
    in_database(database.as_deref(), host, async {
        let mut mutations = Vec::with_capacity(items.len());
        let mut linked = Vec::new();
        for mut item in items {
            // A delete names every address of its record. The caller holds the
            // revision; the node holds the alternate addresses.
            let alternates = if item.op == Op::Delete {
                stored_alternates(host, &item).await?
            } else {
                std::mem::take(&mut item.alternates)
            };
            let link = item.file.take();
            let mut mutation = build(item, alternates);
            if let Some(link) = link {
                linked.push(link_file(host, &mut mutation, link).await?);
            }
            mutations.push(mutation);
        }
        let receipt = commit_mutations(host, mutations).await?;
        for file in &linked {
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

fn parse(body: &[u8]) -> Result<Vec<WriteItem>, HostError> {
    let parsed: WriteBody = serde_json::from_slice(body)
        .map_err(|error| bad(format!("invalid /v1/write body: {error}")))?;
    let Some(first) = parsed.mutations.first() else {
        return Err(bad("mutations must hold at least one mutation"));
    };
    for item in &parsed.mutations {
        if item.database != first.database {
            return Err(bad("every mutation in one write names the same database"));
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
            return Err(bad(message));
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
        // No record holds the key, so no revision of it can match.
        [] => Err(HostError::new(
            409,
            "the record does not exist, so the delete revision does not match",
        )),
        [row] => serde_json::from_value(row["alternates"].clone())
            .map_err(|_| HostError::internal("stored alternates are invalid")),
        _ => Err(bad(
            "the key names more than one record; a hash and range schema needs key.range",
        )),
    }
}

/// Describe the linked file, set it on the mutation, and return what to
/// remember after the commit. The file stays where it is.
async fn link_file(
    host: &Host,
    mutation: &mut Mutation,
    link: FileLink,
) -> Result<LinkedFile, HostError> {
    let records = host.records();
    let write = records
        .db_ops()
        .records()
        .reference_local_file(Path::new(&link.path), link.name, link.media_type)
        .await
        .map_err(|error| bad(format!("file {}: {error}", link.path)))?;
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
