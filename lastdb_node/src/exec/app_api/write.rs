//! `POST /v1/write`: a list of conditional mutations, committed all or nothing.
//!
//! A `create` writes only when no record holds the key. A `replace` and a
//! `delete` name the revision the caller read and write only when the record
//! is still at it. A mutation whose condition does not hold refuses the list.

use std::collections::{BTreeMap, HashMap};

use fold_db::access::AccessContext;
use fold_db::record_replacement::RecordTarget;
use fold_db::schema::types::field::HashRangeFilter;
use fold_db::schema::types::{CasExpectation, KeyValue, Mutation, MutationType, Query};
use serde::Deserialize;
use serde_json::{json, Value};

use super::{bad, fail, in_database, ok};
use crate::exec::mutation::commit_mutations;
use crate::exec::query::{execute_query, QueryResponseFormat};
use crate::exec::reply::HostError;
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
        for mut item in items {
            // A delete names every address of its record. The caller holds the
            // revision; the node holds the alternate addresses.
            let alternates = if item.op == Op::Delete {
                stored_alternates(host, &item).await?
            } else {
                std::mem::take(&mut item.alternates)
            };
            mutations.push(build(item, alternates));
        }
        let receipt = commit_mutations(host, mutations).await?;
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
    let hash = item
        .key
        .hash
        .clone()
        .filter(|hash| !hash.is_empty())
        .ok_or_else(|| bad("key.hash is required"))?;
    let filter = match item.key.range.clone() {
        Some(range) => HashRangeFilter::HashRangeKey { hash, range },
        None => HashRangeFilter::HashKey(hash),
    };
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
