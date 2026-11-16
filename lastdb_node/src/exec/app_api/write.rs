//! `POST /v1/write`: a list of `put` and `delete` mutations, committed all or nothing.
//!
//! The caller sends no condition. The engine still commits against a record
//! revision, so this route reads the current revision of each key, commits
//! against it, and starts again when another writer got there first.

use std::collections::{BTreeMap, HashMap, HashSet};

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

const MAX_ATTEMPTS: usize = 8;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WriteBody {
    mutations: Vec<WriteItem>,
}

#[derive(Clone, Deserialize)]
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
    alternates: BTreeMap<String, Option<KeyValue>>,
}

#[derive(Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum Op {
    Put,
    Delete,
}

struct Current {
    revision: String,
    alternates: BTreeMap<String, Option<KeyValue>>,
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
    let written = items.len();
    let database = items.first().and_then(|item| item.database.clone());
    let items = last_per_key(items);
    in_database(database.as_deref(), host, async {
        for _ in 0..MAX_ATTEMPTS {
            match attempt(host, &items).await {
                Err(error) if error.status == 409 => {}
                other => return other.map(|()| json!({ "written": written })),
            }
        }
        Err(HostError::new(
            503,
            "the records kept changing during the write; retry",
        ))
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
        match (item.op, item.record.is_some()) {
            (Op::Put, false) => return Err(bad("a put carries a record")),
            (Op::Delete, true) => return Err(bad("a delete carries no record")),
            _ => {}
        }
        if item.op == Op::Delete && !item.alternates.is_empty() {
            return Err(bad("a delete carries no alternates"));
        }
    }
    Ok(parsed.mutations)
}

/// When one list names a key more than once, the last mutation for it wins.
fn last_per_key(items: Vec<WriteItem>) -> Vec<WriteItem> {
    let mut seen = HashSet::new();
    let mut kept: Vec<WriteItem> = items
        .into_iter()
        .rev()
        .filter(|item| seen.insert((item.schema.clone(), item.key.to_storage_key())))
        .collect();
    kept.reverse();
    kept
}

async fn attempt(host: &Host, items: &[WriteItem]) -> Result<(), HostError> {
    let mut mutations = Vec::with_capacity(items.len());
    for item in items {
        let current = current(host, item).await?;
        if let Some(mutation) = build(item.clone(), current) {
            mutations.push(mutation);
        }
    }
    if mutations.is_empty() {
        return Ok(());
    }
    commit_mutations(host, mutations).await
}

async fn current(host: &Host, item: &WriteItem) -> Result<Option<Current>, HostError> {
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
        [] => Ok(None),
        [row] => {
            let revision = row["revision"]
                .as_str()
                .ok_or_else(|| HostError::internal("stored record has no revision"))?
                .to_owned();
            let alternates = serde_json::from_value(row["alternates"].clone())
                .map_err(|_| HostError::internal("stored alternates are invalid"))?;
            Ok(Some(Current {
                revision,
                alternates,
            }))
        }
        _ => Err(bad(
            "the key names more than one record; a hash and range schema needs key.range",
        )),
    }
}

fn build(item: WriteItem, current: Option<Current>) -> Option<Mutation> {
    let (kind, expected, alternates, fields) = match (item.op, current) {
        (Op::Put, None) => (
            MutationType::Create,
            CasExpectation::Absent,
            item.alternates,
            item.record.unwrap_or_default(),
        ),
        (Op::Put, Some(current)) => (
            MutationType::Replace,
            CasExpectation::Revision {
                revision: current.revision,
            },
            item.alternates,
            item.record.unwrap_or_default(),
        ),
        (Op::Delete, Some(current)) => (
            MutationType::Delete,
            CasExpectation::Revision {
                revision: current.revision,
            },
            current.alternates,
            HashMap::new(),
        ),
        (Op::Delete, None) => return None,
    };
    let target = RecordTarget {
        schema: item.schema,
        key: item.key,
        alternates,
    };
    Some(Mutation::with_uuid(
        String::new(),
        target,
        fields,
        kind,
        expected,
    ))
}
