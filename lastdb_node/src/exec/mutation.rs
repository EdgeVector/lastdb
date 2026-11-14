//! Single, batch, and deferred record mutation routes and commit executors.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;

use fold_db::access::AccessContext;
use fold_db::fold_db_core::mutation_manager::{MutationManager, ResidentCommitReceipt};
use fold_db::record_value::InlineWrite;
use fold_db::schema::types::{FileWrite, Mutation, SchemaError};
use serde_json::{json, Value};

use super::reply::{content_free, render, HostError};
use super::schema_routes::resolve_schema_name;
use super::wire::{parse_batch_mutation_body, parse_mutation_body, ParsedMutation};
use super::{data_route_budget, with_handler_timeout_budget, ErasedRoute};
use crate::host::Host;
use crate::lastdb_uds::uds_http::{UdsRequest, UdsResponse};
use crate::lastdb_uds::uds_router::DataRoute;

const POISONED_READ_VIEW: &str =
    "Storage backend error: native read view lock is poisoned; restart the process to reopen the home";

pub(in crate::exec) async fn mutation_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    match parse_mutation_body(&req.body) {
        Ok(parsed) => render(commit_parsed_mutation(host, parsed).await, ctx),
        Err(reject) => {
            tracing::warn!(target: "fold_node::http", route = "/api/mutation", kind = reject.kind.as_str(), "mutation request refused on shape");
            reject.response()
        }
    }
}

pub(in crate::exec) async fn batch_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    match parse_batch_mutation_body(&req.body) {
        Ok(batch) => render(execute_mutations_batch(host, batch).await, ctx),
        Err(resp) => resp,
    }
}

pub fn execute_deferred_mutation<'a>(
    route: DataRoute,
    req: &'a UdsRequest,
    ctx: &'a AccessContext,
    host: &'a Host,
) -> ErasedRoute<'a> {
    Box::pin(async move {
        let work = match route {
            DataRoute::Mutation => heap_route!(mutation_route, req, ctx, host),
            DataRoute::MutationBatch => heap_route!(batch_route, req, ctx, host),
            _ => return content_free(404, "Not Found"),
        };
        with_handler_timeout_budget(ctx, data_route_budget(route), work).await
    })
}

async fn stamp_and_validate(
    host: &Host,
    parsed: &mut ParsedMutation,
    pub_key: String,
) -> Result<(), HostError> {
    let req = parsed.mutation.schema_name.clone();
    let canonical = resolve_schema_name(host, &req)?
        .ok_or_else(|| HostError::new(404, format!("Schema not found: {req}")))?;
    validate_mutation_fields(host, &req, &canonical, &parsed.mutation.record_fields).await?;
    parsed.mutation.schema_name = canonical;
    parsed.mutation.uuid = uuid::Uuid::new_v4().to_string();
    parsed.mutation.pub_key = pub_key;
    Ok(())
}

pub(in crate::exec) async fn commit_parsed_mutation(
    host: &Host,
    mut parsed: ParsedMutation,
) -> Result<Value, HostError> {
    stamp_and_validate(host, &mut parsed, host.public_key()).await?;
    let field_bytes: usize = parsed
        .mutation
        .record_fields
        .values()
        .map(|v| v.as_str().map_or_else(|| v.to_string().len(), str::len))
        .sum();
    let inline_bytes = if let Some(InlineWrite::Put(f)) = &parsed.mutation.inline_write {
        f.data.len()
    } else {
        0
    };
    let size = field_bytes
        + parsed
            .mutation
            .file_write
            .as_ref()
            .map_or(0, FileWrite::row_len)
        + inline_bytes;
    let drained = parsed.convergence.waits();
    let r = run_core_commit(host, vec![parsed.mutation], "Mutation execution failed").await?;
    let id = r
        .mutation_ids
        .last()
        .cloned()
        .ok_or_else(|| HostError::internal("Mutation returned no IDs"))?;
    Ok(commit_json(
        &r,
        json!({
            "mutation_id": id, "record_revision": r.record_revisions.get(&id), "success": true,
            "background_tasks_drained": drained, "convergence_pending": !drained, "size": size,
        }),
    ))
}

async fn execute_mutations_batch(
    host: &Host,
    batch: Vec<ParsedMutation>,
) -> Result<Value, HostError> {
    if batch.is_empty() {
        return Err(HostError::new(
            400,
            "Mutation batch must contain at least one mutation",
        ));
    }
    let (drained, pk) = (
        batch.iter().any(|i| i.convergence.waits()),
        host.public_key(),
    );
    let mut mutations = Vec::with_capacity(batch.len());
    for mut parsed in batch {
        stamp_and_validate(host, &mut parsed, pk.clone()).await?;
        mutations.push(parsed.mutation);
    }
    let count = mutations.len();
    let r = run_core_commit(host, mutations, "Mutation batch failed").await?;
    Ok(commit_json(
        &r,
        json!({
            "mutation_ids": &r.mutation_ids, "count": count,
            "background_tasks_drained": drained, "convergence_pending": !drained,
        }),
    ))
}

async fn run_core_commit(
    host: &Host,
    mutations: Vec<Mutation>,
    failed: &str,
) -> Result<ResidentCommitReceipt, HostError> {
    #[inline(never)]
    fn start(
        mgr: &MutationManager,
        m: Vec<Mutation>,
    ) -> Pin<Box<dyn Future<Output = Result<ResidentCommitReceipt, SchemaError>> + Send + '_>> {
        Box::pin(mgr.write_mutations_receipt(m))
    }
    start(host.records().mutation_manager(), mutations)
        .await
        .map_err(|error| {
            if matches!(&error, SchemaError::InvalidData(m) if m == POISONED_READ_VIEW) {
                return HostError::new(503, POISONED_READ_VIEW);
            }
            let mut mapped = HostError::from(error);
            if mapped.status == 500 {
                mapped.message = format!("{failed}: {}", mapped.message);
            }
            mapped
        })
}

async fn validate_mutation_fields(
    host: &Host,
    requested: &str,
    canonical: &str,
    record_fields: &HashMap<String, Value>,
) -> Result<(), HostError> {
    if record_fields.is_empty() {
        return Ok(());
    }
    let Ok(Some(schema)) = host
        .records()
        .schema_manager()
        .resolve_schema(canonical)
        .await
    else {
        return Ok(());
    };
    let mut unknown: Vec<String> = record_fields
        .keys()
        .filter(|f| {
            !schema
                .fields
                .as_ref()
                .is_some_and(|p| p.iter().any(|n| n == *f))
        })
        .cloned()
        .collect();
    if unknown.is_empty() {
        return Ok(());
    }
    unknown.sort();
    let mut avail = schema.fields.clone().unwrap_or_default();
    avail.sort();
    avail.dedup();
    tracing::info!(target: "lastdb_node::exec", schema = %requested, unknown_fields = ?unknown, "execute_mutation: rejecting unknown fields");
    let quoted = unknown
        .iter()
        .map(|f| format!("'{f}'"))
        .collect::<Vec<_>>()
        .join(", ");
    let msg = format!(
        "Field{} {quoted} not writable on schema '{requested}'. Available: {}",
        if unknown.len() == 1 { "" } else { "s" },
        if avail.is_empty() {
            "<none>".into()
        } else {
            avail.join(", ")
        }
    );
    Err(HostError::new(
        400,
        json!({
            "ok": false, "error": "unknown_fields", "message": msg,
            "schema_name": requested, "unknown_fields": unknown, "available_fields": avail,
        })
        .to_string(),
    ))
}

fn commit_json(r: &ResidentCommitReceipt, extra: Value) -> Value {
    let mut base = json!({
        "revision": r.revision, "record_revisions": r.record_revisions,
        "operations": {
            "created": r.operations.created, "updated": r.operations.updated,
            "deleted": r.operations.deleted, "total": r.operations.total(),
        },
        "durability": r.durability.as_str(),
        "resident_stages_us": { "prepare": r.stages.prepare_us, "ack": r.stages.ack_us },
    });
    if let (Some(map), Value::Object(extra_map)) = (base.as_object_mut(), extra) {
        map.extend(extra_map);
    }
    base
}
