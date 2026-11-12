//! Single, batch, file-delete, and deferred mutation routes and executors.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;

use fold_db::access::AccessContext;
use fold_db::fold_db_core::mutation_manager::{MutationManager, ResidentCommitReceipt};
use fold_db::record_value::InlineWrite;
use fold_db::schema::types::operations::Operation;
use fold_db::schema::types::{FileWrite, Mutation, MutationType, SchemaError};
use serde::Deserialize;
use serde_json::{json, Value};

use super::file_blob::Placement;
use super::reply::{content_free, error_response, render, HostError};
use super::schema_resolve::resolve_schema_name;
use super::wire::{parse_batch_mutation_body, parse_mutation_body, ParsedMutation};
use super::{data_route_budget, with_handler_timeout_budget, ErasedRoute};
use crate::host::Host;
use crate::lastdb_uds::uds_http::{UdsRequest, UdsResponse};
use crate::lastdb_uds::uds_router::DataRoute;

const POISONED_READ_VIEW: &str =
    "Storage backend error: native read view lock is poisoned; restart the process to reopen the home";

pub(in crate::exec) async fn execute_mutation_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    let parsed = match parse_mutation_body(&req.body) {
        Ok(parsed) => parsed,
        Err(reject) => {
            tracing::warn!(
                target: "fold_node::http",
                route = "/api/mutation",
                kind = reject.kind().as_str(),
                "mutation request refused on shape"
            );
            return reject.response();
        }
    };
    render(commit_parsed_mutation(host, parsed).await, ctx)
}

pub(in crate::exec) async fn execute_db_delete_blob_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    if !ctx.is_owner {
        return error_response(403, "file delete is owner-only", ctx);
    }
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Body {
        mutation: Operation,
        #[serde(default)]
        placement: Placement,
    }
    let delete = async {
        let body: Body = serde_json::from_slice(&req.body)
            .map_err(|e| HostError::new(400, format!("invalid delete-blob request: {e}")))?;
        let mut parsed = ParsedMutation::from_operation(body.mutation, None);
        if parsed.mutation.mutation_type != MutationType::Replace {
            return Err(HostError::new(
                400,
                "delete-blob requires a complete replacement",
            ));
        }
        match body.placement {
            Placement::Inline => parsed.mutation.inline_write = Some(InlineWrite::Clear),
            Placement::Blob if parsed.mutation.file.is_some() => {
                return Err(HostError::new(
                    400,
                    "delete-blob requires a complete replacement without a file marker",
                ));
            }
            Placement::Blob => parsed.mutation.file_write = Some(FileWrite::Clear),
        }
        commit_parsed_mutation(host, parsed).await
    };
    render(delete.await, ctx)
}

pub(in crate::exec) async fn execute_mutations_batch_route(
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
            DataRoute::Mutation => heap_route!(execute_mutation_route, req, ctx, host),
            DataRoute::MutationBatch => heap_route!(execute_mutations_batch_route, req, ctx, host),
            _ => return content_free(404, "Not Found"),
        };
        with_handler_timeout_budget(ctx, data_route_budget(route), work).await
    })
}

async fn stamp_and_validate_mutation(
    host: &Host,
    parsed: &mut ParsedMutation,
    pub_key: String,
) -> Result<(), HostError> {
    let requested = parsed.mutation.schema_name.clone();
    let canonical = resolve_schema_name(host, &requested)?
        .ok_or_else(|| HostError::new(404, format!("Schema not found: {requested}")))?;
    validate_mutation_fields(host, &requested, &canonical, &parsed.mutation.record_fields).await?;
    parsed.mutation.schema_name = canonical;
    parsed.mutation.uuid = uuid::Uuid::new_v4().to_string();
    parsed.mutation.pub_key = pub_key;
    Ok(())
}

pub(in crate::exec) async fn commit_parsed_mutation(
    host: &Host,
    mut parsed: ParsedMutation,
) -> Result<Value, HostError> {
    stamp_and_validate_mutation(host, &mut parsed, host.public_key()).await?;
    let payload_bytes: usize = parsed
        .mutation
        .record_fields
        .values()
        .map(|v| match v {
            Value::String(s) => s.len(),
            other => other.to_string().len(),
        })
        .sum::<usize>()
        + parsed
            .mutation
            .file_write
            .as_ref()
            .map_or(0, FileWrite::row_len)
        + match &parsed.mutation.inline_write {
            Some(InlineWrite::Put(file)) => file.data.len(),
            Some(InlineWrite::Clear) | None => 0,
        };

    let receipt = start_core_commit(host.records().mutation_manager(), vec![parsed.mutation])
        .await
        .map_err(|e| prefix_core_failure(e, "Mutation execution failed"))?;
    let mutation_id = receipt
        .mutation_ids
        .last()
        .cloned()
        .ok_or_else(|| HostError::internal("Mutation returned no IDs"))?;
    let drained = parsed.convergence.waits();
    Ok(commit_json(
        &receipt,
        json!({
            "mutation_id": mutation_id,
            "record_revision": receipt.record_revisions.get(&mutation_id),
            "success": true,
            "background_tasks_drained": drained,
            "convergence_pending": !drained,
            "size": payload_bytes,
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
    let drained = batch.iter().any(|item| item.convergence.waits());
    let public_key = host.public_key();
    let mut mutations = Vec::with_capacity(batch.len());
    for mut parsed in batch {
        stamp_and_validate_mutation(host, &mut parsed, public_key.clone()).await?;
        mutations.push(parsed.mutation);
    }
    let count = mutations.len();
    let receipt = start_core_commit(host.records().mutation_manager(), mutations)
        .await
        .map_err(|e| prefix_core_failure(e, "Mutation batch failed"))?;
    Ok(commit_json(
        &receipt,
        json!({
            "mutation_ids": &receipt.mutation_ids,
            "count": count,
            "background_tasks_drained": drained,
            "convergence_pending": !drained,
        }),
    ))
}

fn prefix_core_failure(error: SchemaError, failed: &str) -> HostError {
    if let SchemaError::InvalidData(msg) = &error {
        if msg == POISONED_READ_VIEW {
            return HostError::new(503, msg.clone());
        }
    }
    let mapped = HostError::from(error);
    if mapped.status == 500 {
        HostError::internal(format!("{failed}: {}", mapped.message))
    } else {
        mapped
    }
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
                .is_some_and(|plain| plain.iter().any(|n| n == *f))
        })
        .cloned()
        .collect();
    if unknown.is_empty() {
        return Ok(());
    }
    unknown.sort();
    let mut available = schema.fields.clone().unwrap_or_default();
    available.sort();
    available.dedup();
    tracing::info!(
        target: "lastdb_node::exec",
        schema = %requested,
        unknown_fields = ?unknown,
        "execute_mutation: rejecting unknown fields"
    );
    let quoted = unknown
        .iter()
        .map(|f| format!("'{f}'"))
        .collect::<Vec<_>>()
        .join(", ");
    let plural = if unknown.len() == 1 { "" } else { "s" };
    let avail = if available.is_empty() {
        "<none>".to_string()
    } else {
        available.join(", ")
    };
    let message =
        format!("Field{plural} {quoted} not writable on schema '{requested}'. Available: {avail}");
    Err(HostError::new(
        400,
        json!({
            "ok": false, "error": "unknown_fields", "message": message,
            "schema_name": requested, "unknown_fields": unknown, "available_fields": available,
        })
        .to_string(),
    ))
}

fn commit_json(receipt: &ResidentCommitReceipt, extra: Value) -> Value {
    let mut base = json!({
        "revision": receipt.revision,
        "record_revisions": receipt.record_revisions,
        "operations": {
            "created": receipt.operations.created,
            "updated": receipt.operations.updated,
            "deleted": receipt.operations.deleted,
            "total": receipt.operations.total(),
        },
        "durability": receipt.durability.as_str(),
        "resident_stages_us": { "prepare": receipt.stages.prepare_us, "ack": receipt.stages.ack_us },
    });
    if let (Some(map), Value::Object(extra_map)) = (base.as_object_mut(), extra) {
        map.extend(extra_map);
    }
    base
}

type CoreCommitFuture<'a> =
    Pin<Box<dyn Future<Output = Result<ResidentCommitReceipt, SchemaError>> + Send + 'a>>;

#[inline(never)]
fn start_core_commit<'a>(
    manager: &'a MutationManager,
    mutations: Vec<Mutation>,
) -> CoreCommitFuture<'a> {
    Box::pin(manager.write_mutations_receipt(mutations))
}
