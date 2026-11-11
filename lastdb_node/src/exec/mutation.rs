//! Single, batch, and deferred mutation routes and executors.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;

use fold_db::access::AccessContext;
use fold_db::fold_db_core::mutation_manager::{
    MutationManager, ResidentCommitOperations, ResidentCommitReceipt, ResidentCommitStages,
};
use fold_db::record_value::InlineWrite;
use fold_db::schema::types::operations::MutationConvergence;
use fold_db::schema::types::{FileWrite, Mutation, Schema, SchemaError};
use serde_json::Value;

use super::reply::{content_free, render, HostError};
use super::schema_resolve::resolve_schema_name;
use super::timeouts::{data_route_budget, with_handler_timeout_budget};
use super::wire::{parse_batch_mutation_body, parse_mutation_body, ParsedMutation};
use super::ErasedRoute;
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

pub(in crate::exec) async fn execute_mutations_batch_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    let batch = match parse_batch_mutation_body(&req.body) {
        Ok(batch) => batch,
        Err(resp) => return resp,
    };
    render(execute_mutations_batch(host, batch).await, ctx)
}

/// Preserve route scopes, handlers, and budgets when a mutation runs off the socket worker.
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
        with_handler_timeout_budget(
            ctx,
            data_route_budget(route),
            super::dispatch::execute_observed_route(ctx, work),
        )
        .await
    })
}

/// Resolve schema, validate fields, stamp UUID + pubkey, and commit one parsed mutation.
pub(in crate::exec) async fn commit_parsed_mutation(
    host: &Host,
    mut parsed: ParsedMutation,
) -> Result<Value, HostError> {
    let requested = parsed.mutation.schema_name.clone();
    let canonical = resolve_required_schema_name(host, &requested)?;
    validate_mutation_fields(host, &requested, &canonical, &parsed.mutation.record_fields).await?;
    let payload_bytes = fields_payload_bytes(&parsed.mutation.record_fields)
        + parsed
            .mutation
            .file_write
            .as_ref()
            .map_or(0, FileWrite::row_len)
        + match &parsed.mutation.inline_write {
            Some(InlineWrite::Put(file)) => file.data.len(),
            Some(InlineWrite::Clear) | None => 0,
        };

    parsed.mutation.schema_name = canonical;
    parsed.mutation.uuid = uuid::Uuid::new_v4().to_string();
    parsed.mutation.pub_key = host.public_key();

    let receipt = start_core_commit(host.db.mutation_manager(), vec![parsed.mutation])
        .await
        .map_err(|e| prefix_core_failure(e, "Mutation execution failed"))?;
    let mutation_id = receipt
        .mutation_ids
        .last()
        .cloned()
        .ok_or_else(|| HostError::internal("Mutation returned no IDs"))?;
    Ok(mutation_response(
        &mutation_id,
        parsed.convergence.waits(),
        &receipt,
        payload_bytes,
    ))
}

async fn execute_mutations_batch(
    host: &Host,
    batch: Vec<ParsedMutation>,
) -> Result<Value, HostError> {
    let convergence = batch_convergence(&batch)?;
    let public_key = host.public_key();
    let mut mutations = Vec::with_capacity(batch.len());
    for mut parsed in batch {
        let requested = parsed.mutation.schema_name.clone();
        let canonical = resolve_required_schema_name(host, &requested)?;
        validate_mutation_fields(host, &requested, &canonical, &parsed.mutation.record_fields)
            .await?;
        parsed.mutation.schema_name = canonical;
        parsed.mutation.uuid = uuid::Uuid::new_v4().to_string();
        parsed.mutation.pub_key.clone_from(&public_key);
        mutations.push(parsed.mutation);
    }

    let count = mutations.len();
    let receipt = start_core_commit(host.db.mutation_manager(), mutations)
        .await
        .map_err(|e| prefix_core_failure(e, "Mutation batch failed"))?;
    Ok(batch_response(&receipt, count, convergence.waits()))
}

fn batch_convergence(batch: &[ParsedMutation]) -> Result<MutationConvergence, HostError> {
    if batch.is_empty() {
        return Err(HostError::new(
            400,
            "Mutation batch must contain at least one mutation",
        ));
    }
    if batch.iter().any(|item| item.convergence.waits()) {
        Ok(MutationConvergence::Sync)
    } else {
        Ok(MutationConvergence::Async)
    }
}

fn prefix_core_failure(error: SchemaError, failed: &str) -> HostError {
    if let SchemaError::InvalidData(message) = &error {
        if message == POISONED_READ_VIEW {
            return HostError::new(503, message.clone());
        }
    }
    let mapped = HostError::from(error);
    if mapped.status == 500 {
        HostError::internal(format!("{failed}: {}", mapped.message))
    } else {
        mapped
    }
}

fn mutation_writable_field_names(schema: &Schema) -> Vec<String> {
    let mut names = schema.fields.clone().unwrap_or_default();
    names.sort();
    names.dedup();
    names
}

fn mutation_field_is_writable(schema: &Schema, field: &str) -> bool {
    schema
        .fields
        .as_ref()
        .is_some_and(|plain| plain.iter().any(|name| name == field))
}

fn mutation_unknown_fields(
    schema: &Schema,
    record_fields: &HashMap<String, Value>,
) -> Option<(Vec<String>, Vec<String>)> {
    if record_fields.is_empty() {
        return None;
    }
    let mut unknown: Vec<String> = record_fields
        .keys()
        .filter(|field| !mutation_field_is_writable(schema, field))
        .cloned()
        .collect();
    if unknown.is_empty() {
        None
    } else {
        unknown.sort();
        Some((unknown, mutation_writable_field_names(schema)))
    }
}

fn unknown_fields_body(schema_name: &str, unknown: &[String], available: &[String]) -> Value {
    let quoted_unknown = unknown
        .iter()
        .map(|f| format!("'{f}'"))
        .collect::<Vec<_>>()
        .join(", ");
    let plural = if unknown.len() == 1 { "" } else { "s" };
    let available_summary = if available.is_empty() {
        "<none>".to_string()
    } else {
        available.join(", ")
    };
    let message = format!(
        "Field{plural} {quoted_unknown} not writable on schema '{schema_name}'. Available: {available_summary}",
    );
    serde_json::json!({
        "ok": false,
        "error": "unknown_fields",
        "message": message,
        "schema_name": schema_name,
        "unknown_fields": unknown,
        "available_fields": available,
    })
}

async fn validate_mutation_fields(
    host: &Host,
    requested: &str,
    canonical: &str,
    record_fields: &HashMap<String, Value>,
) -> Result<(), HostError> {
    let Ok(Some(schema)) = host.db.schema_manager().resolve_schema(canonical).await else {
        return Ok(());
    };
    if let Some((unknown, available)) = mutation_unknown_fields(&schema, record_fields) {
        tracing::info!(
            target: "lastdb_node::exec",
            schema = %requested,
            unknown_fields = ?unknown,
            "execute_mutation: rejecting unknown fields"
        );
        return Err(HostError::new(
            400,
            unknown_fields_body(requested, &unknown, &available).to_string(),
        ));
    }
    Ok(())
}

fn fields_payload_bytes(fields: &HashMap<String, Value>) -> usize {
    fields
        .values()
        .map(|v| match v {
            Value::String(s) => s.len(),
            other => other.to_string().len(),
        })
        .sum()
}

fn resolve_required_schema_name(host: &Host, requested: &str) -> Result<String, HostError> {
    resolve_schema_name(host, requested)?
        .ok_or_else(|| HostError::new(404, format!("Schema not found: {requested}")))
}

fn operations_json(operations: &ResidentCommitOperations) -> Value {
    serde_json::json!({
        "created": operations.created,
        "updated": operations.updated,
        "deleted": operations.deleted,
        "total": operations.total(),
    })
}

fn stages_json(stages: &ResidentCommitStages) -> Value {
    serde_json::json!({
        "prepare": stages.prepare_us,
        "ack": stages.ack_us,
    })
}

fn commit_core_json(receipt: &ResidentCommitReceipt) -> Value {
    serde_json::json!({
        "revision": receipt.revision,
        "record_revisions": receipt.record_revisions,
        "operations": operations_json(&receipt.operations),
        "durability": receipt.durability.as_str(),
        "resident_stages_us": stages_json(&receipt.stages),
    })
}

fn extend_json(base: Value, extra: Value) -> Value {
    let Value::Object(mut base_map) = base else {
        return extra;
    };
    let Value::Object(extra_map) = extra else {
        return Value::Object(base_map);
    };
    for (key, value) in extra_map {
        base_map.insert(key, value);
    }
    Value::Object(base_map)
}

fn mutation_response(
    mutation_id: &str,
    background_tasks_drained: bool,
    receipt: &ResidentCommitReceipt,
    payload_bytes: usize,
) -> Value {
    extend_json(
        commit_core_json(receipt),
        serde_json::json!({
            "mutation_id": mutation_id,
            "record_revision": receipt.record_revisions.get(mutation_id),
            "success": true,
            "background_tasks_drained": background_tasks_drained,
            "convergence_pending": !background_tasks_drained,
            "size": payload_bytes
        }),
    )
}

fn batch_response(
    receipt: &ResidentCommitReceipt,
    count: usize,
    background_tasks_drained: bool,
) -> Value {
    extend_json(
        commit_core_json(receipt),
        serde_json::json!({
            "mutation_ids": &receipt.mutation_ids,
            "count": count,
            "background_tasks_drained": background_tasks_drained,
            "convergence_pending": !background_tasks_drained,
        }),
    )
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
