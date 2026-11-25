//! Stamp, validate, and commit mutations built by the app API.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;

use fold_db::fold_db_core::mutation_manager::{MutationManager, ResidentCommitReceipt};
use fold_db::schema::types::{Mutation, SchemaError};
use serde_json::{json, Value};

use super::reply::HostError;
use super::schema_routes::resolve_schema_name;
use crate::host::Host;

const POISONED_READ_VIEW: &str =
    "Storage backend error: read view lock poisoned; restart process to reopen home";

async fn stamp_and_validate(host: &Host, mutation: &mut Mutation) -> Result<(), HostError> {
    let req = mutation.schema_name.clone();
    let canonical = resolve_schema_name(host, &req)?
        .ok_or_else(|| HostError::new(404, format!("Schema not found: {req}")))?;
    validate_mutation_fields(host, &req, &canonical, &mutation.record_fields).await?;
    mutation.schema_name = canonical;
    mutation.uuid = uuid::Uuid::new_v4().to_string();
    Ok(())
}

/// Stamp, validate and commit mutations built by the app API as one transaction.
pub(in crate::exec) async fn commit_mutations(
    host: &Host,
    mut mutations: Vec<Mutation>,
) -> Result<ResidentCommitReceipt, HostError> {
    for (index, mutation) in mutations.iter_mut().enumerate() {
        stamp_and_validate(host, mutation)
            .await
            .map_err(|error| error.at(index))?;
    }
    run_core_commit(host, mutations, "Write failed").await
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
