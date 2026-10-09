//! Attribution scope/event construction for the write pipeline.

use std::collections::HashSet;

use sha2::{Digest, Sha256};

use crate::db_operations::{
    AttributionEvent, AttributionObjectKind, AttributionPath, AttributionPendingScope,
    AttributionRootKind, LIVE_ATTRIBUTION_EPOCH_ID,
};
use crate::schema::types::{Mutation, MutationType};
use crate::schema::SchemaError;

pub(super) fn attribution_operation(mutation_type: MutationType) -> &'static str {
    match mutation_type {
        MutationType::Create => "create",
        MutationType::Update => "update",
        MutationType::Delete => "delete",
        MutationType::Purge => "purge",
    }
}

/// Whether request and replay writes record attribution source events.
///
/// Off by default (2026-09-22). The source boundary from #2134 costs two
/// fsync-grade flushes per mutation (`begin_pending_scopes`, then the combined
/// `append_events_and_clear_pending_scopes`): the safe-upgrade latency bar measured a hot
/// `brain put` at 1,065 ms on 0.23.3-2154 against 210 ms on 0.23.3-2127 on
/// the same CoW data, and refused the cutover. That refusal was correct, but
/// it also blocked the flush-storm fix (#2136) the primary needed. Until the
/// boundary is batched into the product write's own durability, it is opt-in:
/// `LASTDB_ATTRIBUTION_SOURCE_EVENTS=1`. With it off, no pending scope and no
/// event is written, so attribution can never call a walk complete — the
/// reclaim path stays fail-closed, which is the design's stated fallback.
/// Card: lastdb-attribution-pr2134-review-repair.
pub(super) fn attribution_source_events_enabled() -> bool {
    // Read per batch, not once: one env read is nothing next to the flushes
    // it gates.
    env_flag::var_truthy("LASTDB_ATTRIBUTION_SOURCE_EVENTS")
}

pub(super) fn attribution_scopes(
    mutations: &[Mutation],
    storage_prefix: Option<&str>,
) -> Result<Vec<AttributionPendingScope>, SchemaError> {
    let scopes: Vec<AttributionPendingScope> = mutations
        .iter()
        .map(|mutation| {
            let mutation_id = if mutation.uuid.trim().is_empty() {
                let encoded = serde_json::to_vec(mutation).map_err(|error| {
                    SchemaError::InvalidData(format!("encode legacy attribution mutation: {error}"))
                })?;
                let digest = Sha256::digest(&encoded);
                format!("legacy:{digest:x}")
            } else {
                mutation.uuid.clone()
            };
            Ok(AttributionPendingScope::new(
                mutation_id,
                mutation.schema_name.clone(),
                attribution_operation(mutation.mutation_type),
                mutation.key_value.hash.clone(),
                mutation.key_value.range.clone(),
            )
            .with_storage_prefix(storage_prefix.map(str::to_string)))
        })
        .collect::<Result<Vec<_>, SchemaError>>()?;
    let mut mutation_ids = HashSet::with_capacity(scopes.len());
    for scope in &scopes {
        if scope.mutation_id.trim().is_empty() {
            return Err(SchemaError::InvalidData(
                "attribution source requires a mutation id".to_string(),
            ));
        }
        if !mutation_ids.insert(&scope.mutation_id) {
            return Err(SchemaError::InvalidData(format!(
                "one write batch repeats mutation id {}",
                scope.mutation_id
            )));
        }
    }
    Ok(scopes)
}

/// The live per-write attribution path for one mutation's pending scope.
///
/// Written under [`LIVE_ATTRIBUTION_EPOCH_ID`] — a fixed sentinel, not a
/// resumable backfill epoch — so every request or replay write leaves durable
/// root evidence before its response returns, independent of whether a batch
/// epoch walk is running. `scope.mutation_id` is already validated non-empty
/// and unique within the batch (`attribution_scopes`), so it is a stable,
/// collision-free object identity without a second hash.
pub(super) fn live_write_attribution_path(scope: &AttributionPendingScope) -> AttributionPath {
    AttributionPath::new(
        LIVE_ATTRIBUTION_EPOCH_ID,
        AttributionObjectKind::Tip,
        scope.mutation_id.clone(),
        AttributionRootKind::Schema,
        format!("schema:{}:field:write", scope.schema),
        format!("live-write:{}", scope.mutation_id),
    )
}

pub(super) fn attribution_events(scopes: &[AttributionPendingScope]) -> Vec<AttributionEvent> {
    scopes
        .iter()
        .map(|scope| {
            AttributionEvent::new(
                scope.mutation_id.clone(),
                scope.schema.clone(),
                scope.operation.clone(),
                scope.hash.clone(),
                scope.range.clone(),
            )
            .with_storage_prefix(scope.storage_prefix.clone())
        })
        .collect()
}
