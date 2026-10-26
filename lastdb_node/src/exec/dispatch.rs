use super::*;

mod routes_core;
mod routes_maintenance;
mod routes_sync_org;
mod telemetry_phases;

use routes_core::*;
use routes_maintenance::*;
use routes_sync_org::*;
use telemetry_phases::phase_timings;

/// Execute a recognized control-socket data route against the minimal host,
/// with the caller's user id bound as the task-local user context (parity with
/// the full node's `run_route_with_user`, so background indexing spawned by a
/// mutation is attributed to the owner).
///
/// Every completed route is recorded into [`crate::request_telemetry`] using
/// the self-reported `X-LastDB-Client` header (or `X-App-Id` fallback), then
/// the schema owner, then the UDS peer process — so an unlabeled caller is
/// still nameable in `lastdb ops`.
pub async fn execute_data_route(
    route: DataRoute,
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    execute_observed_route(
        route,
        req,
        ctx,
        host,
        dispatch_data_route(route, req, ctx, host),
    )
    .await
}

/// Keep attribution and request scopes identical for synchronous dispatch and
/// the narrow, Send mutation executor. Unrelated routes need not become Send.
pub(super) async fn execute_observed_route<F>(
    route: DataRoute,
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
    work: F,
) -> UdsResponse
where
    F: std::future::Future<Output = UdsResponse>,
{
    let user_hash = ctx.user_id.clone();
    // Read the peer identity BEFORE dispatch. A short-lived caller (a `curl`
    // in a poll loop) can exit while a slow route is still running, and then
    // the process-name lookup returns `None` for exactly the requests worth
    // attributing. The pid itself is kernel-set at accept time and stays valid
    // either way.
    let peer_pid = ctx.peer_pid;
    let peer_comm = peer_pid.and_then(crate::request_telemetry::peer_comm_from_pid);
    let client = crate::request_telemetry::client_from_request(req);
    let request_id = crate::request_telemetry::request_id_from_request(req);
    let kind = op_kind_for_route(route);
    let schema = schema_hint_for_route(route, req);
    let client = attribute_client(
        client,
        host,
        schema.as_deref(),
        peer_pid,
        peer_comm.as_deref(),
    );
    let body_bytes = req.body.len() as u64;
    // Async mutations inherit task-local admission time. Synchronous routes
    // use the worker-local pickup time through the same accessor.
    let queue_wait_us = current_socket_queue_wait().map_or(0, fold_db::request_phases::duration_us);
    let started = std::time::Instant::now();

    // Phase accumulator scope: mutation-path recording sites (parse here in
    // exec, resolve/validate/admission/index-wait in `lastdb_host::handlers`,
    // apply/persist/flush lifted from fold_db core's timing breakdown) add
    // into this request's totals. Non-mutation routes add nothing, so their
    // samples keep an absent phase set by construction.
    let ((resp, phase_totals, work_counts, tip_keys), read_receipt) =
        fold_db::read_receipts::observe(Box::pin(
            fold_db::request_phases::run_with_phases_and_keys(
                fold_db::user_context::run_with_user(&user_hash, work),
            ),
        ))
        .await;

    // Skip recording the status probe itself when it is the ops dashboard
    // poll — still record it, but keep schema empty. Always record so
    // lastdb-status callers show up if they hammer the node.
    let duration_ms = started.elapsed().as_millis() as u64;
    let rows = crate::request_telemetry::rows_from_response_body(resp.status, &resp.body);
    let cold_shard_loads = Some(read_receipt.group_opens);
    let path = Some(crate::request_telemetry::path_from_request(
        req.method.as_str(),
        req.target.as_str(),
    ));
    let route_label = Some(route_label(route));
    let uds_snapshot = host
        .uds_workers
        .get()
        .map(lastdb_uds::UdsWorkerPool::snapshot);
    host.request_telemetry.record_with_tip_keys(
        crate::request_telemetry::OpSample {
            ts_ms: fold_db::clock::unix_millis(),
            client,
            request_id,
            kind,
            schema,
            duration_ms,
            status: resp.status,
            rows,
            body_bytes,
            cold_shard_loads,
            phases: phase_timings(queue_wait_us, &phase_totals),
            molecules_persisted: work_counts.molecules_persisted,
            partition_read_rejections: work_counts.partition_read_rejections,
            all_group_walks: work_counts.all_group_walks,
            molecule_store_commits: work_counts.molecule_store_commits,
            resident_commits: work_counts.resident_commits,
            resident_operations: work_counts.resident_operations,
            uds_in_flight: uds_snapshot.map(|snap| snap.in_flight),
            uds_workers: uds_snapshot.map(|snap| snap.workers),
            uds_queue_capacity: uds_snapshot.map(|snap| snap.queue_capacity),
            path,
            route: route_label,
            peer_pid,
            peer_comm,
        },
        &tip_keys,
    );
    resp
}

/// Resolve the telemetry client label: the self-reported name, else the
/// schema owner, else the process on the other end of the socket.
fn attribute_client(
    client: String,
    host: &Host,
    schema: Option<&str>,
    peer_pid: Option<i32>,
    peer_comm: Option<&str>,
) -> String {
    let schema_owner = if client == "unknown" {
        schema_owner_for_hint(host, schema)
    } else {
        None
    };
    let client = crate::request_telemetry::client_or_schema_owner(client, schema_owner.as_deref());
    // Last rung: name the process on the other end of the socket. Only a
    // request whose peer pid the kernel would not report stays `"unknown"`.
    crate::request_telemetry::client_or_peer(client, peer_pid, peer_comm)
}

/// Bounded identity label for the route the router already matched.
///
/// `OpSample::path` is the raw target, so it cannot be an aggregation key:
/// `/api/atom/<id>`, `/api/history/<key>`, `/api/schema/<name>` and
/// `/api/app/blob/cas/sha256/<digest>` put caller data in the path and would
/// mint one telemetry key per atom, per key, per schema and per digest.
/// [`DataRoute`] is the opposite by construction — its own doc says the
/// variant "carries only route identity, with no caller data" — so the set of
/// labels is bounded by the route table and a label is safe in a key.
///
/// The label is the variant name. It is derived rather than hand-written on
/// purpose: a hand-written table next to [`op_kind_for_route`] is a second
/// route table, and a route added without touching it would silently land in
/// whatever bucket the fallback names — which is the defect this label exists
/// to end.
pub(super) fn route_label(route: DataRoute) -> String {
    format!("{route:?}")
}

pub(super) async fn dispatch_data_route(
    route: DataRoute,
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    if let Some(response) = dispatch_core(route, req, ctx, host).await {
        return response;
    }
    if let Some(response) = dispatch_search_app(route, req, ctx, host).await {
        return response;
    }
    if let Some(response) = dispatch_maintenance_gc(route, req, ctx, host).await {
        return response;
    }
    if let Some(response) = dispatch_maintenance_repair(route, req, ctx, host).await {
        return response;
    }
    if let Some(response) = dispatch_blob_identity(route, req, ctx, host).await {
        return response;
    }
    if let Some(response) = dispatch_org_catalog(route, req, ctx, host).await {
        return response;
    }
    if let Some(response) = dispatch_sync_deliver(route, req, ctx, host).await {
        return response;
    }
    // Every `DataRoute` is owned by one group above; an unowned route is
    // answered like an unrecognized path (content-free 404).
    content_free(404, "Not Found")
}

pub(super) fn schema_owner_for_hint(host: &Host, schema: Option<&str>) -> Option<String> {
    let hinted = schema?;
    let canonical = handlers::resolve_schema_name(host, hinted)
        .ok()
        .flatten()
        .unwrap_or_else(|| hinted.to_string());
    host.db
        .schema_manager()
        .get_schema_metadata(&canonical)
        .ok()
        .flatten()
        .and_then(|mut schema| schema.owner_app_id.take())
        .filter(|owner| !owner.trim().is_empty())
}

pub(super) fn op_kind_for_route(route: DataRoute) -> crate::request_telemetry::OpKind {
    use crate::request_telemetry::OpKind;
    match route {
        // `ProteinGet` / `ProteinOfMolecule` are reads and must rank as such:
        // counting them as mutations inflated `kanban verb=mutation` and made
        // the offender tables name the wrong op class during triage.
        DataRoute::Query
        | DataRoute::ListRecordKeys
        | DataRoute::ProteinGet
        | DataRoute::ProteinOfMolecule => OpKind::Query,
        DataRoute::Mutation | DataRoute::AggregateRepair | DataRoute::AggregateFinalize => {
            OpKind::Mutation
        }
        DataRoute::MutationBatch => OpKind::MutationBatch,
        DataRoute::QueryBatch | DataRoute::ConflictMetadata => OpKind::QueryBatch,
        DataRoute::Status
        | DataRoute::AutoIdentity
        | DataRoute::BootIdentity
        | DataRoute::BootLedger => OpKind::Status,
        DataRoute::ListSchemas
        | DataRoute::GetSchema
        | DataRoute::DeclareSchema
        | DataRoute::SeedSystemSchema
        | DataRoute::RetireSchemaNameClaim
        | DataRoute::DropSchema => OpKind::Schema,
        DataRoute::NativeIndexSearch | DataRoute::SearchAppQuery | DataRoute::AppSearch => {
            OpKind::Search
        }
        DataRoute::MoleculeHistory | DataRoute::AppChanges => OpKind::History,
        DataRoute::AtomContent => OpKind::Atom,
        DataRoute::DeliverSnapshot | DataRoute::DeliverList | DataRoute::DeliverReject => {
            OpKind::Deliver
        }
        // Idle long-polls: wall time is mostly sleep, not DB work. Split so
        // `lastdb ops` does not rank them as opaque lastgit "other" latency.
        DataRoute::LocalWatch => OpKind::LocalWatch,
        DataRoute::DbPutBlobLocal | DataRoute::DbFetchFileBlob => OpKind::FileBlob,
        _ => OpKind::Other,
    }
}

pub(super) fn schema_hint_for_route(route: DataRoute, req: &UdsRequest) -> Option<String> {
    use crate::request_telemetry::{schema_from_body, SchemaBodyShape};

    match route {
        DataRoute::Query | DataRoute::Mutation => {
            schema_from_body(&req.body, SchemaBodyShape::Single)
        }
        DataRoute::AggregateRepair | DataRoute::AggregateFinalize => {
            serde_json::from_slice::<Value>(&req.body)
                .ok()
                .and_then(|body| {
                    body.get("target_schema_name")
                        .and_then(Value::as_str)
                        .map(str::to_string)
                })
        }
        DataRoute::MutationBatch => schema_from_body(&req.body, SchemaBodyShape::MutationBatch),
        DataRoute::QueryBatch => schema_from_body(&req.body, SchemaBodyShape::QueryBatch),
        DataRoute::GetSchema => {
            // /api/schema/{name} — single path segment only.
            path_tail(req.target.as_str(), "/api/schema/")
        }
        DataRoute::ListRecordKeys => query_value(&req.target, "schema"),
        _ => None,
    }
}
// lint:file-size-ok moved verbatim from exec.rs; cohesive unit, split further in a later pass
