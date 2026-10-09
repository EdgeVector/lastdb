use super::*;

// ---------------------------------------------------------------------------
// Query / Mutation — shared handler bodies
// ---------------------------------------------------------------------------

pub(super) async fn execute_query_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    // Parse spans body → typed `Query`, matching what the mutation route
    // charges to `parse`: the pagination/cursor extractors below mutate the
    // same object and are part of turning the wire body into a request.
    let parse_started = std::time::Instant::now();
    let mut obj = match body_object(req) {
        Ok(obj) => obj,
        Err(resp) => return resp,
    };
    let limit = match take_pagination(&mut obj, WireKey::Limit) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let offset = match take_pagination(&mut obj, WireKey::Offset) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let cursor = match take_cursor(&mut obj) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let expected_total_count = match take_pagination(&mut obj, WireKey::ExpectedTotalCount) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    // Diagnose a missing required key before `serde` collapses it into an
    // undifferentiated failure, which is what made an absent `fields` surface
    // as the bare `Bad Request` that cost real minutes to bisect by hand.
    //
    // The list lives in `wire` so one test can pin it against the real `Query`
    // type; see `QUERY_REQUIRED_KEYS` for why `filter` is not on it.
    if let Err(resp) = require_keys(&obj, QUERY_REQUIRED_KEYS) {
        return resp;
    }
    let mut query = match serde_json::from_value::<Query>(Value::Object(obj)) {
        Ok(query) => query,
        // Every required key is present, so this is a value that does not match
        // its grammar, or a key this node's grammar does not know (version
        // skew). Neither the value nor the key is echoed (I4).
        Err(err) => return Reject::from_serde_error(&err, None).response(),
    };
    query.expected_total_count = expected_total_count;
    fold_db::request_phases::add_phase(
        fold_db::request_phases::RequestPhase::Parse,
        parse_started.elapsed(),
    );

    // Admin/offline bulk opt-in for unfiltered full-schema drains. Product apps
    // must use HashKey/HashRange; SDK queryAll requires allowFullScan analogously.
    let allow_full_scan = req
        .header("x-lastdb-allow-full-scan")
        .is_some_and(env_flag::truthy);

    render(
        handlers::execute_query(host, query, limit, offset, cursor, ctx, allow_full_scan).await,
        ctx,
    )
}

pub(super) fn mutation_type_label(mt: &fold_db::schema::types::MutationType) -> &'static str {
    use fold_db::schema::types::MutationType;
    match mt {
        MutationType::Create => "create",
        MutationType::Update => "update",
        MutationType::Delete => "delete",
        MutationType::Purge => "purge",
    }
}

/// Record the parse phase and answer a rejection, so a refused mutation is
/// visible to both instruments that went blind on this route.
///
/// `request_ops` previously showed a rejected mutation as `duration_ms=0`,
/// `schema=-`, and **no** `parse` phase at all — the signature that made 3,473
/// failing kanban mutations (26% of the route) undiagnosable: there was no way
/// to tell a request that died at body read from one that never arrived. The
/// phase is added on every exit, and the refusal is logged from static bytes
/// only, so a 4xx storm leaves a trace without echoing a caller byte (**I4**).
pub(super) fn reject_mutation(reject: Reject, parse_started: std::time::Instant) -> UdsResponse {
    fold_db::request_phases::add_phase(
        fold_db::request_phases::RequestPhase::Parse,
        parse_started.elapsed(),
    );
    tracing::warn!(
        target: "fold_node::http",
        route = "/api/mutation",
        kind = reject.kind().as_str(),
        "mutation request refused on shape"
    );
    reject.response()
}

pub(super) fn take_aggregate_set(
    obj: &mut serde_json::Map<String, Value>,
) -> Result<Option<fold_db::schema::types::AggregateSet>, ()> {
    match obj.remove("aggregate_set") {
        Some(value) => serde_json::from_value(value).map(Some).map_err(|_| ()),
        None => Ok(None),
    }
}

pub(super) fn parse_aggregate_finalize(
    body: &[u8],
) -> Result<fold_db::schema::types::AggregateFinalize, ()> {
    serde_json::from_slice(body).map_err(|_| ())
}

pub(super) fn parse_aggregate_repair(
    body: &[u8],
) -> Result<fold_db::schema::types::AggregateRepair, ()> {
    serde_json::from_slice(body).map_err(|_| ())
}

pub(super) async fn execute_aggregate_repair_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    let parse_started = std::time::Instant::now();
    let Ok(repair) = parse_aggregate_repair(&req.body) else {
        fold_db::request_phases::add_phase(
            fold_db::request_phases::RequestPhase::Parse,
            parse_started.elapsed(),
        );
        tracing::warn!(
            target: "fold_node::http",
            route = "/api/aggregate/repair",
            kind = RejectKind::InvalidValue.as_str(),
            "aggregate repair request refused on shape"
        );
        return Reject::new(RejectKind::InvalidValue).response();
    };
    fold_db::request_phases::add_phase(
        fold_db::request_phases::RequestPhase::Parse,
        parse_started.elapsed(),
    );
    render(
        handlers::execute_aggregate_repair(host, repair, ctx).await,
        ctx,
    )
}

pub(super) async fn execute_aggregate_finalize_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    let parse_started = std::time::Instant::now();
    let Ok(finalize) = parse_aggregate_finalize(&req.body) else {
        fold_db::request_phases::add_phase(
            fold_db::request_phases::RequestPhase::Parse,
            parse_started.elapsed(),
        );
        tracing::warn!(
            target: "fold_node::http",
            route = "/api/aggregate/finalize",
            kind = RejectKind::InvalidValue.as_str(),
            "aggregate finalize request refused on shape"
        );
        return Reject::new(RejectKind::InvalidValue).response();
    };
    fold_db::request_phases::add_phase(
        fold_db::request_phases::RequestPhase::Parse,
        parse_started.elapsed(),
    );
    render(
        handlers::execute_aggregate_finalize(host, finalize, ctx).await,
        ctx,
    )
}

// lint:fn-size-ok moved verbatim from exec.rs; splitting this function is separate work.
pub(super) async fn execute_mutation_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    let parse_started = std::time::Instant::now();
    // The rendered response from `body_object` is discarded and rebuilt via
    // `reject_mutation`, so the phase and log line land on this path too.
    let Ok(mut obj) = body_object(req) else {
        return reject_mutation(Reject::malformed_body(), parse_started);
    };
    // Name the absent key before serde collapses it into an undifferentiated
    // failure. Without this an internally-tagged `Operation` reports a missing
    // `type` and a missing `mutation_type` identically, and the route answered
    // both with the same 11 bytes.
    if let Some(key) = first_missing_key(&obj, MUTATION_REQUIRED_KEYS) {
        return reject_mutation(Reject::missing(key), parse_started);
    }
    // `aggregate_set` is a mutation sidecar, not an ordinary `Operation`
    // field. Remove and decode it first so the strict operation grammar stays
    // unchanged for every other route.
    let Ok(aggregate_set) = take_aggregate_set(&mut obj) else {
        return reject_mutation(Reject::new(RejectKind::InvalidValue), parse_started);
    };
    let parsed = serde_json::from_value::<Operation>(Value::Object(obj));
    fold_db::request_phases::add_phase(
        fold_db::request_phases::RequestPhase::Parse,
        parse_started.elapsed(),
    );
    let Operation::Mutation {
        schema,
        fields_and_values,
        key_value,
        mutation_type,
        source_file_name: _,
        expected,
        convergence,
        durability,
        cloud_publication,
        must_exist,
        key_range_prefix,
    } = match parsed {
        Ok(operation) => operation,
        Err(err) => {
            // Every required key is present, so this is either a value outside
            // its grammar or a key this node does not know — the version-skew
            // signal a newer client needs to branch on. Neither the offending
            // value nor the unknown key is echoed (I4).
            let reject = Reject::from_serde_error(&err, None);
            tracing::warn!(
                target: "fold_node::http",
                route = "/api/mutation",
                kind = reject.kind().as_str(),
                "mutation request refused on shape"
            );
            return reject.response();
        }
    };

    let watch_schema = schema.clone();
    let watch_mt = mutation_type_label(&mutation_type).to_string();
    let watch_hash = key_value.hash.clone();
    let watch_range = key_value.range.clone();
    let components = MutationComponents {
        schema,
        fields_and_values,
        key_value,
        mutation_type,
        expected,
        convergence: convergence.unwrap_or_default(),
        durability: durability.unwrap_or_default(),
        cloud_publication,
        must_exist,
        key_range_prefix,
        aggregate_set,
    };
    let change_feed_reservation = match host.change_feed_queue.try_reserve() {
        Ok(reservation) => reservation,
        Err(message) => return error_response(503, message, ctx),
    };
    let result = handlers::execute_mutation(host, components, ctx).await;
    if let Ok(value) = &result {
        let event = durable_change_event(
            watch_schema.clone(),
            watch_mt.clone(),
            watch_hash.clone(),
            watch_range.clone(),
            mutation_id_at(value, 0),
            value,
        );
        // Queue submit is in-memory and cannot wait. Capacity was reserved
        // before product commit, so overload cannot create a committed row
        // without a matching queued hint.
        change_feed_reservation.submit(vec![event]);
        let change_record_started = std::time::Instant::now();
        host.local_outbox
            .append(watch_schema, watch_mt, watch_hash, watch_range);
        fold_db::request_phases::add_phase(
            fold_db::request_phases::RequestPhase::ChangeRecord,
            change_record_started.elapsed(),
        );
    }
    let response_started = std::time::Instant::now();
    let response = render(result, ctx);
    fold_db::request_phases::add_phase(
        fold_db::request_phases::RequestPhase::ResponseEnvelope,
        response_started.elapsed(),
    );
    response
}

/// `GET /api/local-watch?after_seq=0&timeout_ms=0[&schema=App/Event][&hash=H][&start=R][&end=R]`
///
/// Long-poll for thin mutation doorbell events. Not product truth — clients
/// still keyed-read tables after wake. `gap:true` means resync from product
/// keys (cursor fell behind the short TTL ring). Optional `schema` filters are
/// repeated and comma-separated; an exact `hash` filter narrows a watcher to
/// one HashRange partition. When filters are present, unrelated writes do not
/// wake a blocking waiter, but `tip_seq` still advances to the node tip. The
/// optional `start` and `end` bounds use inclusive-start/exclusive-end order.
pub(super) fn execute_local_watch_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    let target = req.target.as_str();
    let after_seq = query_u64(target, "after_seq").unwrap_or(0);
    let timeout_ms = query_u64(target, "timeout_ms").unwrap_or(0);
    let schemas = query_schema_filter(target);
    let hash = query_string_filter(target, "hash");
    let range_start = query_string_filter_any(target, &["start", "range_start"]);
    let range_end = query_string_filter_any(target, &["end", "range_end"]);
    let timeout = std::time::Duration::from_millis(
        timeout_ms.min(crate::local_outbox::MAX_POLL_TIMEOUT.as_millis() as u64),
    );
    // A blocking wait parks THIS UDS worker thread for the whole timeout and
    // takes no QoS permit, so uncapped watchers can occupy the entire pool
    // while `lastdb status` still reports the node idle. Admit through the
    // watcher gate and shed explicitly over the cap rather than silently
    // consuming a worker. `timeout_ms=0` holds a worker only for the read
    // itself, so it is never gated — and it is the fallback a shed client
    // drops to.
    let _watch_slot = if timeout.is_zero() {
        None
    } else {
        let Some(slot) = host.watch_gate.try_acquire() else {
            let snap = host.watch_gate.snapshot();
            tracing::warn!(
                active = snap.active,
                max = snap.max,
                sheds = snap.sheds,
                "local-watch long-poll shed: watcher cap full"
            );
            return error_response(
                503,
                &format!(
                    "node is busy: all {} long-poll watcher slots in use; \
                     retry after 1s or poll with timeout_ms=0",
                    snap.max
                ),
                ctx,
            );
        };
        Some(slot)
    };
    let poll = host.local_outbox.poll_after_schemas_hash_range(
        after_seq,
        timeout,
        schemas.as_ref(),
        hash.as_deref(),
        range_start.as_deref(),
        range_end.as_deref(),
    );
    render(Ok(serde_json::to_value(poll).unwrap_or_else(|_| {
        serde_json::json!({"after_seq": after_seq, "tip_seq": 0, "gap": false, "events": []})
    })), ctx)
}

pub(super) fn query_schema_filter(target: &str) -> Option<HashSet<String>> {
    query_multi_string_filter(target, "schema")
}

pub(super) fn query_multi_string_filter(target: &str, wanted_key: &str) -> Option<HashSet<String>> {
    let q = target.split_once('?')?.1;
    let mut schemas = HashSet::new();
    for pair in q.split('&') {
        let Some((k, v)) = pair.split_once('=') else {
            continue;
        };
        if k != wanted_key {
            continue;
        }
        for schema in v.split(',').map(str::trim).filter(|s| !s.is_empty()) {
            schemas.insert(schema.to_string());
        }
    }
    if schemas.is_empty() {
        None
    } else {
        Some(schemas)
    }
}

pub(super) fn query_string_filter(target: &str, wanted_key: &str) -> Option<String> {
    query_string_filter_any(target, &[wanted_key])
}

pub(super) fn query_string_filter_any(target: &str, wanted_keys: &[&str]) -> Option<String> {
    let q = target.split_once('?')?.1;
    q.split('&').find_map(|pair| {
        let (key, value) = pair.split_once('=')?;
        (wanted_keys.contains(&key) && !value.is_empty()).then(|| percent_decode(value))
    })
}

pub(super) fn query_u64(target: &str, key: &str) -> Option<u64> {
    let q = target.split_once('?')?.1;
    for pair in q.split('&') {
        let (k, v) = pair.split_once('=')?;
        if k == key {
            return v.parse().ok();
        }
    }
    None
}

#[derive(Deserialize)]
pub(super) struct BatchMutationsRequest {
    pub(super) mutations: Vec<Operation>,
    #[serde(default)]
    pub(super) convergence: Option<MutationConvergence>,
}

pub(super) fn mutation_components_from_operation(
    operation: Operation,
    default_convergence: Option<MutationConvergence>,
) -> MutationComponents {
    let Operation::Mutation {
        schema,
        fields_and_values,
        key_value,
        mutation_type,
        source_file_name: _,
        expected,
        convergence,
        durability,
        cloud_publication,
        must_exist,
        key_range_prefix,
    } = operation;

    MutationComponents {
        schema,
        fields_and_values,
        key_value,
        mutation_type,
        expected,
        convergence: convergence.or(default_convergence).unwrap_or_default(),
        durability: durability.unwrap_or_default(),
        cloud_publication,
        must_exist,
        key_range_prefix,
        aggregate_set: None,
    }
}

pub(super) fn parse_batch_mutation_components(
    req: &UdsRequest,
) -> Result<Vec<MutationComponents>, UdsResponse> {
    let Ok(value) = serde_json::from_slice::<Value>(&req.body) else {
        return Err(content_free(400, "Bad Request"));
    };

    // A strict `Operation` refusing a key it does not declare is the
    // client↔node version-skew signal (`unknown_key`), not a malformed body:
    // brain 0.8.0 sent per-mutation `durability` to a primary that predated
    // the key and got the same bare `Bad Request` as a typo (2026-09-03).
    let operations = match value {
        Value::Array(items) => {
            let mut operations = Vec::with_capacity(items.len());
            for item in items {
                match serde_json::from_value::<Operation>(item) {
                    Ok(operation) => operations.push(operation),
                    Err(err) => return Err(Reject::from_serde_error(&err, None).response()),
                }
            }
            operations
        }
        other => match serde_json::from_value::<BatchMutationsRequest>(other) {
            Ok(request) => {
                return Ok(request
                    .mutations
                    .into_iter()
                    .map(|operation| {
                        mutation_components_from_operation(operation, request.convergence)
                    })
                    .collect());
            }
            Err(err) => return Err(Reject::from_serde_error(&err, None).response()),
        },
    };

    Ok(operations
        .into_iter()
        .map(|operation| mutation_components_from_operation(operation, None))
        .collect())
}

pub(super) async fn execute_mutations_batch_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    // One timer around the whole batch parse: the loop inside parses every
    // item, so this is already the per-item parse SUM, not a point sample.
    let parse_started = std::time::Instant::now();
    let parsed = parse_batch_mutation_components(req);
    fold_db::request_phases::add_phase(
        fold_db::request_phases::RequestPhase::Parse,
        parse_started.elapsed(),
    );
    let components = match parsed {
        Ok(components) => components,
        Err(resp) => return resp,
    };
    // Capture thin doorbell hints before move into the batch write.
    let watch_hints = mutation_watch_hints(&components);
    let change_feed_reservation = match host.change_feed_queue.try_reserve() {
        Ok(reservation) => reservation,
        Err(message) => return error_response(503, message, ctx),
    };

    let result = handlers::execute_mutations_batch(host, components, ctx).await;
    if let Ok(value) = &result {
        let mut durable_events = Vec::with_capacity(watch_hints.len());
        let mut change_record_elapsed = std::time::Duration::ZERO;
        for (index, (schema, mt, hash, range)) in watch_hints.into_iter().enumerate() {
            durable_events.push(durable_change_event(
                schema.clone(),
                mt.clone(),
                hash.clone(),
                range.clone(),
                mutation_id_at(value, index),
                value,
            ));
            let change_record_started = std::time::Instant::now();
            host.local_outbox.append(schema, mt, hash, range);
            change_record_elapsed += change_record_started.elapsed();
        }
        change_feed_reservation.submit(durable_events);
        fold_db::request_phases::add_phase(
            fold_db::request_phases::RequestPhase::ChangeRecord,
            change_record_elapsed,
        );
    }
    let response_started = std::time::Instant::now();
    let response = render(result, ctx);
    fold_db::request_phases::add_phase(
        fold_db::request_phases::RequestPhase::ResponseEnvelope,
        response_started.elapsed(),
    );
    response
}

pub(super) type MutationWatchHint = (String, String, Option<String>, Option<String>);

pub(super) fn mutation_watch_hints(components: &[MutationComponents]) -> Vec<MutationWatchHint> {
    // These values must outlive the ownership transfer into the awaited batch
    // execution so that successful commits can emit their durable hints.
    components
        .iter()
        .map(|component| {
            (
                component.schema.clone(),
                mutation_type_label(&component.mutation_type).to_string(),
                component.key_value.hash.clone(),
                component.key_value.range.clone(),
            )
        })
        .collect()
}

pub(super) fn mutation_id_at(value: &Value, index: usize) -> String {
    value
        .get("mutation_ids")
        .and_then(Value::as_array)
        .and_then(|ids| ids.get(index).or_else(|| ids.first()))
        .and_then(Value::as_str)
        .or_else(|| {
            (index == 0)
                .then(|| value.get("mutation_id").and_then(Value::as_str))
                .flatten()
        })
        .unwrap_or("")
        .to_string()
}

pub(super) fn durable_change_event(
    schema: String,
    operation: String,
    hash: Option<String>,
    range: Option<String>,
    mutation_id: String,
    result: &Value,
) -> fold_db::db_operations::ChangeFeedEvent {
    let committed_at_ms = fold_db::clock::unix_millis();
    let background_tasks_drained = result
        .get("background_tasks_drained")
        .and_then(Value::as_bool)
        .unwrap_or(true);
    let convergence_pending = result
        .get("convergence_pending")
        .and_then(Value::as_bool)
        .unwrap_or(!background_tasks_drained);
    fold_db::db_operations::ChangeFeedEvent {
        seq: 0,
        mutation_id,
        schema,
        operation,
        hash,
        range,
        committed_at_ms,
        background_tasks_drained,
        convergence_pending,
    }
}
// lint:file-size-ok moved verbatim from exec.rs; cohesive unit, split further in a later pass
