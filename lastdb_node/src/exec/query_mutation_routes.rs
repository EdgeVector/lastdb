use super::*;

// ---------------------------------------------------------------------------
// Query / Mutation — shared handler bodies
// ---------------------------------------------------------------------------

pub(super) async fn execute_query_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    // Keep the complete body-to-query decode inside the existing parse phase.
    let parse_started = std::time::Instant::now();
    let QueryRequest {
        query,
        limit,
        offset,
        cursor,
    } = match body_object(req).and_then(parse_query_body) {
        Ok(query) => query,
        Err(resp) => return resp,
    };
    fold_db::request_phases::add_phase(
        fold_db::request_phases::RequestPhase::Parse,
        parse_started.elapsed(),
    );

    // Admin/offline bulk opt-in for unfiltered full-schema drains. Product apps
    // must use HashKey/HashRange; SDK queryAll requires allowFullScan analogously.
    let allow_full_scan = req
        .header("x-lastdb-allow-full-scan")
        .is_some_and(env_flag::truthy);

    super::query_read_receipt::render_query(
        req,
        ctx,
        handlers::execute_query(host, query, limit, offset, cursor, ctx, allow_full_scan),
    )
    .await
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

pub(super) async fn execute_mutation_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    let components = match parse_mutation_components(req) {
        Ok(components) => components,
        Err(rejection) => return rejection,
    };
    let watch_schema = components.schema.clone();
    let watch_mt = mutation_type_label(&components.mutation_type).to_string();
    let watch_hash = components.key_value.hash.clone();
    let watch_range = components.key_value.range.clone();
    let change_feed_reservation = match host.change_feed_queue.try_reserve() {
        Ok(reservation) => reservation,
        Err(message) => return error_response(503, message, ctx),
    };
    let (result, captured_notifications) = fold_db::db_operations::ChangeFeedStore::capture_native(
        vec![(
            handlers::resolve_schema_name(host, &watch_schema)
                .ok()
                .flatten()
                .unwrap_or_else(|| watch_schema.clone()),
            durable_change_event(
                watch_schema.clone(),
                watch_mt.clone(),
                watch_hash.clone(),
                watch_range.clone(),
                String::new(),
                &Value::Null,
            ),
        )],
        handlers::execute_mutation(host, components, ctx),
    )
    .await;
    let result = result.and_then(|value| {
        let mutation_id = value
            .get("mutation_id")
            .and_then(Value::as_str)
            .ok_or_else(|| HostError::new(500, "Single mutation receipt has no input identity"))?;
        let event = durable_change_event(
            watch_schema.clone(),
            watch_mt.clone(),
            watch_hash.clone(),
            watch_range.clone(),
            mutation_id.to_string(),
            &value,
        );
        // Queue submit is in-memory and cannot wait. Capacity was reserved
        // before product commit, so overload cannot create a committed row
        // without a matching queued hint.
        if !captured_notifications.contains(mutation_id) {
            change_feed_reservation.submit(vec![event]);
        }
        let change_record_started = std::time::Instant::now();
        host.local_outbox
            .append(watch_schema, watch_mt, watch_hash, watch_range);
        fold_db::request_phases::add_phase(
            fold_db::request_phases::RequestPhase::ChangeRecord,
            change_record_started.elapsed(),
        );
        Ok(value)
    });
    let response_started = std::time::Instant::now();
    let response = render(result, ctx);
    fold_db::request_phases::add_phase(
        fold_db::request_phases::RequestPhase::ResponseEnvelope,
        response_started.elapsed(),
    );
    response
}

/// Decode a `/api/mutation` body into the mutation components, or the
/// rejection response to send back.
fn parse_mutation_components(req: &UdsRequest) -> Result<MutationComponents, UdsResponse> {
    let parse_started = std::time::Instant::now();
    // The rendered response from `body_object` is discarded and rebuilt via
    // `reject_mutation`, so the phase and log line land on this path too.
    let Ok(mut obj) = body_object(req) else {
        return Err(reject_mutation(Reject::malformed_body(), parse_started));
    };
    // Name the absent key before serde collapses it into an undifferentiated
    // failure. Without this an internally-tagged `Operation` reports a missing
    // `type` and a missing `mutation_type` identically, and the route answered
    // both with the same 11 bytes.
    if let Some(key) = first_missing_key(&obj, MUTATION_REQUIRED_KEYS) {
        return Err(reject_mutation(Reject::missing(key), parse_started));
    }
    // `aggregate_set` is a mutation sidecar, not an ordinary `Operation`
    // field. Remove and decode it first so the strict operation grammar stays
    // unchanged for every other route.
    let Ok(aggregate_set) = take_aggregate_set(&mut obj) else {
        return Err(reject_mutation(
            Reject::new(RejectKind::InvalidValue),
            parse_started,
        ));
    };
    let parsed = serde_json::from_value::<Operation>(Value::Object(obj));
    fold_db::request_phases::add_phase(
        fold_db::request_phases::RequestPhase::Parse,
        parse_started.elapsed(),
    );
    let operation = match parsed {
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
            return Err(reject.response());
        }
    };

    Ok(MutationComponents::from_operation(
        operation,
        None,
        aggregate_set,
    ))
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
