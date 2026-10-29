use super::mutation_notifications::{single_mutation_id, MutationWatchHint};
use super::query_filters::LocalWatchRequest;
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
        response_format,
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
        response_format.run(handlers::execute_query(
            host,
            query,
            limit,
            offset,
            cursor,
            ctx,
            allow_full_scan,
        )),
    )
    .await
}

pub(super) async fn execute_mutation_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    let parse_started = std::time::Instant::now();
    let parsed = parse_mutation_body(&req.body);
    // Every exit records one complete parse phase. A refusal logs fixed bytes
    // only, so malformed caller data never enters the log or response (I4).
    fold_db::request_phases::add_phase(
        fold_db::request_phases::RequestPhase::Parse,
        parse_started.elapsed(),
    );
    let components = match parsed {
        Ok(components) => components,
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
    let hint = MutationWatchHint::from(&components);
    let change_feed_reservation = match host.change_feed_queue.try_reserve() {
        Ok(reservation) => reservation,
        Err(message) => return error_response(503, message, ctx),
    };
    let (result, captured_notifications) = fold_db::db_operations::ChangeFeedStore::capture_native(
        vec![(
            handlers::resolve_schema_name(host, &hint.schema)
                .ok()
                .flatten()
                .unwrap_or_else(|| hint.schema.clone()),
            hint.event(String::new(), &Value::Null, fold_db::clock::unix_millis()),
        )],
        handlers::execute_mutation(host, components, ctx),
    )
    .await;
    let result = result.and_then(|value| {
        let mutation_id = single_mutation_id(&value)?;
        let event = hint.event(
            mutation_id.to_string(),
            &value,
            fold_db::clock::unix_millis(),
        );
        // Queue submit is in-memory and cannot wait. Capacity was reserved
        // before product commit, so overload cannot create a committed row
        // without a matching queued hint.
        if !captured_notifications.contains(mutation_id) {
            change_feed_reservation.submit(vec![event]);
        }
        let change_record_started = std::time::Instant::now();
        host.local_outbox
            .append(hint.schema, hint.operation, hint.hash, hint.range);
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
    let LocalWatchRequest {
        after_seq,
        timeout,
        schemas,
        hash,
        range_start,
        range_end,
    } = LocalWatchRequest::parse(&req.target, crate::local_outbox::MAX_POLL_TIMEOUT);
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
