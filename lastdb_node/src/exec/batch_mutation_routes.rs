use super::*;

pub(super) async fn execute_mutations_batch_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    // One timer around the whole batch parse: the loop inside parses every
    // item, so this is already the per-item parse SUM, not a point sample.
    let parse_started = std::time::Instant::now();
    let parsed = parse_batch_mutation_body(&req.body);
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

    let (result, captured_notifications) = fold_db::db_operations::ChangeFeedStore::capture_native(
        watch_hints
            .iter()
            .map(|(schema, operation, hash, range)| {
                let canonical = handlers::resolve_schema_name(host, schema)
                    .ok()
                    .flatten()
                    .unwrap_or_else(|| schema.clone());
                (
                    canonical,
                    durable_change_event(
                        schema.clone(),
                        operation.clone(),
                        hash.clone(),
                        range.clone(),
                        String::new(),
                        &Value::Null,
                    ),
                )
            })
            .collect(),
        handlers::execute_mutations_batch(host, components, ctx),
    )
    .await;
    let result = result.and_then(|value| {
        let mutation_ids = value
            .get("mutation_ids")
            .and_then(Value::as_array)
            .filter(|ids| ids.len() == watch_hints.len())
            .and_then(|ids| ids.iter().map(Value::as_str).collect::<Option<Vec<_>>>())
            .ok_or_else(|| {
                HostError::new(500, "Batch receipt does not match its input identities")
            })?;
        let mut durable_events = Vec::with_capacity(watch_hints.len());
        let mut change_record_elapsed = std::time::Duration::ZERO;
        for ((schema, mt, hash, range), mutation_id) in watch_hints.into_iter().zip(mutation_ids) {
            if !captured_notifications.contains(mutation_id) {
                durable_events.push(durable_change_event(
                    schema.clone(),
                    mt.clone(),
                    hash.clone(),
                    range.clone(),
                    mutation_id.to_string(),
                    &value,
                ));
            }
            let change_record_started = std::time::Instant::now();
            host.local_outbox.append(schema, mt, hash, range);
            change_record_elapsed += change_record_started.elapsed();
        }
        change_feed_reservation.submit(durable_events);
        fold_db::request_phases::add_phase(
            fold_db::request_phases::RequestPhase::ChangeRecord,
            change_record_elapsed,
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
