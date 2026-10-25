use super::mutation_notifications::{batch_mutation_ids, MutationWatchHint};
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
    let watch_hints: Vec<_> = components.iter().map(MutationWatchHint::from).collect();
    let change_feed_reservation = match host.change_feed_queue.try_reserve() {
        Ok(reservation) => reservation,
        Err(message) => return error_response(503, message, ctx),
    };

    let (result, captured_notifications) = fold_db::db_operations::ChangeFeedStore::capture_native(
        watch_hints
            .iter()
            .map(|hint| {
                let canonical = handlers::resolve_schema_name(host, &hint.schema)
                    .ok()
                    .flatten()
                    .unwrap_or_else(|| hint.schema.clone());
                (
                    canonical,
                    hint.event(String::new(), &Value::Null, fold_db::clock::unix_millis()),
                )
            })
            .collect(),
        handlers::execute_mutations_batch(host, components, ctx),
    )
    .await;
    let result = result.and_then(|value| {
        let mutation_ids = batch_mutation_ids(&value, watch_hints.len())?;
        let mut durable_events = Vec::with_capacity(watch_hints.len());
        let mut change_record_elapsed = std::time::Duration::ZERO;
        for (hint, mutation_id) in watch_hints.into_iter().zip(mutation_ids) {
            if !captured_notifications.contains(mutation_id) {
                durable_events.push(hint.event(
                    mutation_id.to_string(),
                    &value,
                    fold_db::clock::unix_millis(),
                ));
            }
            let change_record_started = std::time::Instant::now();
            host.local_outbox
                .append(hint.schema, hint.operation, hint.hash, hint.range);
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
