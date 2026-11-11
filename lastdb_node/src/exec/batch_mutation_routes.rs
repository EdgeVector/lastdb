use super::mutation_notifications::{batch_mutation_ids, MutationWatchHint};
use super::*;

pub(super) async fn execute_mutations_batch_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    let components = match parse_batch_mutation_body(&req.body) {
        Ok(components) => components,
        Err(resp) => return resp,
    };
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
        handlers::execute_mutations_batch(host, components),
    )
    .await;
    let result = result.and_then(|value| {
        let mutation_ids = batch_mutation_ids(&value, watch_hints.len())?;
        let mut durable_events = Vec::with_capacity(watch_hints.len());
        for (hint, mutation_id) in watch_hints.into_iter().zip(mutation_ids) {
            if !captured_notifications.contains(mutation_id) {
                durable_events.push(hint.event(
                    mutation_id.to_string(),
                    &value,
                    fold_db::clock::unix_millis(),
                ));
            }
        }
        change_feed_reservation.submit(durable_events);
        Ok(value)
    });
    render(result, ctx)
}
