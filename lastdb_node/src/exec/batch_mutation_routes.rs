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
    let result = match result {
        Ok(value) => match batch_mutation_ids(&value, watch_hints.len()) {
            Ok(mutation_ids) => {
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
                if !durable_events.is_empty() {
                    if let Err(error) = host
                        .db
                        .db_ops()
                        .change_feed()
                        .append_many(durable_events)
                        .await
                    {
                        tracing::error!(
                            error = %error,
                            "failed to append durable app change-feed batch"
                        );
                    }
                }
                Ok(value)
            }
            Err(error) => Err(error),
        },
        Err(error) => Err(error),
    };
    render(result, ctx)
}
