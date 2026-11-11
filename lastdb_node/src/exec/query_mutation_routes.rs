use super::mutation_notifications::{single_mutation_id, MutationWatchHint};
use super::*;

pub(super) async fn execute_query_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
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

    let allow_full_scan = req
        .header("x-lastdb-allow-full-scan")
        .is_some_and(env_flag::truthy);

    render(
        response_format
            .run(handlers::execute_query(
                host,
                query,
                limit,
                offset,
                cursor,
                allow_full_scan,
            ))
            .await,
        ctx,
    )
}

pub(super) async fn execute_mutation_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    let components = match parse_mutation_body(&req.body) {
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
    let result = commit_mutation_components(host, components).await;
    render(result, ctx)
}

/// Commit one parsed mutation with its change feed event, as `/api/mutation`
/// does. `put-blob` and `delete-blob` use it too.
pub(super) async fn commit_mutation_components(
    host: &Host,
    components: handlers::MutationComponents,
) -> Result<Value, HostError> {
    let hint = MutationWatchHint::from(&components);
    let (result, captured_notifications) = fold_db::db_operations::ChangeFeedStore::capture_native(
        vec![(
            handlers::resolve_schema_name(host, &hint.schema)
                .ok()
                .flatten()
                .unwrap_or_else(|| hint.schema.clone()),
            hint.event(String::new(), &Value::Null, fold_db::clock::unix_millis()),
        )],
        handlers::execute_mutation(host, components),
    )
    .await;
    let value = result?;
    let mutation_id = single_mutation_id(&value)?.to_string();
    if !captured_notifications.contains(&mutation_id) {
        let event = hint.event(mutation_id, &value, fold_db::clock::unix_millis());
        if let Err(error) = host
            .db
            .db_ops()
            .change_feed()
            .append_many(vec![event])
            .await
        {
            tracing::error!(error = %error, "failed to append durable app change-feed event");
        }
    }
    Ok(value)
}
