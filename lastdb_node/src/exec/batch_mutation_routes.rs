use super::*;

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
        source_file_name,
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
        source_file_name,
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
