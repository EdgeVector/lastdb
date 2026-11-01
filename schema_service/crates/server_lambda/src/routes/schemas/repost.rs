//! Read-only canonical repost response.

use super::*;

pub(super) async fn idempotent_repost_response(
    state: &SchemaServiceState,
    schema: &schema_types::Schema,
    mutation_mappers: &HashMap<String, String>,
) -> Option<Result<Response<Body>, Error>> {
    let existing = match state
        .idempotent_repost_reply(schema, mutation_mappers)
        .await
        .into_http()
    {
        Ok(Some(existing)) => *existing,
        Ok(None) => return None,
        Err((status, body)) => return Some(json_response(status, &body)),
    };
    let system = state.is_system_schema(&existing.name);
    Some(json_response(
        200,
        &super::schema_add_body(&existing, mutation_mappers, None, system),
    ))
}
