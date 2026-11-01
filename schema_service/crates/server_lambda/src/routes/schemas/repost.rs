//! Read-only canonical repost response.

use schema_service_core::state::IdempotentRepostReply;

use super::*;

pub(super) async fn idempotent_repost_response(
    state: &SchemaServiceState,
    schema: &schema_types::Schema,
    mutation_mappers: &HashMap<String, String>,
) -> Option<Result<Response<Body>, Error>> {
    let existing = match state
        .idempotent_repost_reply(schema, mutation_mappers)
        .await
    {
        IdempotentRepostReply::Hit(existing) => *existing,
        IdempotentRepostReply::Miss => return None,
        IdempotentRepostReply::Failed { body } => return Some(json_response(500, &body)),
    };
    let system = state.is_system_schema(&existing.name);
    Some(json_response(
        200,
        &super::schema_add_body(&existing, mutation_mappers, None, system),
    ))
}
