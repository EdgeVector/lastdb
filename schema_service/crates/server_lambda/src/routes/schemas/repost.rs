//! Read-only canonical repost response.

use super::*;

pub(super) async fn idempotent_repost_response(
    state: &SchemaServiceState,
    schema: &schema_types::Schema,
    mutation_mappers: &HashMap<String, String>,
) -> Option<Result<Response<Body>, Error>> {
    let existing = match state
        .lookup_idempotent_repost(schema, mutation_mappers)
        .await
    {
        Ok(Some(existing)) => existing,
        Ok(None) => return None,
        Err(error) => {
            return Some(json_response(
                500,
                &json!({"error": format!("Failed to read existing schema: {error}")}),
            ));
        }
    };
    let system = state.is_system_schema(&existing.name);
    Some(json_response(
        200,
        &json!({
            "schema": existing,
            "mutation_mappers": mutation_mappers,
            "system": system,
        }),
    ))
}
