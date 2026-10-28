//! Response for complete coverage by approved component schemas.
use super::*;

/// A registration whose every field is covered by existing components needs
/// no new canonical: answer with the matched shared schema, or with the
/// proposal itself marked `composed`. `None` when residue fields remain.
pub(super) fn full_component_cover_response(
    state: &SchemaServiceState,
    request: &AddSchemaRequest,
    composition: &SchemaRegistrationComposition,
) -> Option<HttpResponse> {
    if !composition.residue_fields.is_empty() {
        return None;
    }
    state.record_schema_match_outcome("component_cover", &request.schema_match_source, None);
    if let Some(hash) = composition.matched_shared_schema_hash.as_deref() {
        match state.get_schema_by_name(hash) {
            Ok(Some(schema)) => {
                let system = state.is_system_schema(&schema.name);
                return Some(HttpResponse::Ok().json(AddSchemaResponse {
                    schema,
                    mutation_mappers: request.mutation_mappers.clone(),
                    replaced_schema: None,
                    system,
                    composed: false,
                    composition: Some(composition.clone()),
                }));
            }
            Ok(None) => {}
            Err(e) => return Some(internal_error("Failed to fetch covered schema", e)),
        }
    }
    let mut normalized = match state.normalized_schema_field_vocabulary(&request.schema) {
        Ok(schema) => schema,
        Err(error) => return Some(internal_error("Cannot normalize covered schema", error)),
    };
    normalized.compute_identity_hash();
    let system = state.is_system_schema(&normalized.name);
    Some(HttpResponse::Ok().json(AddSchemaResponse {
        schema: normalized,
        mutation_mappers: request.mutation_mappers.clone(),
        replaced_schema: None,
        system,
        composed: true,
        composition: Some(composition.clone()),
    }))
}
