use super::*;

/// Public schema admission is narrower than catalog metadata decoding.
pub(crate) fn logical_layout_error(
    ctx: &AccessContext,
    schema: &DeclarativeSchemaDefinition,
) -> Option<UdsResponse> {
    fold_db::schema::require_logical_layout(&schema.schema_type)
        .err()
        .map(|error| error_response(400, &error.to_string(), ctx))
}

pub(super) fn sync_admission_error(
    ctx: &AccessContext,
    declare: &DirectDeclareProposal,
) -> Option<UdsResponse> {
    if crate::ephemeral::is_ephemeral() {
        return Some(error_response(
            403,
            "schema catalog synchronization is not permitted on ephemeral nodes; \
             use check intent for read-only inspection or run against the primary node",
            ctx,
        ));
    }
    logical_layout_error(ctx, &declare.catalog_schema).or_else(|| {
        validate_declare_retention(declare)
            .err()
            .map(|error| error_response(409, &error, ctx))
    })
}
