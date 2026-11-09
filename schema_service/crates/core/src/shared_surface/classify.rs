use schema_types::{Schema, SchemaSource};

/// Decide whether a registered schema belongs in the shared-only resolver
/// projection.
///
/// A schema stays when `superseded_by` is absent or blank after trim, and
/// either `is_system_schema` is true or the source is `SystemSeed` or
/// `StarterSeed`.
pub fn include_in_shared_only_projection(schema: &Schema, is_system_schema: bool) -> bool {
    if schema
        .superseded_by
        .as_deref()
        .is_some_and(|s| !s.trim().is_empty())
    {
        return false;
    }
    is_system_schema
        || matches!(
            schema.source,
            SchemaSource::SystemSeed | SchemaSource::StarterSeed
        )
}
