use super::types::{field::HashRangeFilter, schema::DeclarativeSchemaType};
use super::SchemaError;

/// Enforce supported logical layouts without changing catalog-template decoding.
pub fn require_logical_layout(schema_type: &DeclarativeSchemaType) -> Result<(), SchemaError> {
    if !matches!(
        schema_type,
        DeclarativeSchemaType::Hash | DeclarativeSchemaType::HashRange
    ) {
        return Err(SchemaError::InvalidData(
            "LastDB supports only Hash and HashRange schemas; Single and standalone Range are unsupported"
                .into(),
        ));
    }
    Ok(())
}

/// Require an explicit Hash or HashRange key selector on every logical query.
pub fn require_logical_filter(filter: Option<&HashRangeFilter>) -> Result<(), SchemaError> {
    if filter.is_none() {
        return Err(SchemaError::InvalidData(
            "LastDB requires a Hash or HashRange key selector".into(),
        ));
    }
    Ok(())
}
