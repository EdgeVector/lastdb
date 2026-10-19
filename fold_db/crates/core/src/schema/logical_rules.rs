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

/// Keep key queries within one hash while preserving explicit page and
/// sample operations. Catalog metadata does not establish a logical read path.
pub fn require_logical_filter(filter: Option<&HashRangeFilter>) -> Result<(), SchemaError> {
    if matches!(
        filter,
        Some(
            HashRangeFilter::RangeKey(_)
                | HashRangeFilter::RangePrefix(_)
                | HashRangeFilter::RangeRange { .. }
                | HashRangeFilter::HashRange { .. }
                | HashRangeFilter::HashPattern(_)
                | HashRangeFilter::RangePattern(_)
                | HashRangeFilter::HashRangePattern { .. }
        )
    ) {
        return Err(SchemaError::InvalidData(
            "LastDB does not support ranges without a fixed hash or pattern filters; use Hash or HashRange key selectors"
                .into(),
        ));
    }
    Ok(())
}

/// Logical state contains current values and no historical query view.
pub fn require_current_read(historical: bool) -> Result<(), SchemaError> {
    if historical {
        return Err(SchemaError::InvalidData(
            "LastDB does not support as_of queries; this engine stores current values only".into(),
        ));
    }
    Ok(())
}
