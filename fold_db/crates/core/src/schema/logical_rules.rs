use super::types::{field::HashRangeFilter, schema::DeclarativeSchemaType, Query};
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
    if matches!(filter, Some(HashRangeFilter::PageAfter { .. })) {
        return Err(SchemaError::InvalidData(
            "LastDB does not support cross-hash PageAfter cursors; query one hash with a query cursor, or use Page with an offset"
                .into(),
        ));
    }
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

/// Refuse unsupported controls at the logical read and count boundaries.
pub fn require_logical_query_controls(query: &Query) -> Result<(), SchemaError> {
    if query.as_of.is_some() {
        return Err(SchemaError::InvalidData(
            "LastDB does not support as_of queries; this engine stores current values only".into(),
        ));
    }
    if query.rehydrate_depth.is_some() {
        return Err(SchemaError::InvalidData(
            "LastDB does not support rehydrate_depth queries; reads return fields at the requested keys"
                .into(),
        ));
    }
    Ok(())
}
