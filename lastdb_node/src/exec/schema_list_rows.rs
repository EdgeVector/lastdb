//! Shared annotation for full and summary schema list rows.

use super::*;

/// Serialize one schema row and stamp its record count. Counts stay `null`
/// unless the caller opted in: they walk the store. `None` on a
/// serialization failure.
async fn annotated_schema_row<T: serde::Serialize>(
    host: &Host,
    schema_name: &str,
    schema: &T,
    include_counts: bool,
) -> Option<Value> {
    let Ok(Value::Object(mut row)) = serde_json::to_value(schema) else {
        return None;
    };
    let record_count = if include_counts {
        count_schema_records(host, schema_name).await
    } else {
        None
    };
    row.insert("record_count".into(), record_count.into());
    row.insert(
        "has_data".into(),
        record_count.map(|count| count > 0).into(),
    );
    Some(Value::Object(row))
}

/// Annotate one loaded schema list. `None` on an internal failure.
async fn annotate_schemas<T: serde::Serialize>(
    host: &Host,
    include_system: bool,
    include_counts: bool,
    mut schemas: Vec<T>,
    source_of: impl Fn(&T) -> SchemaSource,
    name_of: impl Fn(&T) -> &str,
) -> Option<Vec<Value>> {
    if !include_system {
        schemas.retain(|schema| source_of(schema) != SchemaSource::SystemSeed);
    }
    let mut annotated = Vec::with_capacity(schemas.len());
    for schema in &schemas {
        annotated.push(annotated_schema_row(host, name_of(schema), schema, include_counts).await?);
    }
    Some(annotated)
}

/// Full schema rows (`include_full`). `None` on an internal failure.
pub(super) async fn annotate_full_schemas(
    host: &Host,
    include_system: bool,
    include_counts: bool,
) -> Option<Vec<Value>> {
    annotate_schemas(
        host,
        include_system,
        include_counts,
        host.db
            .schema_manager()
            .get_active_schemas_with_states()
            .ok()?,
        |schema| schema.schema.source,
        |schema| schema.name(),
    )
    .await
}

/// Summary rows (the default projection). `None` on an internal failure.
pub(super) async fn annotate_summary_schemas(
    host: &Host,
    include_system: bool,
    include_counts: bool,
) -> Option<Vec<Value>> {
    annotate_schemas(
        host,
        include_system,
        include_counts,
        host.db
            .schema_manager()
            .get_active_schema_list_entries_with_states()
            .ok()?,
        |schema| schema.source,
        |schema| schema.name.as_str(),
    )
    .await
}
