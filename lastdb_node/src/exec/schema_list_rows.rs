//! Shared annotation for full and summary schema list rows.

use super::*;

/// Serialize one schema row with null record count fields (`include_counts` is retired).
fn annotated_schema_row<T: serde::Serialize>(schema: &T) -> Option<Value> {
    let Ok(Value::Object(mut row)) = serde_json::to_value(schema) else {
        return None;
    };
    row.insert("record_count".into(), Value::Null);
    row.insert("has_data".into(), Value::Null);
    Some(Value::Object(row))
}

fn annotate_schemas<T: serde::Serialize>(schemas: Vec<T>) -> Option<Vec<Value>> {
    let mut annotated = Vec::with_capacity(schemas.len());
    for schema in &schemas {
        annotated.push(annotated_schema_row(schema)?);
    }
    Some(annotated)
}

/// Full schema rows (`include_full`). `None` on an internal failure.
pub(super) fn annotate_full_schemas(host: &Host) -> Option<Vec<Value>> {
    annotate_schemas(
        host.db
            .schema_manager()
            .get_active_schemas_with_states()
            .ok()?,
    )
}

/// Summary rows (the default projection). `None` on an internal failure.
pub(super) fn annotate_summary_schemas(host: &Host) -> Option<Vec<Value>> {
    annotate_schemas(
        host.db
            .schema_manager()
            .get_active_schema_list_entries_with_states()
            .ok()?,
    )
}
