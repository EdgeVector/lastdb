//! Render one declared key after all of its field dependencies pass validation.

use super::*;

pub(super) fn key(
    definition: &schema_types::AlternateKey,
    fields: &RecordFields,
    coordinates: &BTreeMap<&str, String>,
) -> Result<KeyValue, SchemaError> {
    let hash = definition
        .hash_literal
        .clone()
        .or_else(|| coordinates.get(definition.hash_field.as_str()).cloned())
        .ok_or_else(|| invalid("alternate hash is absent"))?;
    let range = if definition.range_parts.is_empty() {
        definition
            .range_field
            .as_deref()
            .and_then(|field| coordinates.get(field).cloned())
    } else {
        let separator = definition
            .range_separator
            .as_deref()
            .ok_or_else(|| invalid("compound range separator is absent"))?;
        let parts = definition
            .range_parts
            .iter()
            .map(|part| {
                let value = fields
                    .get(&part.field)
                    .ok_or_else(|| invalid("compound range field is absent"))?;
                schema_types::alternate_parts::render(part, value, separator)
                    .map_err(SchemaError::InvalidData)
            })
            .collect::<Result<Vec<_>, _>>()?;
        Some(parts.join(separator))
    };
    Ok(KeyValue::new(Some(hash), range))
}
