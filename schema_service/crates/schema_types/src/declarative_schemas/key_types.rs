use super::DeclarativeSchemaDefinition;
use crate::FieldValueType;
use std::collections::BTreeMap;

impl DeclarativeSchemaDefinition {
    /// Date keys require typed comparison and a versioned storage profile.
    /// Until those exist, reject them instead of silently using string order.
    pub fn validate_key_field_types(&self) -> Result<(), String> {
        for path in self.key.iter().flat_map(|key| {
            key.hash_field
                .iter()
                .chain(key.range_field.iter())
                .map(String::as_str)
        }) {
            let temporal = if let Some(field_type) = self.field_types.get(path) {
                temporal_at_path(field_type, None)
            } else if let Some((root, child)) = path.split_once('.') {
                self.field_types
                    .get(root)
                    .is_some_and(|field_type| temporal_at_path(field_type, Some(child)))
            } else {
                false
            };
            if temporal {
                return Err(format!(
                    "key field '{path}' uses Date or DateTime; typed date keys are not supported yet"
                ));
            }
        }
        Ok(())
    }
}

fn temporal_at_path(field_type: &FieldValueType, child: Option<&str>) -> bool {
    match (field_type, child) {
        (FieldValueType::Date | FieldValueType::DateTime, None) => true,
        (FieldValueType::OneOf(variants), _) => variants
            .iter()
            .any(|variant| temporal_at_path(variant, child)),
        (FieldValueType::Object(fields), Some(path)) => temporal_in_object(fields, path),
        _ => false,
    }
}

fn temporal_in_object(fields: &BTreeMap<String, FieldValueType>, path: &str) -> bool {
    // Match literal names before nested paths, as mutation key resolution does.
    if let Some(field_type) = fields.get(path) {
        return temporal_at_path(field_type, None);
    }
    path.split_once('.').is_some_and(|(root, child)| {
        fields
            .get(root)
            .is_some_and(|field_type| temporal_at_path(field_type, Some(child)))
    })
}
