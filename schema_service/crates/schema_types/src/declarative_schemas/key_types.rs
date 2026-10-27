use super::DeclarativeSchemaDefinition;
use crate::FieldValueType;
use std::collections::BTreeMap;

impl DeclarativeSchemaDefinition {
    /// Temporal ranges require a typed home; temporal hashes remain unsupported.
    pub fn validate_key_field_types(&self) -> Result<(), String> {
        self.validate_temporal_keys(false)
    }

    /// A legacy home must never gain date semantics from its schema alone.
    pub fn validate_legacy_key_field_types(&self) -> Result<(), String> {
        self.validate_temporal_keys(true)
    }

    fn validate_temporal_keys(&self, include_range: bool) -> Result<(), String> {
        for path in self.key.iter().flat_map(|key| {
            key.hash_field
                .iter()
                .chain(key.range_field.iter().filter(|_| include_range))
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
                    "key field '{path}' uses Date or DateTime outside a typed range contract"
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
        (FieldValueType::Map(value_type), Some(path)) => {
            temporal_at_path(value_type, None)
                || path
                    .split_once('.')
                    .is_some_and(|(_, child)| temporal_at_path(value_type, Some(child)))
        }
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
