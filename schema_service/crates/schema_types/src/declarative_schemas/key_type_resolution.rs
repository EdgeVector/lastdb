use super::DeclarativeSchemaDefinition;
use crate::FieldValueType;

impl DeclarativeSchemaDefinition {
    /// Resolve exact field names first, then object/map paths and union branches.
    /// Missing legacy declarations remain Any; no value-based type inference occurs.
    #[must_use]
    pub fn key_field_type(&self, path: &str) -> FieldValueType {
        if let Some(field_type) = self.field_types.get(path) {
            return field_type.clone();
        }
        path.split_once('.')
            .and_then(|(root, child)| {
                self.field_types
                    .get(root)
                    .and_then(|ty| nested_type(ty, child))
            })
            .unwrap_or(FieldValueType::Any)
    }
}

fn nested_type(field_type: &FieldValueType, path: &str) -> Option<FieldValueType> {
    match field_type {
        FieldValueType::Object(fields) => Some(
            fields
                .get(path)
                .cloned()
                .or_else(|| {
                    path.split_once('.').and_then(|(root, child)| {
                        fields.get(root).and_then(|ty| nested_type(ty, child))
                    })
                })
                .unwrap_or(FieldValueType::Any),
        ),
        FieldValueType::Map(value_type) => Some((**value_type).clone()),
        FieldValueType::Any => Some(FieldValueType::Any),
        FieldValueType::OneOf(variants) => {
            let types: Vec<_> = variants
                .iter()
                .filter_map(|ty| nested_type(ty, path))
                .collect();
            match types.as_slice() {
                [] => None,
                [only] => Some(only.clone()),
                _ => Some(FieldValueType::OneOf(types)),
            }
        }
        _ => None,
    }
}
