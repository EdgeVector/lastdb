use super::DeclarativeSchemaDefinition;
use crate::FieldValueType;

impl DeclarativeSchemaDefinition {
    /// Resolve exact field names first, then object/map paths and union branches.
    /// `None` when the path names no declared type; `validate_definition`
    /// refuses such a key path. No value-based type inference occurs.
    #[must_use]
    pub fn key_field_type(&self, path: &str) -> Option<FieldValueType> {
        if let Some(field_type) = self.field_types.get(path) {
            return Some(field_type.clone());
        }
        path.split_once('.').and_then(|(root, child)| {
            self.field_types
                .get(root)
                .and_then(|ty| nested_type(ty, child))
        })
    }

    /// The declared type of the primary range key, or `None` without a range
    /// key. A range key with no declared type is an error, never an empty range.
    pub fn range_key_type(&self) -> Result<Option<FieldValueType>, String> {
        self.key
            .as_ref()
            .and_then(|key| key.range_field.as_deref())
            .map(|path| {
                self.key_field_type(path)
                    .ok_or_else(|| format!("range key field '{path}' has no declared type"))
            })
            .transpose()
    }
}

fn nested_type(field_type: &FieldValueType, path: &str) -> Option<FieldValueType> {
    match field_type {
        FieldValueType::Object(fields) => fields.get(path).cloned().or_else(|| {
            path.split_once('.')
                .and_then(|(root, child)| fields.get(root).and_then(|ty| nested_type(ty, child)))
        }),
        FieldValueType::Map(value_type) => Some((**value_type).clone()),
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
