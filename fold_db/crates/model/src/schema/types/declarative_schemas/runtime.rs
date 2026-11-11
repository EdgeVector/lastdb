use super::DeclarativeSchemaDefinition;
use std::collections::HashMap;

impl DeclarativeSchemaDefinition {
    /// A copy of the schema for one read. It takes `&self`, so callers hold
    /// only a shared `RwLock::read()` guard and readers clone in parallel. A
    /// field holds no data, so the copy is small.
    #[must_use]
    pub fn clone_for_read_shared(&self) -> Self {
        self.clone()
    }

    /// Populates `runtime_fields` from declarative schema definition.
    pub fn populate_runtime_fields(&mut self) -> Result<(), crate::schema::SchemaError> {
        use crate::schema::types::field::FieldVariant;

        self.validate_definition()
            .map_err(crate::schema::SchemaError::InvalidData)?;

        let mut runtime_fields = HashMap::new();
        if let Some(field_list) = self.fields.clone() {
            for field_name in field_list {
                let schema_type = self.schema_type.clone();
                runtime_fields.insert(field_name, FieldVariant::from_schema_type(schema_type));
            }
        }

        self.runtime_fields = runtime_fields;
        Ok(())
    }
}
