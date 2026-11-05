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

    /// Populates runtime_fields from declarative schema definition
    /// This is called after deserializing from database to ensure runtime state is initialized
    /// Also regenerates transform metadata (hash mappings, inputs, source schemas) which are not persisted
    pub fn populate_runtime_fields(&mut self) -> Result<(), crate::schema::SchemaError> {
        use crate::schema::types::field::FieldVariant;

        self.validate_definition()
            .map_err(crate::schema::SchemaError::InvalidData)?;

        let mut runtime_fields = HashMap::new();
        let mut add_field = |field_name: String| {
            let schema_type = self.schema_type.clone();
            runtime_fields.insert(field_name, FieldVariant::from_schema_type(schema_type));
        };

        if let Some(field_list) = self.fields.clone() {
            for field_name in field_list {
                add_field(field_name);
            }
        }

        if let Some(transform_map) = self.transform_fields.clone() {
            for (field_name, _) in transform_map {
                add_field(field_name);
            }
        }

        self.runtime_fields = runtime_fields;

        // Regenerate transform metadata that isn't persisted (marked with #[serde(skip)])
        // This is needed when schemas are loaded from the database
        self.regenerate_metadata();

        Ok(())
    }
}
