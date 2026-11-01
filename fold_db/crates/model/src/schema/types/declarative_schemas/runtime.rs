use super::DeclarativeSchemaDefinition;
use std::collections::HashMap;

impl DeclarativeSchemaDefinition {
    /// All ordinary runtime fields, in deterministic name order.
    pub fn runtime_field_names(&self) -> Vec<String> {
        let mut names: Vec<_> = self.runtime_fields.keys().cloned().collect();
        names.sort();
        names
    }

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

        // Restore persisted field key UUIDs from field_key_ids if
        // available. Otherwise derive the UUID from this schema's own name and
        // the field name: each schema owns the values of its fields.
        let persisted = self.field_key_ids.clone().unwrap_or_default();
        let schema_name = self.name.clone();
        for (field_name, field) in &mut self.runtime_fields {
            let mol_uuid = persisted.get(field_name).cloned().unwrap_or_else(|| {
                crate::field_key_id::deterministic_field_key_id(&schema_name, field_name)
            });
            field.common_mut().set_field_key_id(mol_uuid);
        }

        // Regenerate transform metadata that isn't persisted (marked with #[serde(skip)])
        // This is needed when schemas are loaded from the database
        self.regenerate_metadata();

        Ok(())
    }

    /// Copies field key UUIDs from runtime_fields into the persisted field_key_ids map.
    /// Called after mutations so that the UUIDs survive serialization to DB.
    pub fn sync_field_key_ids(&mut self) {
        let mut uuids = HashMap::new();
        for (field_name, field) in &self.runtime_fields {
            if let Some(uuid) = field.common().field_key_id() {
                uuids.insert(field_name.clone(), uuid.clone());
            }
        }
        if !uuids.is_empty() {
            self.field_key_ids = Some(uuids);
        }
    }
}
