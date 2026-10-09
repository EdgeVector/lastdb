use super::SchemaCore;
use crate::schema::types::{DeclarativeSchemaDefinition, SchemaError};

impl SchemaCore {
    /// Apply this schema's `FieldMapper` entries, copying molecule UUIDs from the
    /// referenced source fields onto the corresponding target runtime fields.
    ///
    /// This is the schema-expansion adoption step: a superset schema's shared
    /// fields point back at the superseded source schema, and this resolves them
    /// so reads/writes land on the same molecules (no data migration). It is a
    /// no-op for schemas without field mappers and is idempotent.
    ///
    /// Must run AFTER `validate_field_mapper_compatibility` (cross-type molecule
    /// copies corrupt reads) and, during expansion, BEFORE the old schema is
    /// blocked (so the source resolves without following its redirect).
    pub async fn apply_field_mappers(&self, schema_name: &str) -> Result<(), SchemaError> {
        self.field_mapper.apply_field_mappers(schema_name).await
    }

    /// Copy the predecessor's record molecule UUID when `record_mapper` is set
    /// and the source already has R. No-op when the predecessor has only
    /// `field_molecule_uuids`. Does not drop FieldMappers.
    pub async fn apply_record_mapper(&self, schema_name: &str) -> Result<(), SchemaError> {
        self.field_mapper.apply_record_mapper(schema_name).await
    }

    /// Load schema from JSON string (creates Available schema)
    /// Only supports declarative schema format
    pub async fn load_schema_from_json(&self, json_str: &str) -> Result<(), SchemaError> {
        // Parse JSON string to DeclarativeSchemaDefinition
        let declarative_schema: DeclarativeSchemaDefinition = serde_json::from_str(json_str)
            .map_err(|e| {
                SchemaError::InvalidData(format!("Failed to parse declarative schema: {e}"))
            })?;

        // Validate all fields have data classifications
        if let Some(ref fields) = declarative_schema.fields {
            let unclassified: Vec<&str> = fields
                .iter()
                .filter(|f| {
                    !declarative_schema
                        .field_data_classifications
                        .contains_key(*f)
                })
                .map(String::as_str)
                .collect();
            if !unclassified.is_empty() {
                return Err(SchemaError::InvalidData(format!(
                    "Schema '{}' has unclassified fields: {}. All fields must have a DataClassification.",
                    declarative_schema.name,
                    unclassified.join(", ")
                )));
            }
        }

        // Convert declarative schema to Schema
        let schema = crate::schema::SchemaInterpreter::interpret(declarative_schema)?;

        // Load the schema using the existing method
        self.load_schema_internal(schema).await
    }

    /// Load schema from file (creates Available schema)
    /// Only supports declarative schema format
    pub async fn load_schema_from_file<P: AsRef<std::path::Path>>(
        &self,
        path: P,
    ) -> Result<(), SchemaError> {
        // Use the existing parse_schema_file method which handles declarative schemas
        if let Some(schema) = self.parse_schema_file(path.as_ref())? {
            self.load_schema_internal(schema).await
        } else {
            Err(SchemaError::InvalidData(
                "No schema found in file".to_string(),
            ))
        }
    }
}
