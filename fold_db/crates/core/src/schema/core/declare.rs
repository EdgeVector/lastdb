use super::SchemaCore;
use crate::schema::types::{DeclarativeSchemaDefinition, SchemaError};

impl SchemaCore {
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
        crate::schema::require_logical_layout(&schema.schema_type)?;

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
            crate::schema::require_logical_layout(&schema.schema_type)?;
            self.load_schema_internal(schema).await
        } else {
            Err(SchemaError::InvalidData(
                "No schema found in file".to_string(),
            ))
        }
    }
}
