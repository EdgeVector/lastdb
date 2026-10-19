use std::path::Path;

use crate::schema::SchemaError;

use super::FoldDB;

impl FoldDB {
    /// Load schema from JSON string (creates Available schema)
    pub async fn load_schema_from_json(&self, json_str: &str) -> Result<(), SchemaError> {
        self.schema_manager.load_schema_from_json(json_str).await
    }

    /// Load schema from file (creates Available schema)
    pub async fn load_schema_from_file<P: AsRef<Path>>(&self, path: P) -> Result<(), SchemaError> {
        self.schema_manager.load_schema_from_file(path).await
    }
}
