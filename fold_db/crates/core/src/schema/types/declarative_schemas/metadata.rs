use super::DeclarativeSchemaDefinition;
use schema_types::derive_transform_metadata;
use std::collections::HashMap;

impl DeclarativeSchemaDefinition {
    /// Regenerate all derived transform metadata (hash mappings, inputs, source schemas).
    /// Called after construction and after deserialization from database.
    ///
    /// The derivation lives once in `schema_types::derive_transform_metadata`.
    pub(crate) fn regenerate_metadata(&mut self) {
        let meta = derive_transform_metadata(self.transform_fields.as_ref());
        self.field_to_hash_code.extend(meta.field_to_hash_code);
        self.hash_to_code = meta.hash_to_code;
        self.inputs_schema_fields = meta.inputs_schema_fields;
        self.source_schemas = meta.source_schemas;
    }

    pub fn get_inputs(&self) -> Vec<String> {
        self.inputs_schema_fields.clone()
    }

    pub fn get_source_schemas(&self) -> Vec<String> {
        self.source_schemas.clone()
    }

    /// Gets a reference to the hash-to-code mapping.
    pub fn hash_to_code(&self) -> &HashMap<String, String> {
        &self.hash_to_code
    }
}
