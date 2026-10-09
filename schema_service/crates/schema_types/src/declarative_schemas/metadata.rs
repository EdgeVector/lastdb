use super::DeclarativeSchemaDefinition;
use sha2::{Digest, Sha256};
use std::collections::HashMap;

/// Transform metadata derived from a schema's `transform_fields`.
///
/// This is the one derivation. Both `schema_types::DeclarativeSchemaDefinition`
/// and `fold_db`'s own schema struct (which carries extra runtime state) call
/// [`derive_transform_metadata`] and store the result, so the two cannot drift.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct TransformMetadata {
    /// Field name -> hash of that field's transform expression.
    pub field_to_hash_code: HashMap<String, String>,
    /// Expression hash -> expression text.
    pub hash_to_code: HashMap<String, String>,
    /// Sorted, de-duplicated `Schema.field` inputs read by the expressions.
    pub inputs_schema_fields: Vec<String>,
    /// Sorted, de-duplicated source schema names of those inputs.
    pub source_schemas: Vec<String>,
}

/// Hex-encoded SHA256 of a transform expression.
pub fn hash_expression(expression: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(expression.as_bytes());
    let hash_bytes = hasher.finalize();
    format!("{hash_bytes:x}")
}

/// Parse a single transform expression into a "Schema.field" input reference.
fn parse_expression_input(expression: &str) -> Option<String> {
    // Split by "." and filter out method calls containing "(" or ")"
    let parts: Vec<&str> = expression
        .split(".")
        .filter(|part| !part.contains("(") && !part.contains(")"))
        .collect();

    if parts.len() >= 2 {
        Some(format!("{}.{}", parts[0], parts[1]))
    } else if parts.len() == 1 {
        Some(parts[0].to_string())
    } else {
        None
    }
}

/// Derive hash mappings, inputs and source schemas from `transform_fields`.
///
/// Missing `transform_fields` and blank expressions are tolerated.
pub fn derive_transform_metadata(
    transform_fields: Option<&HashMap<String, String>>,
) -> TransformMetadata {
    let mut meta = TransformMetadata::default();

    if let Some(map) = transform_fields {
        for (field_name, field_def) in map {
            let field_def_str = field_def.as_str();
            if !field_def_str.trim().is_empty() {
                let hash = hash_expression(field_def_str);
                meta.hash_to_code
                    .insert(hash.clone(), field_def_str.to_string());
                meta.field_to_hash_code.insert(field_name.clone(), hash);
            }
        }
    }

    let mut inputs: Vec<String> = meta
        .hash_to_code
        .values()
        .filter_map(|expr| parse_expression_input(expr))
        .collect();
    inputs.sort();
    inputs.dedup();

    let mut sources: Vec<String> = inputs
        .iter()
        .filter_map(|input| input.split('.').next())
        .map(str::to_string)
        .collect();
    sources.sort();
    sources.dedup();

    meta.inputs_schema_fields = inputs;
    meta.source_schemas = sources;
    meta
}

impl DeclarativeSchemaDefinition {
    /// Regenerate all derived transform metadata (hash mappings, inputs, source schemas).
    /// Called after construction and after deserialization from database.
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
