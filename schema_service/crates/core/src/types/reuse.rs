use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::catalog::SchemaEnvelope;

/// A single schema lookup entry in a batch reuse request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaLookupEntry {
    pub descriptive_name: String,
    pub fields: Vec<String>,
}

/// Batch request: multiple schema names to check at once
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchSchemaReuseRequest {
    pub schemas: Vec<SchemaLookupEntry>,
}

/// Result for a single matched schema in the batch reuse check.
/// `schema` carries the system/user classification via
/// `SchemaEnvelope`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaReuseMatch {
    pub schema: SchemaEnvelope,
    pub matched_descriptive_name: String,
    pub is_exact_match: bool,
    pub field_rename_map: HashMap<String, String>,
    pub is_superset: bool,
    pub unmapped_fields: Vec<String>,
}

/// Batch response: input descriptive_name -> match result.
/// Only names with matches are included; missing keys = no match found.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchSchemaReuseResponse {
    pub matches: HashMap<String, SchemaReuseMatch>,
}
