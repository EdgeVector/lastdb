use crate::schema::types::key_value::KeyValue;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum IndexChangeKind {
    Upsert,
    Tombstone,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct IndexChange {
    pub mutation_id: String,
    pub kind: IndexChangeKind,
    pub key_value: KeyValue,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub record_fields: HashMap<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct IndexChangeBatch {
    #[serde(
        default = "default_format_version",
        skip_serializing_if = "is_current_format_version"
    )]
    pub format_version: u32,
    pub schema_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub searchable_fields: Option<HashSet<String>>,
    pub changes: Vec<IndexChange>,
}

fn default_format_version() -> u32 {
    1
}

fn is_current_format_version(v: &u32) -> bool {
    *v == 1
}
