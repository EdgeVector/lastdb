use crate::schema::types::key_value::KeyValue;
use crate::schema::SchemaError;
use async_trait::async_trait;
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
    pub fields_and_values: HashMap<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct IndexChangeBatch {
    pub schema_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub searchable_fields: Option<HashSet<String>>,
    pub changes: Vec<IndexChange>,
}

/// Off-path consumer of committed mutation changes that should be reflected in
/// a local/rebuildable search index.
#[async_trait]
pub trait IndexSink: Send + Sync {
    async fn apply_change_batch(&self, batch: IndexChangeBatch) -> Result<(), SchemaError>;
}
