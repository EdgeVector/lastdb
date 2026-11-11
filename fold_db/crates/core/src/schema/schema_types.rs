use crate::schema::types::declarative_schemas::DeclarativeSchemaDefinition;
use crate::schema::types::schema::DeclarativeSchemaType;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum SchemaState {
    #[default]
    Available,
    Blocked,
}

/// Node-local time-based retention policy for one installed schema.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaRetentionPolicy {
    pub ttl_seconds: u64,
    pub source: RetentionSource,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub overridden_manifest_ttl_seconds: Option<u64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub hash_partitions: Vec<String>,
}

impl SchemaRetentionPolicy {
    pub fn operator(ttl_seconds: u64, hash_partitions: Vec<String>) -> Self {
        Self {
            ttl_seconds,
            source: RetentionSource::Operator,
            overridden_manifest_ttl_seconds: None,
            hash_partitions,
        }
    }

    pub fn manifest(ttl_seconds: u64) -> Self {
        Self {
            ttl_seconds,
            source: RetentionSource::Manifest,
            overridden_manifest_ttl_seconds: None,
            hash_partitions: Vec::new(),
        }
    }

    pub fn operator_over(
        previous: Option<&Self>,
        ttl_seconds: u64,
        mut hash_partitions: Vec<String>,
    ) -> Self {
        hash_partitions.sort();
        hash_partitions.dedup();
        Self {
            ttl_seconds,
            source: RetentionSource::Operator,
            overridden_manifest_ttl_seconds: previous.and_then(Self::manifest_ttl_seconds),
            hash_partitions,
        }
    }

    pub fn after_operator_clear(previous: Option<&Self>) -> Option<Self> {
        previous
            .filter(|policy| policy.source == RetentionSource::Operator)
            .and_then(|policy| policy.overridden_manifest_ttl_seconds)
            .map(Self::manifest)
    }

    pub fn manifest_ttl_seconds(&self) -> Option<u64> {
        match self.source {
            RetentionSource::Manifest => Some(self.ttl_seconds),
            RetentionSource::Operator => self.overridden_manifest_ttl_seconds,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RetentionSource {
    Operator,
    Manifest,
}

impl RetentionSource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Operator => "operator",
            Self::Manifest => "manifest",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaWithState {
    #[serde(flatten)]
    pub schema: DeclarativeSchemaDefinition,
    pub state: SchemaState,
}

impl SchemaWithState {
    pub fn new(schema: DeclarativeSchemaDefinition, state: SchemaState) -> Self {
        Self { schema, state }
    }

    pub fn name(&self) -> &str {
        &self.schema.name
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaListEntry {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub descriptive_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub purpose_statement: Option<String>,
    pub schema_type: DeclarativeSchemaType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub record_keys: Option<schema_types::record_keys::RecordKeys>,
    pub fields: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub identity_hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub owner_app_id: Option<String>,
    pub state: SchemaState,
}

impl SchemaListEntry {
    pub fn from_schema(schema: &DeclarativeSchemaDefinition, state: SchemaState) -> Self {
        Self {
            name: schema.name.clone(),
            descriptive_name: schema.descriptive_name.clone(),
            purpose_statement: schema.purpose_statement.clone(),
            schema_type: schema.schema_type.clone(),
            record_keys: schema.record_keys.clone(),
            fields: schema.fields.clone(),
            hash: schema.hash.clone(),
            identity_hash: schema.identity_hash.clone(),
            owner_app_id: schema.owner_app_id.clone(),
            state,
        }
    }
}
