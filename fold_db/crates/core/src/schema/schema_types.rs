use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::schema::types::declarative_schemas::DeclarativeSchemaDefinition;
use crate::schema::types::declarative_schemas::SchemaSource;
use crate::schema::types::key_config::KeyConfig;
use crate::schema::types::schema::DeclarativeSchemaType;

/// State of a schema within the system.
///
/// Access is enforced cryptographically at the operation layer (capability
/// tokens / caller identity), not by a generic schema-state gate, so there is
/// no separate "approved" review state: a schema is usable as soon as it is
/// installed. The only state that changes behavior is `Blocked`, which is used
/// by schema expansion to retire a superseded schema and redirect its queries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum SchemaState {
    /// Schema is installed and usable — can be queried, mutated, field-mapped,
    /// and have transforms run against it.
    ///
    /// The `Approved` alias lets on-disk states persisted before the
    /// approved/available collapse deserialize transparently as `Available`.
    #[default]
    #[serde(alias = "Approved")]
    Available,
    /// Schema blocked by the system (typically superseded during schema
    /// expansion): cannot be queried, but field-mapping and transforms still
    /// run so the successor schema can adopt its molecules.
    Blocked,
}

/// Node-local record that one installed schema no longer answers
/// `descriptive_name` resolution.
///
/// This is the retire primitive for a duplicate name claim. It is deliberately
/// separate from [`DeclarativeSchemaDefinition`], for the same reason
/// [`SchemaRetentionPolicy`] is: `descriptive_name` folds into
/// `compute_identity_hash_parts`, so retiring a claim by editing the schema
/// would mint a DIFFERENT identity and leave the old one Available under the
/// old name. Keeping the record outside the schema artifact leaves the
/// identity hash — and therefore every by-hash pin that addresses this
/// schema — untouched.
///
/// A retired claimant stays [`SchemaState::Available`]: it is still queried,
/// mutated and read when a caller names it by canonical name or identity hash.
/// It is only removed from the candidate set when a caller names it by
/// `descriptive_name`. That is the difference from [`SchemaState::Blocked`],
/// which redirects every lookup — including a by-hash one — to the successor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaNameClaim {
    /// True when this schema is excluded from descriptive-name resolution.
    pub retired: bool,
}

/// Node-local time-based retention policy for one installed schema.
///
/// This is operational state owned by the node. It is deliberately separate
/// from [`DeclarativeSchemaDefinition`], so changing retention cannot alter a
/// published schema artifact, its identity hash, or canonicalization.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaRetentionPolicy {
    /// Maximum age of retained records, in seconds.
    ///
    /// Store APIs reject zero; keeping the wire value as a plain integer makes
    /// the durable JSON straightforward for owner tooling to inspect later.
    pub ttl_seconds: u64,

    /// Legacy HashRange partitions explicitly covered by this policy.
    ///
    /// Range schemas leave this empty. A retained HashRange write now records
    /// its hash in node-local retention state. The sweeper queries each named
    /// hash and never uses a cross-partition read. This field keeps coverage
    /// for policy rows that predate the write-side registry.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub hash_partitions: Vec<String>,
}

/// Schema definition bundled with its current state for UI/API responses
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaWithState {
    /// All schema fields serialized at the top level
    #[serde(flatten)]
    pub schema: DeclarativeSchemaDefinition,
    /// Current state of the schema
    pub state: SchemaState,
}

impl SchemaWithState {
    /// Create a new [`SchemaWithState`] from components
    pub fn new(schema: DeclarativeSchemaDefinition, state: SchemaState) -> Self {
        Self { schema, state }
    }

    /// Access the schema name (helper to avoid cloning when only the name is needed)
    pub fn name(&self) -> &str {
        &self.schema.name
    }
}

/// Lean schema entry for `GET /api/schemas`.
///
/// The catalog list is used for identity and app-schema resolution. It must not
/// serialize the heavy per-field metadata carried by the single-schema detail
/// route (`GET /api/schema/{name}`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaListEntry {
    /// Canonical runtime schema name.
    pub name: String,
    /// Human-readable schema name.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub descriptive_name: Option<String>,
    /// One-sentence semantic purpose, when known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub purpose_statement: Option<String>,
    /// Schema type ("Single" | "Hash" | "Range" | "HashRange").
    pub schema_type: DeclarativeSchemaType,
    /// Key configuration for keyed schemas.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key: Option<KeyConfig>,
    /// Declared data field names.
    pub fields: Option<Vec<String>>,
    /// Transform fields are needed to distinguish transform schemas in a list.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transform_fields: Option<HashMap<String, String>>,
    /// SHA256 hash of the schema content for integrity verification.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hash: Option<String>,
    /// SHA256 identity hash for the schema.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub identity_hash: Option<String>,
    /// Successor schema for superseded entries, when present.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub superseded_by: Option<String>,
    /// Owning app namespace, when app-owned.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub owner_app_id: Option<String>,
    /// Origin of this schema in the service.
    #[serde(default)]
    pub source: SchemaSource,
    /// Current state of the schema.
    pub state: SchemaState,
}

impl SchemaListEntry {
    /// Build a list projection from a full schema without cloning heavy field
    /// metadata such as field hashes, molecule UUIDs, classifiers, or mappers.
    pub fn from_schema(schema: &DeclarativeSchemaDefinition, state: SchemaState) -> Self {
        Self {
            name: schema.name.clone(),
            descriptive_name: schema.descriptive_name.clone(),
            purpose_statement: schema.purpose_statement.clone(),
            schema_type: schema.schema_type.clone(),
            key: schema.key.clone(),
            fields: schema.fields.clone(),
            transform_fields: schema.transform_fields.clone(),
            hash: schema.hash.clone(),
            identity_hash: schema.identity_hash.clone(),
            superseded_by: schema.superseded_by.clone(),
            owner_app_id: schema.owner_app_id.clone(),
            source: schema.source,
            state,
        }
    }
}
