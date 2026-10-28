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
    #[default]
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
///
/// How the sweep reads the age of a record depends on the schema layout:
///
/// - Range and HashRange: the entry time at the front of the range key, in
///   unix seconds zero-padded to 20 digits (for example
///   `00000000001791863226-0007`). A rewrite of the same key does not change
///   it. A range key that does not start with 20 ASCII digits is never expired.
/// - Hash and Single: the time since the last write to the key. A rewrite
///   resets it.
///
/// What a policy deletes depends on the layout. The sweep removes a record with
/// the sweep verb (the default is changing from purge to delete; see
/// `docs/app-manifest-retention.md`), and an app cannot read it back.
///
/// - Range: every record whose range key is older than the TTL, at the next
///   sweep, **including records written before the policy existed**.
/// - HashRange: in every partition the node has seen written since the policy
///   exists, every record whose range key is older than the TTL, including
///   records written before the policy. A partition with no later write is not
///   swept.
/// - Hash and Single: a record whose last write after the policy exists is
///   older than the TTL. Records written before the policy are not tracked and
///   are kept.
///
/// An app schema has two installed names that share records: the manifest name
/// (`app/Name`) and the catalog schema named by its identity hash. The policy,
/// the age index and the HashRange partition registry are all kept under one
/// anchor name, so a write through either name is covered. The anchor of a
/// schema is itself unless an app manifest declared a `retention` for it: that
/// declare records the manifest name as an alias of the catalog schema. A
/// policy set by hand on a schema without such an alias covers only the name it
/// names. The sweep reads and deletes through the anchor, so a policy reaches
/// every schema bound to the anchor; a manifest declare refuses a TTL on an
/// anchor that another schema or another app shares.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaRetentionPolicy {
    /// Maximum age of retained records, in seconds.
    ///
    /// Store APIs reject zero; keeping the wire value as a plain integer makes
    /// the durable JSON straightforward for owner tooling to inspect later.
    pub ttl_seconds: u64,

    /// Who set the policy in force. A policy written before sources existed
    /// reads as [`RetentionSource::Operator`].
    #[serde(default)]
    pub source: RetentionSource,

    /// The manifest TTL that an operator policy is shadowing, in seconds.
    ///
    /// Set only while `source` is `operator` over a manifest policy. Clearing
    /// the operator policy puts this value back in force, so the manifest
    /// policy returns without a second declare.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub overridden_manifest_ttl_seconds: Option<u64>,

    /// Legacy HashRange partitions explicitly covered by this policy.
    ///
    /// Range schemas leave this empty. A retained HashRange write now records
    /// its hash in node-local retention state. The sweeper queries each named
    /// hash and never uses a cross-partition read. This field keeps coverage
    /// for policy rows that predate the write-side registry.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub hash_partitions: Vec<String>,
}

impl SchemaRetentionPolicy {
    /// A policy set by `lastdb schema-retention set`.
    pub fn operator(ttl_seconds: u64, hash_partitions: Vec<String>) -> Self {
        Self {
            ttl_seconds,
            source: RetentionSource::Operator,
            overridden_manifest_ttl_seconds: None,
            hash_partitions,
        }
    }

    /// A policy applied from an app manifest `retention` key.
    pub fn manifest(ttl_seconds: u64) -> Self {
        Self {
            ttl_seconds,
            source: RetentionSource::Manifest,
            overridden_manifest_ttl_seconds: None,
            hash_partitions: Vec::new(),
        }
    }

    /// The operator policy that follows `previous` when the operator sets
    /// `ttl_seconds`. A manifest TTL in force or already shadowed is kept
    /// beside it, and `clear` puts it back.
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

    /// The policy in force after the operator clears `previous`: the manifest
    /// policy an operator policy shadowed, otherwise none.
    pub fn after_operator_clear(previous: Option<&Self>) -> Option<Self> {
        previous
            .filter(|policy| policy.source == RetentionSource::Operator)
            .and_then(|policy| policy.overridden_manifest_ttl_seconds)
            .map(Self::manifest)
    }

    /// The manifest TTL this policy holds or shadows, if any.
    pub fn manifest_ttl_seconds(&self) -> Option<u64> {
        match self.source {
            RetentionSource::Manifest => Some(self.ttl_seconds),
            RetentionSource::Operator => self.overridden_manifest_ttl_seconds,
        }
    }
}

/// Who set the retention policy in force for a schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RetentionSource {
    /// Set with `lastdb schema-retention set`. An operator policy wins over a
    /// manifest policy until the operator clears it.
    #[default]
    Operator,
    /// Applied from the `retention` key of an app manifest schema entry when
    /// the schema was declared.
    Manifest,
}

impl RetentionSource {
    /// The wire name, as printed by `lastdb schema-retention get`.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Operator => "operator",
            Self::Manifest => "manifest",
        }
    }
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
