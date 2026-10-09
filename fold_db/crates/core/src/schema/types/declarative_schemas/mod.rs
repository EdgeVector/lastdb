use crate::schema::types::data_classification::DataClassification;
use crate::schema::types::field_value_type::FieldValueType;
use crate::schema::types::key_config::KeyConfig;
use crate::schema::types::schema::DeclarativeSchemaType;
use serde::Serialize;
use std::collections::HashMap;

mod construct;
mod deserialize;
mod equality;
mod identity;
mod metadata;
mod runtime;

// Pure wire helpers live in schema_types (shared with schema_service).
pub use schema_types::{FieldMapper, RecordMapper, SchemaSource, RECORD_SENTINEL};

/// Declarative schema definition - the primary schema representation.
/// This is the unified schema type that replaces the old Schema/DeclarativeSchemaDefinition split.
///
/// Wire fields match [`schema_types::DeclarativeSchemaDefinition`]; fold_db
/// additionally carries `runtime_fields` for molecule hydration.
#[derive(Debug, Clone, Serialize)]
pub struct DeclarativeSchemaDefinition {
    /// Schema name
    pub name: String,
    /// Human-readable descriptive name for the schema (used in AI-generated proposals)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub descriptive_name: Option<String>,
    /// One-sentence declaration of what the schema is semantically for; used by
    /// dual-signal canonicalization to veto structural merges when purpose
    /// differs. Phase A: optional on the wire; the schema service defaults it
    /// to `Some(descriptive_name)` at registration when absent so downstream
    /// consumers always see a value. Phase B will tighten the contract once
    /// the purpose-embedding pipeline is in place.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub purpose_statement: Option<String>,
    /// Schema type ("Single" | "Hash" | "Range" | "HashRange")
    pub schema_type: DeclarativeSchemaType,
    /// Key configuration (required when schema_type == "Hash", "Range", or "HashRange")
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key: Option<KeyConfig>,
    /// Field names - plain data fields without transformations
    // #[serde(skip_serializing_if = "Option::is_none")]
    pub fields: Option<Vec<String>>,
    /// Transform fields - computed fields with expressions (optional, only for transform schemas)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transform_fields: Option<HashMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub field_mappers: Option<HashMap<String, FieldMapper>>,
    /// Same-key expand: this catalog's record molecule is the named predecessor.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub record_mapper: Option<RecordMapper>,
    /// Schema-level record molecule UUID. Set after compact or after expand
    /// copies a predecessor that already has R.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub molecule_uuid: Option<String>,
    /// SHA256 hash of the schema content for integrity verification
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hash: Option<String>,
    /// Molecule UUIDs for each field (persisted for data continuity after mutations)
    /// Maps field_name -> molecule_uuid. Synced from runtime_fields before persistence.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub field_molecule_uuids: Option<HashMap<String, String>>,
    /// Classification tags for each field (e.g. "word", "name:person", "date", "number")
    /// Maps field_name -> list of classification strings
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub field_classifications: HashMap<String, Vec<String>>,
    /// Natural language descriptions for each field (e.g. "the person who created the artwork")
    /// Maps field_name -> description string. Used for semantic field matching in the canonical registry.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub field_descriptions: HashMap<String, String>,
    /// Data classification labels for each field: (sensitivity_level, data_domain).
    /// Maps field_name -> DataClassification. Required for new fields at schema creation.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub field_data_classifications: HashMap<String, DataClassification>,
    /// Interest categories for each field (e.g. "Photography", "Cooking", "Running").
    /// Assigned by the schema service from the canonical field registry.
    /// Maps field_name -> interest category string.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub field_interest_categories: HashMap<String, String>,
    /// Reference fields that point to child schemas
    /// Maps field_name -> child_schema_name
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub ref_fields: HashMap<String, String>,
    /// Strongly typed field value types from the canonical field registry.
    /// Maps field_name -> FieldValueType. Fields not in this map default to Any.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub field_types: HashMap<String, FieldValueType>,
    /// Schema Service field identity hashes. Local coherence: same hash + same
    /// key → mapper; same hash + different key → protein.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub field_hashes: HashMap<String, String>,
    /// Field identity versions for `field_hashes` (default 1).
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub field_versions: HashMap<String, u32>,
    /// Declared-field provenance: local field name -> Schema Service
    /// `declaration_id` (brain `design-lastdb-declared-fields`).
    ///
    /// Presence is what marks the matching `field_hashes` entry as a **declared
    /// (v2)** identity rather than a locally-minted v1 one. The distinction is
    /// load-bearing: v1 identities are network-global, so `created_at` is
    /// byte-identical across 46 unrelated schemas on the live primary, and
    /// binding on a v1 identity alone would fold unrelated products together.
    ///
    /// The local field name may differ from the declared name — a schema
    /// calling the field `name` can stamp the identity a peer stamped under
    /// `title`, and they still bind. That is the rename case, and it comes free
    /// because matching is on identity, never on the local name.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub field_declarations: HashMap<String, String>,
    /// SHA256 hash of sorted field names — unique fingerprint of schema structure
    #[serde(skip_serializing_if = "Option::is_none")]
    pub identity_hash: Option<String>,
    /// Version of the algorithm that minted `identity_hash`.
    ///
    /// `None` means the row predates stamping and is treated as
    /// `schema_types::LEGACY_IDENTITY_HASH_ALGO_VERSION`. `load_schema_internal`
    /// refuses to replace an identity stamped with a version **newer** than the
    /// one this binary implements — that refusal is what makes the 2026-08-06
    /// silent-downgrade class of bug impossible.
    ///
    /// Deliberately **not** part of `PartialEq`: it describes the provenance of
    /// the hash, not the schema's content.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub identity_hash_algo_version: Option<u32>,
    /// If set, this schema has been superseded by the named schema.
    /// Superseded schemas are excluded from active indexes and matching.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub superseded_by: Option<String>,
    /// Default trust domain for all fields in this schema.
    /// If set, fields without an explicit `trust_domain` in their access policy
    /// inherit this value.
    /// Default: "personal".
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub trust_domain: Option<String>,

    /// Identifier of the app that owns this schema, e.g. `"fbrain"`.
    ///
    /// **Participates in `compute_identity_hash`.** Schemas with identical
    /// shape but different `owner_app_id` produce distinct identity hashes —
    /// `fbrain/Concept` and `kanban/Concept` are distinct identities even
    /// when the field list matches.
    ///
    /// `None` is reserved for legacy schemas (registered before app identity
    /// shipped) and the `SystemSeed` / `StarterSeed` primitives the schema
    /// service ships with. For back-compat, schemas with `owner_app_id ==
    /// None` keep the pre-app-identity identity-hash scheme so existing
    /// hashes remain stable.
    ///
    /// See `exemem-workspace/docs/designs/app_identity.md` for the full
    /// design (Lane B2a).
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub owner_app_id: Option<String>,

    /// Origin of this schema in the service. See [`SchemaSource`] for variants.
    /// Existing stored schemas without this field deserialize as `User`.
    #[serde(default)]
    pub source: SchemaSource,

    // Runtime state fields (not serialized)
    /// Runtime field storage with molecules (for database operations)
    #[serde(skip)]
    pub runtime_fields: HashMap<String, crate::schema::types::field::FieldVariant>,

    /// Input fields extracted from transform expressions
    #[serde(skip)]
    inputs_schema_fields: Vec<String>,

    /// Source schemas extracted from input fields (for transforms)
    #[serde(skip)]
    source_schemas: Vec<String>,

    /// Field to hash code mapping for transforms
    #[serde(skip)]
    field_to_hash_code: HashMap<String, String>,

    /// Hash to code mapping for transforms
    #[serde(skip)]
    hash_to_code: HashMap<String, String>,
}

impl From<schema_types::DeclarativeSchemaDefinition> for DeclarativeSchemaDefinition {
    fn from(base: schema_types::DeclarativeSchemaDefinition) -> Self {
        // Go through `new` so the private transform metadata is regenerated.
        // `hash` and `superseded_by` are not carried: the wire load never
        // carried them either.
        let mut schema = Self::new(
            base.name,
            base.schema_type,
            base.key,
            base.fields,
            base.transform_fields,
            base.field_mappers,
        );
        schema.descriptive_name = base.descriptive_name;
        schema.purpose_statement = base.purpose_statement;
        schema.field_molecule_uuids = base.field_molecule_uuids;
        schema.record_mapper = base.record_mapper;
        schema.molecule_uuid = base.molecule_uuid;
        schema.field_classifications = base.field_classifications;
        schema.field_descriptions = base.field_descriptions;
        schema.field_data_classifications = base.field_data_classifications;
        schema.field_interest_categories = base.field_interest_categories;
        schema.ref_fields = base.ref_fields;
        schema.field_types = base.field_types;
        schema.field_hashes = base.field_hashes;
        schema.field_declarations = base.field_declarations;
        schema.field_versions = base.field_versions;
        schema.identity_hash = base.identity_hash;
        schema.identity_hash_algo_version = base.identity_hash_algo_version;
        schema.trust_domain = base.trust_domain;
        schema.owner_app_id = base.owner_app_id;
        schema.source = base.source;

        // owner_app_id participates in identity_hash; a schema that arrives
        // without a hash must reflect it.
        if schema.identity_hash.is_none() && schema.owner_app_id.is_some() {
            schema.compute_identity_hash();
        }
        schema
    }
}

impl From<&DeclarativeSchemaDefinition> for schema_types::DeclarativeSchemaDefinition {
    fn from(schema: &DeclarativeSchemaDefinition) -> Self {
        let json = serde_json::to_value(schema).expect("fold_db Schema serializes");
        serde_json::from_value(json).expect("schema_types Schema deserializes from wire JSON")
    }
}

impl From<DeclarativeSchemaDefinition> for schema_types::DeclarativeSchemaDefinition {
    fn from(schema: DeclarativeSchemaDefinition) -> Self {
        Self::from(&schema)
    }
}
