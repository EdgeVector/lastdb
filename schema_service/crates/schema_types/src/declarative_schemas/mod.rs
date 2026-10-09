use crate::schema_type::DeclarativeSchemaType;
use crate::DataClassification;
use crate::FieldValueType;
use crate::KeyConfig;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::convert::TryFrom;

mod construct;
mod deserialize;
mod equality;
mod identity;
mod metadata;

pub use identity::{
    canonical_name_parts, compute_identity_hash_parts, parse_canonical_name,
    refuses_identity_downgrade, IdentityRecompute, IDENTITY_HASH_ALGO_VERSION,
    LEGACY_IDENTITY_HASH_ALGO_VERSION,
};
/// The schema identity algorithm lives here and nowhere else. Every schema
/// type in the monorepo — including `fold_db`'s, which is a distinct struct —
/// calls these, so the algorithm cannot be forked back apart.
pub use metadata::{derive_transform_metadata, hash_expression, TransformMetadata};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(into = "String", try_from = "String")]
pub struct FieldMapper {
    source_schema: String,
    source_field: String,
}

impl FieldMapper {
    pub fn new<S: Into<String>, F: Into<String>>(source_schema: S, source_field: F) -> Self {
        Self {
            source_schema: source_schema.into(),
            source_field: source_field.into(),
        }
    }

    pub fn source_schema(&self) -> &str {
        &self.source_schema
    }

    pub fn source_field(&self) -> &str {
        &self.source_field
    }
}

impl TryFrom<String> for FieldMapper {
    type Error = String;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            return Err("FieldMapper definition cannot be empty".to_string());
        }

        // Split on first dot only — field names may contain dots (e.g. "hash.policy.number"
        // means schema="hash", field="policy.number")
        let (source_schema, source_field) = trimmed
            .split_once('.')
            .ok_or_else(|| "FieldMapper must be in 'schema.field' format".to_string())?;

        let source_schema = source_schema.trim();
        let source_field = source_field.trim();

        if source_schema.is_empty() || source_field.is_empty() {
            return Err("FieldMapper must include non-empty source schema and field".to_string());
        }

        Ok(Self::new(source_schema, source_field))
    }
}

impl TryFrom<&str> for FieldMapper {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::try_from(value.to_string())
    }
}

impl From<FieldMapper> for String {
    fn from(mapper: FieldMapper) -> Self {
        format!("{}.{}", mapper.source_schema, mapper.source_field)
    }
}

/// Sentinel used as the field component of a deterministic *record* molecule
/// UUID (`deterministic(schema, RECORD_SENTINEL)`). It cannot be a declared
/// field name: a real field called this would collide with the record grain.
pub const RECORD_SENTINEL: &str = "\u{1f}record";

/// Same-key expand pointer: the new catalog's record molecule is the
/// predecessor's. Wire form is the source schema identity (no field component).
///
/// This does **not** replace [`FieldMapper`]. Dual-stamp both so an old Mini
/// that ignores `record_mapper` (`serde(default)`) still copies field UUIDs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(into = "String", try_from = "String")]
pub struct RecordMapper {
    source_schema: String,
}

impl RecordMapper {
    pub fn new<S: Into<String>>(source_schema: S) -> Self {
        Self {
            source_schema: source_schema.into(),
        }
    }

    pub fn source_schema(&self) -> &str {
        &self.source_schema
    }
}

impl TryFrom<String> for RecordMapper {
    type Error = String;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        let source_schema = value.trim();
        if source_schema.is_empty() {
            return Err("RecordMapper source schema cannot be empty".to_string());
        }
        Ok(Self::new(source_schema))
    }
}

impl TryFrom<&str> for RecordMapper {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::try_from(value.to_string())
    }
}

impl From<RecordMapper> for String {
    fn from(mapper: RecordMapper) -> Self {
        mapper.source_schema
    }
}

impl<'__s> utoipa::ToSchema<'__s> for RecordMapper {
    fn schema() -> (
        &'__s str,
        utoipa::openapi::RefOr<utoipa::openapi::schema::Schema>,
    ) {
        (
            "RecordMapper",
            utoipa::openapi::schema::ObjectBuilder::new()
                .schema_type(utoipa::openapi::schema::SchemaType::String)
                .into(),
        )
    }
}

impl<'__s> utoipa::ToSchema<'__s> for FieldMapper {
    fn schema() -> (
        &'__s str,
        utoipa::openapi::RefOr<utoipa::openapi::schema::Schema>,
    ) {
        (
            "FieldMapper",
            utoipa::openapi::schema::ObjectBuilder::new()
                .schema_type(utoipa::openapi::schema::SchemaType::String)
                .into(),
        )
    }
}

/// Origin of a schema in the service.
///
/// Everything the schema service pre-loads at startup is a **seed**. Seeds differ
/// by ownership: `SystemSeed` schemas are service primitives that the code
/// references by name (fingerprint subsystem, etc.) and must not be deleted;
/// `StarterSeed` schemas are pre-classified starter buckets (e.g. Schema.org
/// types) that users can adopt, fork, or ignore. `User` schemas are created by
/// node operators via `/v1/schemas/propose` and are not seeds.
///
/// This field is `#[serde(default)]` so existing stored schemas without a
/// `source` marker deserialize as `User`, which is the safe default (no
/// service-side dependency implied).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum SchemaSource {
    /// Service-owned primitive (fingerprint subsystem, canonical junction schemas, etc.).
    /// Service code depends on these by name; must not be deleted.
    SystemSeed,
    /// Pre-classified starter bucket loaded at service startup (e.g. Schema.org types).
    /// Safe to ignore, fork, or supersede with a user schema.
    StarterSeed,
    /// Node-created schema via `/v1/schemas/propose`. Default.
    #[default]
    User,
}

/// Declarative schema definition - pure wire/domain form (no runtime FieldVariant).
///
/// This is the unified schema type used by schema_service. fold_db wraps this
/// type and adds `runtime_fields` for molecule hydration.
#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
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
    pub fields: Option<Vec<String>>,
    /// Transform fields - computed fields with expressions (optional, only for transform schemas)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transform_fields: Option<HashMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub field_mappers: Option<HashMap<String, FieldMapper>>,
    /// Same-key expand: this catalog's record molecule is the named predecessor.
    /// Absent on catalogs that predate record-molecule compact. `serde(default)`
    /// so old Mini and old JSON keep loading.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub record_mapper: Option<RecordMapper>,
    /// Schema-level record molecule UUID. Set only after compact mints R, or
    /// after expand copies a predecessor that already has R. Not derived from
    /// FieldMappers.
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
    /// Schema Service field identity hashes (name+description+type+version).
    /// Local Mini uses these for same-key mapping vs different-key protein bind.
    /// Maps field_name -> hex field_hash.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub field_hashes: HashMap<String, String>,
    /// Field identity version used when minting `field_hashes` (default 1).
    /// Maps field_name -> version.
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
    /// [`identity::LEGACY_IDENTITY_HASH_ALGO_VERSION`]. A node refuses to
    /// replace an identity stamped with a version **newer** than the one it
    /// implements, so an old binary meeting new data reconciles loudly instead
    /// of silently downgrading.
    ///
    /// Deliberately **not** part of `PartialEq`: it describes the provenance of
    /// the hash, not the schema's content, so an unstamped row and a freshly
    /// stamped one with the same hash still compare equal.
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

    /// Input fields extracted from transform expressions
    #[serde(skip)]
    pub(crate) inputs_schema_fields: Vec<String>,

    /// Source schemas extracted from input fields (for transforms)
    #[serde(skip)]
    pub(crate) source_schemas: Vec<String>,

    /// Field to hash code mapping for transforms
    #[serde(skip)]
    pub(crate) field_to_hash_code: HashMap<String, String>,

    /// Hash to code mapping for transforms
    #[serde(skip)]
    pub(crate) hash_to_code: HashMap<String, String>,
}
