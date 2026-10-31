use crate::schema::types::field::FieldVariant;
use std::collections::HashMap;
use std::ops::{Deref, DerefMut};

mod runtime;

// Pure wire helpers live in schema_types (shared with schema_service).
pub use schema_types::SchemaSource;

/// Declarative schema definition - the primary schema representation.
///
/// This is [`schema_types::DeclarativeSchemaDefinition`] (the wire fields, the
/// identity hash, construction, equality and parsing) plus `runtime_fields` for
/// molecule hydration. Every wire field and method is reached through `Deref`,
/// so there is one definition of the schema shape in the workspace.
#[derive(Debug, Clone)]
pub struct DeclarativeSchemaDefinition {
    wire: schema_types::DeclarativeSchemaDefinition,
    /// Runtime field storage with molecules (for database operations). Not
    /// serialized and not part of equality.
    pub runtime_fields: HashMap<String, FieldVariant>,
}

impl Deref for DeclarativeSchemaDefinition {
    type Target = schema_types::DeclarativeSchemaDefinition;

    fn deref(&self) -> &Self::Target {
        &self.wire
    }
}

impl DerefMut for DeclarativeSchemaDefinition {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.wire
    }
}

impl DeclarativeSchemaDefinition {
    /// Share the wire contract with direct core mutation callers.
    pub fn validate_field_name(name: &str) -> Result<(), String> {
        schema_types::DeclarativeSchemaDefinition::validate_field_name(name)
    }

    /// Creates a new schema with no runtime fields and all hash mappings generated.
    pub fn new(
        name: String,
        schema_type: schema_types::DeclarativeSchemaType,
        key: Option<schema_types::KeyConfig>,
        fields: Option<Vec<String>>,
        transform_fields: Option<HashMap<String, String>>,
    ) -> Self {
        Self::with_wire(schema_types::DeclarativeSchemaDefinition::new(
            name,
            schema_type,
            key,
            fields,
            transform_fields,
        ))
    }

    fn with_wire(wire: schema_types::DeclarativeSchemaDefinition) -> Self {
        Self {
            wire,
            runtime_fields: HashMap::new(),
        }
    }

    /// Split a canonical name into `(owner_app_id, name)`. See
    /// [`schema_types::parse_canonical_name`].
    #[must_use]
    pub fn parse_canonical_name(canonical: &str) -> (Option<String>, &str) {
        schema_types::parse_canonical_name(canonical)
    }

    /// The wire form of this schema: what the schema service and the stored
    /// catalog see. `hash` and `superseded_by` are not carried, matching what a
    /// wire load has always produced, and an `owner_app_id` without an identity
    /// hash gets one, because `owner_app_id` participates in it.
    #[must_use]
    pub fn to_wire(&self) -> schema_types::DeclarativeSchemaDefinition {
        let mut wire = self.wire.clone();
        Self::normalize_wire(&mut wire);
        wire
    }

    fn normalize_wire(wire: &mut schema_types::DeclarativeSchemaDefinition) {
        wire.hash = None;
        wire.superseded_by = None;
        wire.regenerate_metadata();
        if wire.identity_hash.is_none() && wire.owner_app_id.is_some() {
            wire.compute_identity_hash();
        }
    }
}

impl From<schema_types::DeclarativeSchemaDefinition> for DeclarativeSchemaDefinition {
    fn from(mut base: schema_types::DeclarativeSchemaDefinition) -> Self {
        Self::normalize_wire(&mut base);
        Self::with_wire(base)
    }
}

impl From<&DeclarativeSchemaDefinition> for schema_types::DeclarativeSchemaDefinition {
    fn from(schema: &DeclarativeSchemaDefinition) -> Self {
        schema.to_wire()
    }
}

impl From<DeclarativeSchemaDefinition> for schema_types::DeclarativeSchemaDefinition {
    fn from(schema: DeclarativeSchemaDefinition) -> Self {
        schema.to_wire()
    }
}

// Equality is the wire equality: it excludes derived transform metadata, and
// `runtime_fields` is runtime state that never affected schema identity.
impl PartialEq for DeclarativeSchemaDefinition {
    fn eq(&self, other: &Self) -> bool {
        self.wire == other.wire
    }
}

impl serde::Serialize for DeclarativeSchemaDefinition {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.wire.serialize(serializer)
    }
}

// The wire parsing (omitted `schema_type`, array-or-map `fields`, identity
// recompute) lives once in `schema_types`. This type only adds runtime state.
impl<'de> serde::Deserialize<'de> for DeclarativeSchemaDefinition {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        schema_types::DeclarativeSchemaDefinition::deserialize(deserializer).map(Self::from)
    }
}
