//! Unified schema field runtime type.
//!
//! One struct keyed by [`FieldKind`] (Single / Hash / Range / HashRange)
//! holds the common field metadata.
//!
//! A field is a value. It never reads the store. A format 7 home reads
//! records, not field keys.

mod kind;
mod value;

use serde::{Deserialize, Serialize};

use crate::schema::types::field::FieldCommon;
use crate::schema::types::schema::DeclarativeSchemaType;

pub use kind::FieldKind;
pub use value::FieldValue;

/// Unified runtime field: common metadata and key-shape kind. Replaces the
/// former four-way enum of field structs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldVariant {
    #[serde(flatten)]
    pub inner: FieldCommon,
    /// Key shape — fixed at construction from the schema's
    /// [`DeclarativeSchemaType`].
    pub kind: FieldKind,
}

impl FieldVariant {
    /// Construct an empty field of the given kind.
    #[must_use]
    pub fn new(kind: FieldKind) -> Self {
        Self {
            inner: FieldCommon::default(),
            kind,
        }
    }

    /// Construct from a declarative schema type (the usual construction path).
    #[must_use]
    pub fn from_schema_type(schema_type: DeclarativeSchemaType) -> Self {
        Self::new(FieldKind::from(schema_type))
    }

    /// Gets the common field data.
    pub fn common(&self) -> &FieldCommon {
        &self.inner
    }

    /// Gets the common field data mutably.
    pub fn common_mut(&mut self) -> &mut FieldCommon {
        &mut self.inner
    }
}
