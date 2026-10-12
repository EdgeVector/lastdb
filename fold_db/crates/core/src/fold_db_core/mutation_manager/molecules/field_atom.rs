//! Atom creation for one mutation field during the prepare phase.

use crate::atom::Atom;
use crate::db_operations::AtomStore;
use crate::schema::types::schema::DeclarativeSchemaType;
use crate::schema::types::{Mutation, Schema};
use crate::schema::SchemaError;

use super::super::MutationManager;

impl MutationManager {
    /// Create the in-memory atom for one mutation field, or `None` when the
    /// field does not produce an atom (document-write extras, non-schema keys).
    pub(super) fn create_field_atom(
        schema_name: &str,
        schema: &Schema,
        mutation: &Mutation,
        field_name: &str,
        value: &serde_json::Value,
        document_write: bool,
    ) -> Result<Option<Atom>, SchemaError> {
        // Envelope writes keep the declared HashRange key fields as
        // normal atoms. They remain the row spine after compaction.
        // Other keys stay only for the document and protein fold.
        let declared_hashrange_key = matches!(schema.schema_type, DeclarativeSchemaType::HashRange)
            && schema.key.as_ref().is_some_and(|key| {
                key.hash_field.as_deref() == Some(field_name)
                    || key.range_field.as_deref() == Some(field_name)
            });
        if document_write
            && field_name != crate::schema::types::RECORD_SENTINEL
            && !declared_hashrange_key
        {
            return Ok(None);
        }
        // Extra keys (e.g. sibling partition values for field_hash
        // protein fold) are not schema fields — keep them on the
        // mutation for fold field maps, but do not create atoms.
        if !schema.runtime_fields.contains_key(field_name) {
            return Ok(None);
        }
        // Skip type validation for tombstone-shaped values. Without
        // this, repurposed Delete writes would fail against any
        // schema whose fields declare a concrete type (`String`,
        // `Integer`, etc.) because the reserved tombstone shape is
        // a JSON object. The validator is meant to guard the
        // application payload, not the deletion marker.
        if !crate::atom::is_tombstone_value(value) {
            let field_type = schema.get_field_type(field_name);
            if let Err(type_err) = field_type.validate(value) {
                return Err(SchemaError::InvalidData(format!(
                    "Type error in field '{}' of schema '{}': {}. Expected {}, got {}",
                    field_name,
                    schema_name,
                    type_err,
                    field_type,
                    serde_json::to_string(value).unwrap_or_else(|_| "?".to_string())
                )));
            }
        }
        AtomStore::create_atom(
            schema_name,
            value.clone(),
            mutation.source_file_name.clone(),
            mutation.metadata.clone(),
        )
        .map(Some)
    }
}
