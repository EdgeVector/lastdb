//! Verify one portable schema binding before its entries reach the writer.

use lastdb_model::portable_mutation::{MutationEntry, MutationOperation, SchemaIdentity};
use schema_types::{DeclarativeSchemaDefinition, PortableSchemaDefinition, PortableSchemaId};

use super::key_values::validate_field_value;
use crate::schema::{Schema, SchemaError};

/// Resolve once per schema and batch. The local schema carries the exact
/// runtime address to use for records and alternate pointers.
pub struct PortableReplaySchema<'a> {
    id: &'a PortableSchemaId,
    local: &'a Schema,
    /// The record's definition when the local schema may hold fields that
    /// later definitions added. Its records use only its own fields.
    declared: Option<&'a DeclarativeSchemaDefinition>,
}

impl<'a> PortableReplaySchema<'a> {
    pub fn bind(
        definition: &'a PortableSchemaDefinition,
        expected: &PortableSchemaId,
        local: &'a Schema,
    ) -> Result<Self, SchemaError> {
        definition
            .bind_local_alias(expected, &local.to_wire())
            .map_err(SchemaError::InvalidData)?;
        Ok(Self {
            id: definition.id(),
            local,
            declared: None,
        })
    }

    /// Bind a record's definition to the schema of its name, which may hold
    /// fields that later accepted definitions added.
    pub fn bind_version(
        definition: &'a PortableSchemaDefinition,
        expected: &PortableSchemaId,
        local: &'a Schema,
    ) -> Result<Self, SchemaError> {
        let declared = definition
            .bind_local_version(expected, &local.to_wire())
            .map_err(SchemaError::InvalidData)?;
        Ok(Self {
            id: definition.id(),
            local,
            declared: Some(declared),
        })
    }

    #[must_use]
    pub fn local(&self) -> &Schema {
        self.local
    }

    #[must_use]
    pub fn identity(&self) -> SchemaIdentity {
        SchemaIdentity::Shared(self.id.as_str().to_owned())
    }

    /// A mismatched identity, key, or field cannot advance replay progress.
    pub fn validate_entry(&self, entry: &MutationEntry) -> Result<(), SchemaError> {
        if !matches!(&entry.schema, SchemaIdentity::Shared(id) if id == self.id.as_str()) {
            return Err(SchemaError::InvalidData(
                "portable mutation targets a different schema revision".into(),
            ));
        }
        let invalid = |error: lastdb_model::portable_mutation::PortableMutationError| {
            SchemaError::InvalidData(error.to_string())
        };
        entry.validate().map_err(invalid)?;
        entry
            .validate_keys(self.local.require_record_keys()?)
            .map_err(invalid)?;
        if let MutationOperation::ReplaceRecord { data, .. } = &entry.operation {
            for (field, value) in &data.fields {
                let declared = self.declared.is_none_or(|definition| {
                    definition.fields.iter().flatten().any(|f| f == field)
                });
                if !declared
                    || validate_field_value(&self.local.name, self.local, field, value).is_err()
                {
                    return Err(SchemaError::InvalidData(
                        "portable mutation has an undeclared or wrongly typed field".into(),
                    ));
                }
            }
        }
        Ok(())
    }
}
