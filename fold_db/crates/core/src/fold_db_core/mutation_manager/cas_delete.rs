//! User Delete reads the same current document values that queries return.

use super::cas::CurrentRowFields;
use super::MutationManager;
use crate::atom::Atom;
use crate::record_molecule::{
    envelope_is_complete, is_record_molecule_field, parse_document_fields,
};
use crate::schema::types::{Mutation, Schema};
use crate::schema::SchemaError;
use std::collections::BTreeMap;

impl MutationManager {
    /// A complete document owns the value and its query-visible atom identity.
    /// A partial document falls back only for a name it does not carry.
    pub(super) async fn read_current_delete_head(
        &self,
        mutation: &Mutation,
        field: &str,
        scope: Option<&str>,
    ) -> Result<(Option<String>, Option<serde_json::Value>), SchemaError> {
        let (schema, atom) = self.current_delete_document(mutation, scope).await?;
        if !schema.runtime_fields.contains_key(field) {
            return Err(SchemaError::InvalidField(format!(
                "CAS expectation names field '{field}' absent from schema '{}'",
                mutation.schema_name
            )));
        }
        if !is_record_molecule_field(field) {
            if let Some(atom) = atom {
                let document = parse_document_fields(atom.content()).expect("document selected");
                if document.contains_key(field) || envelope_is_complete(atom.content()) {
                    return Ok(match document.get(field) {
                        Some(value) if !crate::atom::is_tombstone_value(value) => {
                            (Some(atom.uuid().to_owned()), Some(value.clone()))
                        }
                        _ => (None, None),
                    });
                }
            }
        }
        self.read_current_head(mutation, field, scope).await
    }

    /// Current document values own peer keys after record compaction. Read
    /// omitted partial-document fields through their existing field molecules.
    pub(super) async fn read_current_delete_fields(
        &self,
        mutation: &Mutation,
        fields: &[String],
        scope: Option<&str>,
    ) -> Result<CurrentRowFields, SchemaError> {
        let (_, atom) = self.current_delete_document(mutation, scope).await?;
        let fields: Vec<_> = fields
            .iter()
            .filter(|name| !is_record_molecule_field(name))
            .cloned()
            .collect();
        let Some(atom) = atom else {
            return self.read_current_row_fields(mutation, &fields, scope).await;
        };
        let document = parse_document_fields(atom.content()).expect("document selected");
        let complete = envelope_is_complete(atom.content());
        let missing: Vec<_> = fields
            .iter()
            .filter(|name| !document.contains_key(*name))
            .cloned()
            .collect();
        let mut values = BTreeMap::new();
        if !complete && !missing.is_empty() {
            match self
                .read_current_row_fields(mutation, &missing, scope)
                .await?
            {
                CurrentRowFields::Present(fallback) => values = fallback,
                corrupt @ CurrentRowFields::Corrupt { .. } => return Ok(corrupt),
                CurrentRowFields::Absent => {}
            }
        }
        for name in fields {
            if let Some(value) = document.get(&name) {
                if !crate::atom::is_tombstone_value(value) {
                    values.insert(name, value.clone());
                }
            }
        }
        Ok(CurrentRowFields::Present(values))
    }

    async fn current_delete_document(
        &self,
        mutation: &Mutation,
        scope: Option<&str>,
    ) -> Result<(Schema, Option<Atom>), SchemaError> {
        let mut schema = self
            .schema_manager
            .get_schema_metadata(&mutation.schema_name)?
            .ok_or_else(|| SchemaError::NotFound(mutation.schema_name.clone()))?;
        super::helpers::apply_storage_prefix_to_schema(&mut schema, scope);
        let Some(record) = schema.molecule_uuid.as_ref() else {
            return Ok((schema, None));
        };
        let key = Self::resolve_mutation_key_value(&mutation.schema_name, &schema, mutation)?;
        let atom = crate::record_molecule::load_record_tip_atoms(
            &self.db_ops,
            record,
            std::slice::from_ref(&key),
            scope,
            Some(&schema),
        )
        .await?
        .remove(&key)
        .filter(|atom| parse_document_fields(atom.content()).is_some());
        Ok((schema, atom))
    }
}
