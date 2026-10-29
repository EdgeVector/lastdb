//! Reads of the current head and fixed-field row state for CAS and aggregates.

use crate::atom::AtomEntry;
use crate::schema::types::{AggregateWinner, Mutation};
use crate::schema::SchemaError;

use super::cas::CurrentRowFields;
use super::cas_rules::{self, CurrentHead};
use super::MutationManager;

impl MutationManager {
    /// Read the current head at `mutation.key_value` for `field`: its head atom
    /// UUID (the record's content hash for CAS purposes) and its dereferenced
    /// content value. Returns an empty head when the field has no live value
    /// at the key, INCLUDING when the head is a tombstone — a deleted key reads
    /// as absent so `Absent` matches after a delete and `Value`/`ContentHash`
    /// correctly miss.
    ///
    /// Resolve the expected field directly, independently of the mutation's
    /// payload. Delete has no payload, and Update can compare an unchanged field.
    pub(super) async fn read_current_head(
        &self,
        mutation: &Mutation,
        field: &str,
        storage_prefix: Option<&str>,
    ) -> Result<CurrentHead, SchemaError> {
        let schema = self.load_schema_for_point_read(mutation, storage_prefix)?;

        let key_value =
            self.resolve_mutation_key_value(&mutation.schema_name, &schema, mutation)?;

        let Some(schema_field) = schema.runtime_fields.get(field) else {
            return Err(SchemaError::InvalidField(format!(
                "CAS expectation names field '{field}' absent from schema '{}'",
                mutation.schema_name
            )));
        };

        if !self
            .current_key_belongs(&schema, &key_value, storage_prefix)
            .await?
        {
            return Ok(CurrentHead::default());
        }

        let Some(entry) = self
            .point_read_field_entry(
                &mutation.schema_name,
                field,
                schema_field,
                &key_value,
                storage_prefix,
            )
            .await?
        else {
            return Ok(CurrentHead::default());
        };
        let mutation_uuid = entry.mutation_uuid;
        let uuid = entry.atom_uuid;
        let atom = crate::db_operations::tip_atoms::get_atoms_for_tips(
            &self.db_ops,
            &[uuid.as_str()],
            storage_prefix,
        )
        .await?
        .pop()
        .ok_or(SchemaError::MissingSelectedAtom)?;

        // A tombstoned head reads as absent — a deleted record has no live
        // value to compare against.
        if crate::atom::is_tombstone_value(atom.content()) {
            return Ok(CurrentHead::default());
        }

        Ok(CurrentHead {
            mutation_uuid: Some(mutation_uuid),
            atom_uuid: Some(uuid),
            value: Some(atom.content().clone()),
        })
    }

    fn load_schema_for_point_read(
        &self,
        mutation: &Mutation,
        storage_prefix: Option<&str>,
    ) -> Result<crate::schema::Schema, SchemaError> {
        let Some(mut schema) = self
            .schema_manager
            .get_schema_metadata(&mutation.schema_name)?
        else {
            return Err(SchemaError::InvalidData(format!(
                "Schema '{}' not found",
                mutation.schema_name
            )));
        };
        super::helpers::apply_storage_prefix_to_schema(&mut schema, storage_prefix);
        Ok(schema)
    }

    async fn current_key_belongs(
        &self,
        schema: &crate::schema::Schema,
        key: &crate::schema::types::KeyValue,
        storage_prefix: Option<&str>,
    ) -> Result<bool, SchemaError> {
        Ok(
            crate::db_operations::logical_membership::logical_key_membership(
                &self.db_ops,
                schema,
                std::slice::from_ref(key),
                storage_prefix,
            )
            .await?[0],
        )
    }

    /// Point-read a fixed field set from one exact row.
    ///
    /// Aggregate apply uses this to load one member row and one summary row.
    /// The cost depends only on the declared field count, not partition size.
    pub(super) async fn read_current_row_fields(
        &self,
        mutation: &Mutation,
        fields: &[String],
        storage_prefix: Option<&str>,
    ) -> Result<CurrentRowFields, SchemaError> {
        self.read_current_row_fields_with_winner(mutation, fields, storage_prefix)
            .await
            .map(|(row, _winner)| row)
    }

    /// Point-read a fixed field set and its latest source-order winner.
    ///
    /// Select each tip once, then batch-load their bodies at the same scope.
    /// The winner comes from the same selected tips as the returned values.
    /// AggregateSet uses it to check whether the stored source order matches
    /// an exact replay before it advances the member's winner.
    pub(super) async fn read_current_row_fields_with_winner(
        &self,
        mutation: &Mutation,
        fields: &[String],
        storage_prefix: Option<&str>,
    ) -> Result<(CurrentRowFields, Option<AggregateWinner>), SchemaError> {
        let schema = self.load_schema_for_point_read(mutation, storage_prefix)?;
        let key_value =
            self.resolve_mutation_key_value(&mutation.schema_name, &schema, mutation)?;
        if let Some(field) = fields
            .iter()
            .find(|field| !schema.runtime_fields.contains_key(*field))
        {
            return Err(SchemaError::InvalidField(format!(
                "point read names field '{field}' absent from schema '{}'",
                mutation.schema_name,
            )));
        }
        if !self
            .current_key_belongs(&schema, &key_value, storage_prefix)
            .await?
        {
            return Ok((CurrentRowFields::Absent, None));
        }
        let selected = futures::future::try_join_all(fields.iter().map(|field| {
            let schema_field = schema
                .runtime_fields
                .get(field)
                .expect("validated current field");
            let key_value = &key_value;
            async move {
                let entry = self
                    .point_read_field_entry(
                        &mutation.schema_name,
                        field,
                        schema_field,
                        key_value,
                        storage_prefix,
                    )
                    .await?;
                Ok::<_, SchemaError>((field, entry))
            }
        }))
        .await?;
        let selected: Vec<_> = selected
            .into_iter()
            .filter_map(|(field, entry)| entry.map(|entry| (field, entry)))
            .collect();
        if selected.is_empty() {
            return Ok((CurrentRowFields::Absent, None));
        }
        let uuids: Vec<&str> = selected
            .iter()
            .map(|(_, entry)| entry.atom_uuid.as_str())
            .collect();
        let atoms = crate::db_operations::tip_atoms::get_atoms_for_tips(
            &self.db_ops,
            &uuids,
            storage_prefix,
        )
        .await?;
        cas_rules::assemble_current_row(selected, atoms)
    }

    /// Load the current selected tip of one field through the core storage
    /// reader. Schema metadata does not carry record values.
    /// `None` means the key is tombstoned or has no tip.
    async fn point_read_field_entry(
        &self,
        schema_name: &str,
        field: &str,
        schema_field: &crate::schema::types::field::FieldVariant,
        key_value: &crate::schema::types::KeyValue,
        storage_prefix: Option<&str>,
    ) -> Result<Option<AtomEntry>, SchemaError> {
        let changed_key = schema_field.changed_key_for(key_value).ok_or_else(|| {
            SchemaError::InvalidData(format!(
                "point read cannot resolve field '{field}' key in schema '{schema_name}'"
            ))
        })?;
        let molecule_uuid = schema_field
            .common()
            .molecule_uuid()
            .cloned()
            .ok_or_else(|| {
                SchemaError::InvalidData(format!(
                    "point read field '{schema_name}.{field}' has no molecule identity"
                ))
            })?;
        let key = crate::schema::types::KeyValue::new(changed_key.hash, changed_key.range);
        let Some((_, record)) = self
            .db_ops
            .atoms()
            .load_query_tip_records(storage_prefix, &molecule_uuid, &[key])
            .await?
            .pop()
            .flatten()
        else {
            return Ok(None);
        };
        if record.meta.as_ref().is_some_and(|meta| meta.tombstoned) {
            return Ok(None);
        }
        Ok(Some(record.entry))
    }
}
