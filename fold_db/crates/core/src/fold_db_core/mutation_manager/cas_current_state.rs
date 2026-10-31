//! Reads of the current head and fixed-field row state for CAS and aggregates.
//!
//! Each read gathers storage state; [`super::plan::cas`] decides what that
//! state means (absent, live head, or corrupt row).

use crate::atom::AtomEntry;
use crate::schema::types::{AggregateWinner, Mutation};
use crate::schema::SchemaError;

use super::plan::cas::{self, CurrentHead, CurrentRowFields};
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

        let schema_field = cas::expected_field(&schema, mutation, field)?;

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
        let atom = crate::db_operations::tip_atoms::get_atoms_for_tips(
            &self.db_ops,
            &[entry.atom_uuid.as_str()],
            storage_prefix,
        )
        .await?
        .pop()
        .ok_or(SchemaError::MissingSelectedAtom)?;
        Ok(cas::head_from_tip(entry, &atom))
    }

    fn load_schema_for_point_read(
        &self,
        mutation: &Mutation,
        storage_prefix: Option<&str>,
    ) -> Result<crate::schema::Schema, SchemaError> {
        let mut schema = self
            .schema_manager
            .require_schema_metadata(&mutation.schema_name)?;
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
        cas::require_point_fields(&mutation.schema_name, &schema, fields)?;
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
        cas::assemble_current_row(selected, atoms)
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
        let (molecule_uuid, key) = cas::point_tip_key(schema_name, field, schema_field, key_value)?;
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
        Ok(cas::live_tip_entry(record.entry, record.meta.as_ref()))
    }
}
