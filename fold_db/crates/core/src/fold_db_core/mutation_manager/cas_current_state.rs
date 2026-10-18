//! Reads of the current head and fixed-field row state for CAS and aggregates.

use std::collections::{BTreeMap, HashMap};

use crate::schema::types::{AggregateWinner, Mutation};
use crate::schema::SchemaError;

use super::cas::CurrentRowFields;
use super::helpers::current_atom_uuid;
use super::MutationManager;

impl MutationManager {
    /// Read the current head at `mutation.key_value` for `field`: its head atom
    /// UUID (the record's content hash for CAS purposes) and its dereferenced
    /// content value. Returns `(None, None)` when the field has no live value
    /// at the key, INCLUDING when the head is a tombstone — a deleted key reads
    /// as absent so `Absent` matches after a delete and `Value`/`ContentHash`
    /// correctly miss.
    ///
    /// Mirrors the molecule-restore + head-read done by
    /// [`Self::idempotency_hit_matches_current_state`], so it observes the same
    /// freshly-persisted state the write pipeline will build on.
    pub(super) async fn read_current_head(
        &self,
        mutation: &Mutation,
        field: &str,
        storage_prefix: Option<&str>,
    ) -> Result<(Option<String>, Option<serde_json::Value>), SchemaError> {
        let mut schema = self.load_schema_for_point_read(mutation, storage_prefix)?;

        let key_value = Self::resolve_mutation_key_value(&mutation.schema_name, &schema, mutation)?;
        let single = vec![mutation.clone()];
        let key_values = vec![key_value.clone()];
        let changed_keys = Self::changed_keys_by_field(&schema, &single, &key_values);
        self.restore_missing_molecules(&mut schema, &changed_keys)
            .await?;

        let Some(schema_field) = schema.runtime_fields.get(field) else {
            return Err(SchemaError::InvalidField(format!(
                "CAS expectation names field '{field}' absent from schema '{}'",
                mutation.schema_name
            )));
        };

        let hint = schema_field.partition_hint(&key_value);

        let Some(uuid) = current_atom_uuid(schema_field, &key_value) else {
            return Ok((None, None));
        };

        // `key_value` is API-form, so this hint can be wrong under a blinded
        // HashKey encoding; that costs one point read and the flat/locator
        // fallback still returns the right atom.
        // `restore_missing_molecules` can seed the head from an acknowledged
        // resident tip before its atom drains to storage. Resolve the paired
        // resident atom first or this point read can observe a new head as an
        // absent value during that persistence window.
        let content = self
            .read_atom_content(&uuid, hint.as_ref(), storage_prefix)
            .await?;

        // A tombstoned head reads as absent — a deleted record has no live
        // value to compare against.
        if content
            .as_ref()
            .is_some_and(crate::atom::is_tombstone_value)
        {
            return Ok((None, None));
        }

        Ok((Some(uuid), content))
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

    /// Resident atom body when present, else a storage point read.
    async fn read_atom_content(
        &self,
        uuid: &str,
        hint: Option<&crate::atom::AtomPartition>,
        storage_prefix: Option<&str>,
    ) -> Result<Option<serde_json::Value>, SchemaError> {
        Ok(self
            .db_ops
            .atoms()
            .get_atom_by_uuid_in_partition(uuid, hint, storage_prefix)
            .await?
            .map(|atom| atom.content().clone()))
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
    /// The winner comes from the same hydrated tips as the returned values.
    /// AggregateSet uses it to distinguish an exact replay from a newer
    /// byte-identical mutation that the molecule layer would keep as a no-op.
    pub(super) async fn read_current_row_fields_with_winner(
        &self,
        mutation: &Mutation,
        fields: &[String],
        storage_prefix: Option<&str>,
    ) -> Result<(CurrentRowFields, Option<AggregateWinner>), SchemaError> {
        let mut schema = self.load_schema_for_point_read(mutation, storage_prefix)?;
        let key_value = Self::resolve_mutation_key_value(&mutation.schema_name, &schema, mutation)?;
        let mut changed_keys: HashMap<
            String,
            std::collections::HashSet<crate::db_operations::ChangedKey>,
        > = HashMap::new();
        for field in fields {
            let schema_field = schema.runtime_fields.get(field).ok_or_else(|| {
                SchemaError::InvalidField(format!(
                    "point read names field '{field}' absent from schema '{}'",
                    mutation.schema_name
                ))
            })?;
            if let Some(changed) = schema_field.changed_key_for(&key_value) {
                changed_keys
                    .entry(field.clone())
                    .or_default()
                    .insert(changed);
            }
        }
        self.restore_missing_molecules(&mut schema, &changed_keys)
            .await?;

        let mut values = BTreeMap::new();
        let mut saw_tip = false;
        let mut latest_winner: Option<AggregateWinner> = None;
        for field in fields {
            let schema_field = schema.runtime_fields.get(field).ok_or_else(|| {
                SchemaError::InvalidField(format!(
                    "point read names field '{field}' absent from schema '{}'",
                    mutation.schema_name
                ))
            })?;
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
                continue;
            };
            let uuid = entry.atom_uuid.clone();
            saw_tip = true;
            let winner = AggregateWinner {
                logical_counter: entry.logical_counter,
                written_at: entry.written_at,
                writer_id: entry.lww_device().to_string(),
                mutation_uuid: entry.mutation_uuid.clone(),
            };
            match &mut latest_winner {
                Some(current) if winner.cmp_source(current).is_gt() => *current = winner,
                None => latest_winner = Some(winner),
                _ => {}
            }
            let hint = schema_field.partition_hint(&key_value);
            let content = self
                .read_atom_content(&uuid, hint.as_ref(), storage_prefix)
                .await?;
            let content = match content {
                Some(content) if !crate::atom::is_tombstone_value(&content) => content,
                _ => {
                    return Ok((
                        CurrentRowFields::Corrupt {
                            field: field.clone(),
                            atom_uuid: uuid,
                        },
                        None,
                    ));
                }
            };
            values.insert(field.clone(), content);
        }
        if saw_tip {
            Ok((CurrentRowFields::Present(values), latest_winner))
        } else {
            Ok((CurrentRowFields::Absent, None))
        }
    }

    /// Find the current atom entry of one field at one key: resident overlay,
    /// then resident tip, then the durable molecule, then the schema cache.
    /// `None` means the key is tombstoned or has no tip.
    async fn point_read_field_entry(
        &self,
        schema_name: &str,
        field: &str,
        schema_field: &crate::schema::types::field::FieldVariant,
        key_value: &crate::schema::types::KeyValue,
        storage_prefix: Option<&str>,
    ) -> Result<Option<crate::atom::AtomEntry>, SchemaError> {
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
        let changed = std::collections::HashSet::from([changed_key.clone()]);
        let durable_entry = self
            .db_ops
            .atoms()
            .load_molecule_for_write(&molecule_uuid, storage_prefix, &changed)
            .await?
            .and_then(|molecule| {
                molecule
                    .get_atom_entry(changed_key.disk_hash(), changed_key.disk_range())
                    .cloned()
            });
        Ok(durable_entry.or_else(|| schema_field.atom_entry_at_key(key_value).cloned()))
    }
}
