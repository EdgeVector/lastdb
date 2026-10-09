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
    // lint:fn-size-ok verbatim move from cas.rs; splitting this function is separate work
    pub(super) async fn read_current_head(
        &self,
        mutation: &Mutation,
        field: &str,
        storage_prefix: Option<&str>,
    ) -> Result<(Option<String>, Option<serde_json::Value>), SchemaError> {
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

        // Prefer the acknowledged resident tip over the catalog molecule head.
        // A sibling schema that shares a node identity can reload the catalog
        // with a stale `state` tip while event_id/log_excerpt already advanced.
        // CAS that followed the catalog atom UUID then compared the pre-write
        // value (`expected "pending", found "failure"` on 2026-09-02).
        let hint = schema_field.partition_hint(&key_value);
        if let (Some(mol_uuid), Some(changed)) = (
            schema_field.common().molecule_uuid(),
            schema_field.changed_key_for(&key_value),
        ) {
            // A hard-erase (`Delete`/`Purge`) stamps this resident overlay
            // synchronously, before its ack, and removes the resident tip
            // in the same step — but the catalog molecule head below is
            // only updated later by the deferred converge/purge lane. A
            // CAS read that lands in that window would otherwise fall
            // through resolve_tip's miss straight to the stale catalog
            // uuid and its still-undeleted atom body, which is never
            // tombstone-shaped for a `Delete` (no per-field tombstone
            // atom is written — see `MutationType::Delete`), so the old
            // value would read as live. Check the overlay first, exactly
            // like the sibling `read_current_row_fields_with_winner`
            // does, so a tombstoned key reads as absent regardless of
            // which path (resident tip or catalog fallback) would
            // otherwise have answered next.
            //
            // Org-prefix Delete stamps the same overlay
            // (`purge_apply_gate_transaction`). Skipping the consult when
            // `storage_prefix` is set made a byte-identical org Create a
            // catalog-head no-op while count already hid the row.
            if self.db_ops.resident().is_key_tombstoned(
                mol_uuid,
                changed.disk_hash(),
                changed.disk_range(),
            ) {
                return Ok((None, None));
            }
            if let Some(tip) = self.db_ops.resident().resolve_tip(
                mol_uuid,
                changed.disk_hash(),
                changed.disk_range(),
            ) {
                let uuid = tip.value.atom_uuid;
                let content = if let Some(atom) = self.db_ops.resident().resolve_atom(&uuid) {
                    Some(atom.value.content)
                } else {
                    self.db_ops
                        .atoms()
                        .get_atom_by_uuid_in_partition(&uuid, hint.as_ref(), storage_prefix)
                        .await?
                        .map(|atom| atom.content().clone())
                };
                if content
                    .as_ref()
                    .is_some_and(crate::atom::is_tombstone_value)
                {
                    return Ok((None, None));
                }
                return Ok((Some(uuid), content));
            }
        }

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
        let content = if let Some(atom) = self.db_ops.resident().resolve_atom(&uuid) {
            Some(atom.value.content)
        } else {
            self.db_ops
                .atoms()
                .get_atom_by_uuid_in_partition(&uuid, hint.as_ref(), storage_prefix)
                .await?
                .map(|atom| atom.content().clone())
        };

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
    // lint:fn-size-ok verbatim move from cas.rs; splitting this function is separate work
    pub(super) async fn read_current_row_fields_with_winner(
        &self,
        mutation: &Mutation,
        fields: &[String],
        storage_prefix: Option<&str>,
    ) -> Result<(CurrentRowFields, Option<AggregateWinner>), SchemaError> {
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
            let changed_key = schema_field.changed_key_for(&key_value).ok_or_else(|| {
                SchemaError::InvalidData(format!(
                    "point read cannot resolve field '{field}' key in schema '{}'",
                    mutation.schema_name
                ))
            })?;
            let molecule_uuid =
                schema_field
                    .common()
                    .molecule_uuid()
                    .cloned()
                    .ok_or_else(|| {
                        SchemaError::InvalidData(format!(
                            "point read field '{}.{field}' has no molecule identity",
                            mutation.schema_name
                        ))
                    })?;
            if self.db_ops.resident().is_key_tombstoned(
                &molecule_uuid,
                changed_key.disk_hash(),
                changed_key.disk_range(),
            ) {
                continue;
            }
            let resident_entry = self
                .db_ops
                .resident()
                .resolve_tip(
                    &molecule_uuid,
                    changed_key.disk_hash(),
                    changed_key.disk_range(),
                )
                .map(|tip| {
                    crate::atom::AtomEntry::thin_with_author(
                        tip.value.atom_uuid,
                        tip.value.written_at,
                        tip.value.logical_counter,
                        tip.value.device_id,
                        tip.value.mutation_uuid,
                        String::new(),
                    )
                });
            let durable_entry = if resident_entry.is_none() {
                let changed = std::collections::HashSet::from([changed_key.clone()]);
                self.db_ops
                    .atoms()
                    .load_molecule_for_write(&molecule_uuid, storage_prefix, &changed)
                    .await?
                    .and_then(|molecule| {
                        molecule
                            .get_atom_entry(changed_key.disk_hash(), changed_key.disk_range())
                            .cloned()
                    })
            } else {
                None
            };
            let Some(entry) = resident_entry
                .or(durable_entry)
                .or_else(|| schema_field.atom_entry_at_key(&key_value).cloned())
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
            let content = if let Some(atom) = self.db_ops.resident().resolve_atom(&uuid) {
                Some(atom.value.content.clone())
            } else {
                self.db_ops
                    .atoms()
                    .get_atom_by_uuid_in_partition(&uuid, hint.as_ref(), storage_prefix)
                    .await?
                    .map(|atom| atom.content().clone())
            };
            let Some(content) = content else {
                return Ok((
                    CurrentRowFields::Corrupt {
                        field: field.clone(),
                        atom_uuid: uuid,
                    },
                    None,
                ));
            };
            if crate::atom::is_tombstone_value(&content) {
                return Ok((
                    CurrentRowFields::Corrupt {
                        field: field.clone(),
                        atom_uuid: uuid,
                    },
                    None,
                ));
            }
            values.insert(field.clone(), content);
        }
        if saw_tip {
            Ok((CurrentRowFields::Present(values), latest_winner))
        } else {
            Ok((CurrentRowFields::Absent, None))
        }
    }
}
