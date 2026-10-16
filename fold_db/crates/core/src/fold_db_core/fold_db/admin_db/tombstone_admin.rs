//! Legacy tombstone flag audit and drain over the owner socket.

use super::*;
use crate::fold_db_core::purge::StorageSlotPurgeTarget;
use crate::schema::types::field::FieldKind;

/// Owning `(schema, field, kind)` rows for one molecule uuid, sorted.
type MoleculeOwners = HashMap<String, Vec<(String, String, FieldKind)>>;

impl FoldDB {
    /// Audit `KeyMetadata.tombstoned` against atom content, optionally stamping
    /// the flag onto legacy `mk:` records whose content is already a tombstone.
    ///
    /// `schema` narrows the walk to one schema's field molecules; `None` audits
    /// the whole store. A named schema that resolves to no molecules is an error
    /// rather than a silent whole-store scan — the difference between "that
    /// schema has nothing" and "I just read your entire database" is not one to
    /// discover from the runtime.
    ///
    /// The name may be either the stored schema name (a hash on a real home) or
    /// a `descriptive_name`. A descriptive name can match several registered
    /// schemas — successive versions of the same product schema — and all of
    /// their molecules are audited together, because a page over that data is
    /// served from all of them.
    ///
    /// `max_keys` / `after_key` bound one call and resume the next; see
    /// [`crate::db_operations::AtomStore::audit_key_tombstone_flags`].
    pub async fn audit_tombstone_flags(
        &self,
        schema: Option<&str>,
        stamp: bool,
        max_keys: Option<usize>,
        after_key: Option<&str>,
    ) -> Result<TombstoneFlagBackfillReport, FoldDbError> {
        let molecules = match schema {
            None => None,
            Some(name) => {
                let mut matched: Vec<(String, Schema)> = Vec::new();
                if let Ok(Some(schema)) = self.schema_manager().get_schema_metadata(name) {
                    matched.push((name.to_string(), schema));
                } else {
                    let all = self
                        .db_ops()
                        .get_all_schemas()
                        .await
                        .map_err(|e| FoldDbError::Database(format!("list schemas: {e}")))?;
                    matched
                        .extend(all.into_iter().filter(|(_, schema)| {
                            schema.descriptive_name.as_deref() == Some(name)
                        }));
                }
                if matched.is_empty() {
                    // The operator named a schema; nothing resolves to it. That
                    // is a 404 about their argument, not a server fault.
                    return Err(FoldDbError::Schema(crate::schema::SchemaError::NotFound(
                        format!("unknown schema: {name}"),
                    )));
                }
                let mut map = std::collections::HashMap::new();
                for (stored_name, schema) in &matched {
                    // A stored name is a hash on a real home; the descriptive
                    // name is what the operator asked about, so label with it.
                    let label = schema.descriptive_name.as_deref().unwrap_or(stored_name);
                    for (field_name, field) in &schema.runtime_fields {
                        if let Some(uuid) = field.common().molecule_uuid() {
                            map.insert(uuid.clone(), format!("{label}.{field_name}"));
                        }
                    }
                }
                if map.is_empty() {
                    // Resolvable but not auditable: still a fact about the
                    // caller's selector, so 400 rather than 500.
                    return Err(FoldDbError::Schema(
                        crate::schema::SchemaError::InvalidField(format!(
                            "schema {name} has no field molecules to audit"
                        )),
                    ));
                }
                Some(map)
            }
        };
        self.db_ops()
            .atoms()
            .audit_key_tombstone_flags(molecules.as_ref(), stamp, max_keys, after_key, None)
            .await
            .map_err(|e| admin_op_error("audit_key_tombstone_flags", e))
    }

    /// Drain one bounded page of legacy tombstone-content tips into the same
    /// reachability-guarded erasure core used by purge.
    ///
    /// The walk and resume cursor are storage-shaped because BlindV1 hashes
    /// cannot be reversed into caller keys. `dry_run` performs the identical
    /// discovery/ownership classification without deleting anything.
    pub async fn drain_legacy_tombstones(
        &self,
        schema: Option<&str>,
        dry_run: bool,
        max_keys: usize,
        after_key: Option<&str>,
    ) -> Result<LegacyTombstoneDrainReport, FoldDbError> {
        let schemas = self.select_tombstone_drain_schemas(schema).await?;
        let owners = tombstone_molecule_owners(&schemas);
        let molecules: HashSet<String> = owners.keys().cloned().collect();
        if molecules.is_empty() {
            return Err(FoldDbError::Schema(
                crate::schema::SchemaError::InvalidField(
                    "selected schemas have no field molecules to drain".into(),
                ),
            ));
        }

        let scan = self
            .db_ops()
            .atoms()
            .scan_legacy_tombstone_slots(&molecules, max_keys.max(1), after_key, None)
            .await
            .map_err(|e| admin_op_error("scan legacy tombstones", e))?;
        let mut report = LegacyTombstoneDrainReport {
            dry_run,
            keys_scanned: scan.keys_scanned,
            keys_unreadable: scan.keys_unreadable,
            atoms_fetched: scan.atoms_fetched,
            atoms_missing: scan.atoms_missing,
            tombstones_found: scan.slots.len() as u64,
            more_remaining: scan.more_remaining,
            next_after_key: scan.next_after_key,
            ..Default::default()
        };

        let by_schema = group_slots_by_schema(&owners, scan.slots, &mut report);
        if dry_run {
            return Ok(report);
        }
        self.purge_tombstone_groups(by_schema, &mut report).await?;
        Ok(report)
    }

    /// Load the schemas a drain pass covers, sorted by stored name.
    async fn select_tombstone_drain_schemas(
        &self,
        schema: Option<&str>,
    ) -> Result<Vec<(String, Schema)>, FoldDbError> {
        let mut schemas: Vec<(String, Schema)> = self
            .db_ops()
            .get_all_schemas()
            .await
            .map_err(|e| FoldDbError::Database(format!("list schemas for tombstone drain: {e}")))?
            .into_iter()
            .filter(|(stored_name, value)| {
                schema.is_none_or(|wanted| {
                    stored_name == wanted || value.descriptive_name.as_deref() == Some(wanted)
                })
            })
            .collect();
        if schema.is_some() && schemas.is_empty() {
            return Err(FoldDbError::Schema(crate::schema::SchemaError::NotFound(
                format!("unknown schema: {}", schema.unwrap_or_default()),
            )));
        }
        schemas.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(schemas)
    }

    /// Purge each schema's grouped slots under the guarded erasure core.
    async fn purge_tombstone_groups(
        &self,
        mut by_schema: HashMap<String, Vec<StorageSlotPurgeTarget>>,
        report: &mut LegacyTombstoneDrainReport,
    ) -> Result<(), FoldDbError> {
        let mut schema_names: Vec<String> = by_schema.keys().cloned().collect();
        schema_names.sort();
        for schema_name in schema_names {
            let targets = by_schema
                .remove(&schema_name)
                .expect("schema_names came from by_schema");
            let evidence = self
                .mutation_manager()
                .purge_storage_slots_guarded(&schema_name, &targets)
                .await
                .map_err(|e| {
                    FoldDbError::Database(format!(
                        "drain legacy tombstones for schema {schema_name}: {e}"
                    ))
                })?;
            let drained = evidence.len() as u64;
            report.tombstones_drained += drained;
            report.search_tombstones_queued += drained;
            report.per_schema_drained.insert(schema_name, drained);
        }
        Ok(())
    }
}

/// Map each molecule uuid to the sorted fields that own it.
fn tombstone_molecule_owners(schemas: &[(String, Schema)]) -> MoleculeOwners {
    let mut owners: MoleculeOwners = HashMap::new();
    for (schema_name, value) in schemas {
        for (field_name, field) in &value.runtime_fields {
            if let Some(molecule) = field.common().molecule_uuid() {
                owners.entry(molecule.clone()).or_default().push((
                    schema_name.clone(),
                    field_name.clone(),
                    field.kind,
                ));
            }
        }
    }
    for rows in owners.values_mut() {
        rows.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
    }
    owners
}

/// Group scanned slots by owning schema; count slots with no compatible owner.
fn group_slots_by_schema(
    owners: &MoleculeOwners,
    slots: Vec<crate::db_operations::admin_db::LegacyTombstoneSlot>,
    report: &mut LegacyTombstoneDrainReport,
) -> HashMap<String, Vec<StorageSlotPurgeTarget>> {
    let mut by_schema: HashMap<String, Vec<StorageSlotPurgeTarget>> = HashMap::new();
    for slot in slots {
        let compatible = |kind: FieldKind| match kind {
            FieldKind::Single => slot.storage_hash.is_empty() && slot.storage_range.is_empty(),
            FieldKind::Hash => !slot.storage_hash.is_empty() && slot.storage_range.is_empty(),
            FieldKind::Range => slot.storage_hash.is_empty() && !slot.storage_range.is_empty(),
            FieldKind::HashRange => !slot.storage_hash.is_empty() && !slot.storage_range.is_empty(),
        };
        let owner = owners
            .get(&slot.molecule_uuid)
            .and_then(|rows| rows.iter().find(|(_, _, kind)| compatible(*kind)));
        let Some((schema_name, field_name, _)) = owner else {
            report.unowned_tombstones += 1;
            continue;
        };
        by_schema.entry(schema_name.clone()).or_default().push(
            StorageSlotPurgeTarget::new(
                field_name,
                &slot.molecule_uuid,
                slot.storage_hash,
                slot.storage_range,
            )
            .with_expected_tombstone(slot.atom_uuid),
        );
    }
    by_schema
}
