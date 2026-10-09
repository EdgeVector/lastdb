//! Exact storage-slot purge: targets, planning, and the chunked bulk drain.

use super::*;

/// One exact, already-encoded molecule slot discovered from storage.
///
/// The hash/range segments are deliberately private and this type does not
/// implement `Debug`: BlindV1/OpeV1 storage material must not drift into logs.
/// Callers construct it from a decoded `mk:` suffix and pass it straight to the
/// guarded purge path; no API-key reconstruction or codec pass occurs.
#[derive(Clone, PartialEq, Eq)]
pub struct StorageSlotPurgeTarget {
    field_name: String,
    molecule_uuid: String,
    storage_key: KeyValue,
}

impl StorageSlotPurgeTarget {
    pub fn new(
        field_name: impl Into<String>,
        molecule_uuid: impl Into<String>,
        storage_hash: impl Into<String>,
        storage_range: impl Into<String>,
    ) -> Self {
        Self {
            field_name: field_name.into(),
            molecule_uuid: molecule_uuid.into(),
            storage_key: KeyValue::new(Some(storage_hash.into()), Some(storage_range.into())),
        }
    }

    /// Approximate heap bytes retained by one persist-lane envelope.
    pub(in crate::fold_db_core) fn approx_bytes(&self) -> u64 {
        u64::try_from(
            self.field_name
                .len()
                .saturating_add(self.molecule_uuid.len())
                .saturating_add(self.storage_key.hash.as_deref().map_or(0, str::len))
                .saturating_add(self.storage_key.range.as_deref().map_or(0, str::len)),
        )
        .unwrap_or(u64::MAX)
    }
}

/// Validate permanent storage-slot properties before lane admission.
///
/// The lane retries storage failures. A malformed target cannot become valid
/// through a retry, so it must fail while the caller still owns the request.
pub(in crate::fold_db_core) fn validate_storage_slot_purge_targets(
    schema_name: &str,
    schema: &crate::schema::types::Schema,
    targets: &[StorageSlotPurgeTarget],
) -> Result<(), SchemaError> {
    for target in targets {
        let field = schema
            .runtime_fields
            .get(&target.field_name)
            .ok_or_else(|| {
                SchemaError::InvalidData(format!(
                    "Storage-slot purge field '{}' is not present in schema '{schema_name}'",
                    target.field_name
                ))
            })?;
        let actual_molecule = field.common().molecule_uuid().ok_or_else(|| {
            SchemaError::InvalidData(format!(
                "Storage-slot purge field '{}' has no molecule in schema '{schema_name}'",
                target.field_name
            ))
        })?;
        if actual_molecule != &target.molecule_uuid {
            return Err(SchemaError::InvalidData(format!(
                "Storage-slot purge molecule does not belong to field '{}' in schema '{schema_name}'",
                target.field_name
            )));
        }
        if field.disk_slot_for_key(&target.storage_key).is_none() {
            return Err(SchemaError::InvalidData(format!(
                "Storage-slot purge target has the wrong key shape for field '{}' in schema '{schema_name}'",
                target.field_name
            )));
        }
    }
    Ok(())
}

/// Typed post-commit evidence for one storage-slot invalidation.
///
/// The persist lane turns [`Self::storage_key_value`] directly into an
/// `IndexChangeKind::Tombstone`. It does not print or reverse opaque storage
/// segments.
#[derive(Clone)]
pub struct StorageSlotPurgeEvidence {
    field_name: String,
    molecule_uuid: String,
    storage_key: KeyValue,
}

pub(in crate::fold_db_core) struct StorageSlotBulkPurgeResult {
    pub report: BulkPurgeReport,
}

impl StorageSlotPurgeEvidence {
    pub fn field_name(&self) -> &str {
        &self.field_name
    }

    pub fn molecule_uuid(&self) -> &str {
        &self.molecule_uuid
    }

    pub fn storage_key_value(&self) -> &KeyValue {
        &self.storage_key
    }
}

#[derive(Clone)]
pub(in crate::fold_db_core) struct PlannedStorageSlot {
    pub(super) field_name: String,
    pub(super) molecule_uuid: String,
    pub(super) storage_key: KeyValue,
    /// API-form coordinates for T0 resident eviction. Normal record purge has
    /// these; storage-slot drains address already-encoded legacy rows directly.
    pub(super) resident_key: Option<KeyValue>,
    pub(super) disk_hash: String,
    pub(super) disk_range: String,
}

pub(in crate::fold_db_core) type StorageSlotIdentity = (String, String, String);
pub(in crate::fold_db_core) type ResidentApiKey = (String, String);

impl PlannedStorageSlot {
    pub(super) fn identity(&self) -> StorageSlotIdentity {
        (
            self.molecule_uuid.clone(),
            self.disk_hash.clone(),
            self.disk_range.clone(),
        )
    }
}

/// Schema-wide work that a storage-slot purge needs once, not once per barrier
/// chunk.
///
/// Both members are `O(F·R)` in the schema's fields and records, and neither
/// depends on which slots a chunk carries. Recomputing them inside the chunk
/// loop turned an `O(F·R)` drain into `O(N/CHUNK · F·R)`: on the primary's
/// 943k-tombstone population that was 25–35 s per 64-slot chunk, about 2
/// records/sec, or ~130 hours for the store. See
/// [`prepare_storage_slot_purge`].
pub(in crate::fold_db_core) struct StorageSlotPurgePrep {
    schema: crate::schema::types::Schema,
    /// Atoms named by history rows the purge must preserve.
    history_referenced_by_retained: HashSet<String>,
}

/// Do a schema's once-per-drain purge work: resolve live molecules and collect
/// the atoms retained history still names.
///
/// # Why this is safe to hoist above the chunk loop
///
/// `history_referenced_by_retained` is a **preserve** set: every atom in it is
/// excluded from deletion. Computing it once, before any chunk runs, snapshots
/// a strictly *larger* set than recomputing it after earlier chunks have
/// already removed history rows. The hoist therefore errs toward deleting less,
/// never more, which is the safe direction for a destructive verb.
///
/// The cost is that a write landing mid-drain is not reflected until the next
/// call. Callers scope one prep to one owner-scan page (see
/// `purge_storage_slots_guarded`), so the staleness window is a single daemon
/// call, and the direction of the error still preserves rather than deletes.
pub(in crate::fold_db_core) async fn prepare_storage_slot_purge(
    db_ops: &Arc<DbOperations>,
    schema_manager: &Arc<SchemaCore>,
    schema_name: &str,
) -> Result<StorageSlotPurgePrep, SchemaError> {
    let mut schema = schema_manager
        .get_schema_metadata(schema_name)?
        .ok_or_else(|| {
            SchemaError::InvalidData(format!(
                "Schema '{schema_name}' not found for storage-slot purge"
            ))
        })?;
    refresh_runtime_field_molecules(db_ops, &mut schema).await?;

    // A storage cursor cannot correlate a BlindV1/OpeV1 slot to plaintext
    // legacy `history:` FieldKeys. Preserve every history row and every atom it
    // names instead of guessing. Current stores use `tv:` chains; this is the
    // conservative legacy fallback required for safe owner scans.
    let mut history_referenced_by_retained = HashSet::new();
    for field in schema.runtime_fields.values() {
        let Some(molecule_uuid) = field.common().molecule_uuid() else {
            continue;
        };
        for event in db_ops
            .atoms()
            .get_mutation_events(molecule_uuid, None)
            .await?
        {
            collect_event_atom_uuids(&event, &mut history_referenced_by_retained);
        }
    }

    Ok(StorageSlotPurgePrep {
        schema,
        history_referenced_by_retained,
    })
}

/// Plan one batch of exact storage-form slots, then execute it through the same
/// guarded destructive core as API-key purge.
///
/// Callers must hold the schema purge barrier exclusively. The
/// [`crate::fold_db_core::mutation_manager::MutationManager`] entry point owns
/// that barrier and chunks large owner-scan batches before reaching here.
///
/// `missing` decides what an already-absent slot means. A compliance purge
/// handed an explicit key must [`PurgeMissingPolicy::Refuse`], but a
/// scan-driven drain must [`PurgeMissingPolicy::Skip`]: tombstone atoms are
/// content-addressed, so every field of one tombstoned record shares a single
/// atom, and purging the first field's slot strips the atom the same record's
/// other queued slots still point at. Refusing there aborted whole drains.
// These arguments keep the shared purge core explicit at its only call site.
#[allow(clippy::too_many_arguments)]
pub(in crate::fold_db_core) async fn purge_storage_slots_bulk(
    db_ops: &Arc<DbOperations>,
    schema_manager: &Arc<SchemaCore>,
    schema_name: &str,
    targets: &[StorageSlotPurgeTarget],
    missing: PurgeMissingPolicy,
    prep: &StorageSlotPurgePrep,
    acct: &mut PurgeCommitAccounting,
    evidence_checkpoint: &mut Vec<StorageSlotPurgeEvidence>,
) -> Result<StorageSlotBulkPurgeResult, SchemaError> {
    // lint:fn-size-ok verbatim move from purge/mod.rs; splitting this function is separate work
    if targets.is_empty() {
        return Ok(StorageSlotBulkPurgeResult {
            report: BulkPurgeReport::empty(),
        });
    }

    let schema = &prep.schema;

    let mut planned = Vec::new();
    let mut seen: HashSet<(String, String, String)> = HashSet::new();
    let mut candidate_atoms: HashSet<String> = HashSet::new();
    let mut tip_version_keys = Vec::new();
    let mut atom_ref_v2_edge_keys = Vec::new();

    for target in targets {
        let field = schema
            .runtime_fields
            .get(&target.field_name)
            .ok_or_else(|| {
                SchemaError::InvalidData(format!(
                    "Storage-slot purge field '{}' is not present in schema '{schema_name}'",
                    target.field_name
                ))
            })?;
        let actual_molecule = field.common().molecule_uuid().ok_or_else(|| {
            SchemaError::InvalidData(format!(
                "Storage-slot purge field '{}' has no molecule in schema '{schema_name}'",
                target.field_name
            ))
        })?;
        if actual_molecule != &target.molecule_uuid {
            return Err(SchemaError::InvalidData(format!(
                "Storage-slot purge molecule does not belong to field '{}' in schema '{schema_name}'",
                target.field_name
            )));
        }
        let Some((disk_hash, disk_range)) = field.disk_slot_for_key(&target.storage_key) else {
            return Err(SchemaError::InvalidData(format!(
                "Storage-slot purge target has the wrong key shape for field '{}' in schema '{schema_name}'",
                target.field_name
            )));
        };
        let identity = (
            target.molecule_uuid.clone(),
            disk_hash.clone(),
            disk_range.clone(),
        );
        if !seen.insert(identity) {
            continue;
        }
        let Some(atom_uuid) = current_atom_for_key(field, &target.storage_key) else {
            match missing {
                PurgeMissingPolicy::Refuse => {
                    return Err(SchemaError::InvalidData(format!(
                        "Storage-slot purge target not found in field '{}' of schema '{schema_name}'",
                        target.field_name
                    )));
                }
                // Already gone is the expected steady state for a scan-driven
                // drain: an earlier chunk in this same run can legitimately
                // have erased this slot's shared tombstone atom.
                PurgeMissingPolicy::Skip => continue,
            }
        };
        candidate_atoms.insert(atom_uuid);
        let chain = collect_target_chain(db_ops, field, &target.storage_key).await?;
        tip_version_keys.extend(chain.tip_version_keys);
        atom_ref_v2_edge_keys.extend(chain.atom_ref_v2_edge_keys);
        candidate_atoms.extend(chain.atom_uuids);
        planned.push(PlannedStorageSlot {
            field_name: target.field_name.clone(),
            molecule_uuid: target.molecule_uuid.clone(),
            storage_key: target.storage_key.clone(),
            resident_key: None,
            disk_hash,
            disk_range,
        });
    }

    // Every slot in this chunk was already erased (see `PurgeMissingPolicy::Skip`
    // above) — nothing to execute, and `execute_guarded_purge` must not be
    // handed an empty plan.
    if planned.is_empty() {
        return Ok(StorageSlotBulkPurgeResult {
            report: BulkPurgeReport::empty(),
        });
    }

    let history_referenced_by_retained = prep.history_referenced_by_retained.clone();

    let evidence = planned
        .iter()
        .map(|slot| StorageSlotPurgeEvidence {
            field_name: slot.field_name.clone(),
            molecule_uuid: slot.molecule_uuid.clone(),
            storage_key: slot.storage_key.clone(),
        })
        .collect::<Vec<_>>();
    // Keep the invalidation intent before the first destructive operation.
    // A flush can fail after the core removes the slot and reloads the
    // schema. The next idempotent attempt then finds no slot, so only this
    // retained checkpoint can still produce the Search tombstone.
    for item in &evidence {
        let duplicate = evidence_checkpoint.iter().any(|saved| {
            saved.field_name == item.field_name
                && saved.molecule_uuid == item.molecule_uuid
                && saved.storage_key == item.storage_key
        });
        if !duplicate {
            evidence_checkpoint.push(item.clone());
        }
    }
    // The descriptor is consumed only by the delete-ledger fingerprint helper;
    // it is never persisted or logged. That preserves per-slot audit identity
    // without exposing opaque storage segments in operator-facing material.
    let ledger_descriptor = if planned.len() == 1 {
        format!(
            "storage-slot:{}:{}",
            planned[0].molecule_uuid,
            describe_key(&planned[0].storage_key)
        )
    } else {
        format!("<bulk-storage-slots:{}>", planned.len())
    };
    let report = execute_guarded_purge(
        db_ops,
        schema_manager,
        schema_name,
        schema.clone(),
        &planned,
        candidate_atoms,
        Vec::new(),
        tip_version_keys,
        Vec::new(),
        atom_ref_v2_edge_keys,
        history_referenced_by_retained,
        planned.len(),
        &ledger_descriptor,
        HardEraseVerb::Purge,
        acct,
        true,
        PurgeReachability::GuardedComplement,
        None,
    )
    .await?;
    Ok(StorageSlotBulkPurgeResult { report })
}
