//! Resolve record-envelope candidates in bounded tip and atom batches.

use crate::atom::{molecule_key_codec, Atom, AtomPartition};
use crate::db_operations::atom_store::PerKeyRecord;
use crate::db_operations::DbOperations;
use crate::resident::core_keys::CoreKeyRange;
use crate::schema::types::key_value::KeyValue;
use crate::schema::{Schema, SchemaError};
use crate::storage::disk_pack::logical_paths::record;
use std::collections::{HashMap, HashSet};
use std::ops::Bound;

const BATCH_KEYS: usize = 1000;
struct SelectedAtom {
    base: String,
    uuid: String,
    record: Option<PerKeyRecord>,
}

pub(super) async fn load_record_tip_atoms(
    db_ops: &DbOperations,
    molecule: &str,
    keys: &[KeyValue],
    scope: Option<&str>,
    schema: Option<&Schema>,
) -> Result<HashMap<KeyValue, Atom>, SchemaError> {
    let mut output = HashMap::new();
    for keys in keys.chunks(BATCH_KEYS) {
        let mut cold = Vec::new();
        for key in keys {
            let warm = schema
                .map(|schema| {
                    record::lookup(
                        db_ops.namespaced_store().disk_pack_runtime(),
                        schema,
                        scope,
                        molecule,
                        key,
                    )
                })
                .transpose()?
                .flatten();
            match warm {
                Some(Some(atom)) => {
                    output.insert(key.clone(), atom.as_ref().clone());
                }
                Some(None) => {}
                None => cold.push(key.clone()),
            }
        }
        let candidates = candidate_groups(db_ops, molecule, &cold)?;
        let groups = keep_possible_candidates(db_ops, scope, candidates.clone())?;
        let (groups, resident) = legacy_candidates(db_ops, scope, groups);
        let tips = db_ops
            .atoms()
            .load_first_present_candidate_with_key(scope, &groups)
            .await?;
        let selected: Vec<_> = tips
            .into_iter()
            .zip(resident)
            .map(|(tip, resident)| {
                tip.map(|(base, record)| SelectedAtom {
                    base,
                    uuid: record.entry.atom_uuid.clone(),
                    record: Some(record),
                })
                .or(resident)
            })
            .collect();
        let atoms = load_selected_atoms(db_ops, scope, &selected).await?;
        for (((key, atom), selected), candidates) in
            cold.into_iter().zip(atoms).zip(selected).zip(candidates)
        {
            if let Some(atom) = atom {
                if let (Some(schema), Some(selected)) = (schema, selected) {
                    if let Some(tip) = selected.record {
                        let hint = AtomPartition::from_record_key(&selected.base);
                        record::admit(
                            db_ops.namespaced_store().disk_pack_runtime(),
                            schema,
                            scope,
                            molecule,
                            &key,
                            (&selected.base, &tip, &candidates),
                            hint.as_ref(),
                        )?;
                    }
                }
                output.insert(key, atom);
            } else if selected.is_none() {
                if let Some(schema) = schema {
                    record::admit_absent(
                        db_ops.namespaced_store().disk_pack_runtime(),
                        schema,
                        scope,
                        molecule,
                        &key,
                        &candidates,
                    )?;
                }
            }
        }
    }
    Ok(output)
}

fn candidate_groups(
    db_ops: &DbOperations,
    molecule: &str,
    keys: &[KeyValue],
) -> Result<Vec<Vec<String>>, SchemaError> {
    let codec = db_ops.atoms().key_codec_for_molecule(molecule);
    keys.iter()
        .map(|key| {
            let hash = key.hash.as_deref().unwrap_or_default();
            let range = key.range.as_deref().unwrap_or_default();
            let mut group = vec![molecule_key_codec::hash_range_record_key(
                molecule, hash, range,
            )];
            for encoded in codec
                .api_hash_range_record_keys_for_read(molecule, hash, range)
                .map_err(|error| SchemaError::InvalidData(error.to_string()))?
            {
                if !group.contains(&encoded) {
                    group.push(encoded);
                }
            }
            Ok(group)
        })
        .collect()
}

/// A complete candidate directory certifies absence, not current tip visibility.
fn keep_possible_candidates(
    db_ops: &DbOperations,
    scope: Option<&str>,
    groups: Vec<Vec<String>>,
) -> Result<Vec<Vec<String>>, SchemaError> {
    let Some(runtime) = db_ops.namespaced_store().disk_pack_runtime() else {
        return Ok(groups);
    };
    let mut domains = HashSet::new();
    let mut retained = Vec::with_capacity(groups.len());
    for group in &groups {
        let mut candidates = Vec::new();
        for base in group {
            let Some((molecule, hash, range)) = coordinates(base) else {
                candidates.push(base.clone());
                continue;
            };
            let Some(lease) = runtime.schema_key_lease(scope, molecule) else {
                candidates.push(base.clone());
                continue;
            };
            domains.insert(molecule.to_owned());
            let mut exact = CoreKeyRange::all(&hash, 1);
            exact.start = Bound::Included(&range);
            exact.end = Bound::Included(&range);
            exact.include_tombstones = true;
            if !lease
                .select(exact)
                .map_err(|error| SchemaError::InvalidData(error.to_string()))?
                .keys
                .is_empty()
            {
                candidates.push(base.clone());
            }
        }
        retained.push(candidates);
    }
    // The directory is mutable. A write during selection requires the old
    // captured disk boundary, rather than an answer from a newer key list.
    if domains
        .iter()
        .any(|molecule| runtime.schema_key_lease(scope, molecule).is_none())
    {
        return Ok(groups);
    }
    Ok(retained)
}

fn coordinates(base: &str) -> Option<(&str, String, String)> {
    let molecule = molecule_key_codec::molecule_uuid_from_storage_key(base)?;
    let (hash, range) = molecule_key_codec::decode_hash_range_any(base)?;
    Some((molecule, hash, range))
}

/// Preserve the legacy per-candidate graph-before-disk priority.
fn legacy_candidates(
    db_ops: &DbOperations,
    scope: Option<&str>,
    mut groups: Vec<Vec<String>>,
) -> (Vec<Vec<String>>, Vec<Option<SelectedAtom>>) {
    let mut resident: Vec<Option<SelectedAtom>> = (0..groups.len()).map(|_| None).collect();
    if !crate::db_operations::resident_read::tip_reads_resident_first(db_ops, scope) {
        return (groups, resident);
    }
    for (group, fallback) in groups.iter_mut().zip(&mut resident) {
        let resident_tip = group.iter().enumerate().find_map(|(index, base)| {
            let (molecule, hash, range) = coordinates(base)?;
            db_ops
                .resident()
                .resolve_tip(molecule, &hash, &range)
                .map(|tip| {
                    (
                        index,
                        SelectedAtom {
                            base: base.clone(),
                            uuid: tip.value.atom_uuid,
                            record: None,
                        },
                    )
                })
        });
        if let Some((index, tip)) = resident_tip {
            *fallback = Some(tip);
            group.truncate(index);
        }
    }
    (groups, resident)
}

async fn load_selected_atoms(
    db_ops: &DbOperations,
    scope: Option<&str>,
    selected: &[Option<SelectedAtom>],
) -> Result<Vec<Option<Atom>>, SchemaError> {
    let legacy = crate::db_operations::resident_read::tip_reads_resident_first(db_ops, scope);
    let mut output = vec![None; selected.len()];
    let mut missing = Vec::new();
    for (index, selected) in selected.iter().enumerate() {
        let Some(selected) = selected else {
            continue;
        };
        output[index] = legacy
            .then(|| db_ops.resident().resolve_atom(&selected.uuid))
            .flatten()
            .and_then(|hit| hit.value.into_atom());
        if output[index].is_none() {
            missing.push((
                index,
                selected.uuid.as_str(),
                AtomPartition::from_record_key(&selected.base),
            ));
        }
    }
    if missing.is_empty() {
        return Ok(output);
    }
    let slots: Vec<_> = missing
        .iter()
        .map(|(_, uuid, hint)| (*uuid, hint.clone()))
        .collect();
    let atoms = db_ops
        .atoms()
        .get_atoms_located(&slots, scope)
        .await
        .map_err(SchemaError::read_failure)?;
    for ((index, _, _), atom) in missing.into_iter().zip(atoms) {
        if atom.is_none() && scope.is_none() {
            return Err(SchemaError::MissingLocalAtom);
        }
        output[index] = atom;
    }
    Ok(output)
}
