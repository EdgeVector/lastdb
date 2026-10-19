//! A protein move retains values and retires only its captured old association.

use super::super::MutationManager;
use super::prepared::{SiblingMove, SiblingOrigin, SiblingUpdate, SourceId};
use super::protein_batch::{schema_storage_prefix, ProteinFoldMetadata};
use super::protein_key_fields::ProteinKeyFields;
use crate::atom::{molecule_key_codec, Atom, MoleculeHashRange};
use crate::db_operations::atom_store::{ChangedKey, MoleculeHeader, PerKeyRecord};
use crate::schema::types::field::build_storage_key;
use crate::schema::types::{KeyValue, Schema};
use crate::schema::SchemaError;
use chrono::Utc;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

pub(super) struct ProteinMoveInput<'a> {
    pub(super) schema: &'a Schema,
    pub(super) keys: &'a [KeyValue],
    pub(super) eligible: &'a [(usize, &'a Atom, &'a str)],
    pub(super) fields: &'a ProteinKeyFields,
    pub(super) metadata: &'a ProteinFoldMetadata,
}

struct MoveRequest {
    index: usize,
    source_molecule: String,
    molecule: String,
    old: KeyValue,
    new: KeyValue,
    trigger: SourceId,
    value: Option<String>,
    source_slot: usize,
    old_slot: usize,
    new_slot: usize,
    association_slots: Vec<usize>,
}

type Slot = (String, String, String);
type UpdateIndex = HashMap<(usize, String, String, String, String), usize>;

struct MoveState {
    records: Vec<Option<PerKeyRecord>>,
    headers: HashMap<String, MoleculeHeader>,
}

impl MutationManager {
    pub(super) async fn prepare_protein_moves(
        &self,
        input: &ProteinMoveInput<'_>,
        updates: &mut Vec<SiblingUpdate>,
        origins: &mut Vec<SiblingOrigin>,
    ) -> Result<Vec<SiblingMove>, SchemaError> {
        if self.db_ops.namespaced_store().disk_pack_runtime().is_none() {
            return Ok(Vec::new());
        }
        let mut slots = BTreeMap::new();
        let requests = move_requests(input, &mut slots)?;
        if requests.is_empty() {
            return Ok(Vec::new());
        }
        let state = self.read_move_state(input, slots, &requests).await?;
        let mut index = update_index(updates, origins);
        let mut movements = Vec::new();
        for request in requests {
            let Some(source) = state.records[request.source_slot].as_ref() else {
                continue;
            };
            let updated =
                self.ensure_moved_value(&request, source, &state, updates, origins, &mut index);
            let Some(old) = state.records[request.old_slot].as_ref() else {
                continue;
            };
            // An equal body alone does not establish which record owns a
            // colliding destination. Require the old source association too.
            if old.entry.atom_uuid != source.entry.atom_uuid
                || old.meta != source.meta
                || (old.entry.lww_key() != source.entry.lww_key()
                    && !request.association_slots.iter().any(|&slot| {
                        state.records[slot]
                            .as_ref()
                            .is_some_and(|record| record.entry.lww_key() == old.entry.lww_key())
                    }))
            {
                continue;
            }
            movements.push(SiblingMove {
                source: origins[updated].source.clone(),
                molecule_uuid: request.molecule,
                old_key: request.old,
                new_key: request.new,
                expected_old: old.clone(),
            });
        }
        Ok(movements)
    }

    async fn read_move_state(
        &self,
        input: &ProteinMoveInput<'_>,
        slots: BTreeMap<Slot, usize>,
        requests: &[MoveRequest],
    ) -> Result<MoveState, SchemaError> {
        let atoms = self.db_ops.atoms();
        let prefix = schema_storage_prefix(input.schema, None);
        let mut groups = vec![Vec::new(); slots.len()];
        for ((molecule, hash, range), position) in slots {
            groups[position] = atoms
                .key_codec_for_molecule(&molecule)
                .api_hash_range_record_keys_for_read(&molecule, &hash, &range)
                .map_err(|error| SchemaError::InvalidData(error.to_string()))?;
        }
        let molecules: BTreeSet<_> = requests.iter().map(|request| &request.molecule).collect();
        let keys: Vec<_> = molecules
            .iter()
            .map(|uuid| build_storage_key(prefix, &molecule_key_codec::header_key(uuid)))
            .collect();
        let headers = async {
            atoms
                .raw()
                .get_items::<MoleculeHeader>(&keys)
                .await
                .map_err(|error| SchemaError::StorageRead {
                    detail: format!("read protein move headers: {error}"),
                })
        };
        let (records, headers) =
            tokio::try_join!(atoms.load_first_present_candidate(prefix, &groups), headers)?;
        if records.len() != groups.len() || headers.len() != molecules.len() {
            return Err(SchemaError::StorageRead {
                detail: "protein move winner or header count differs".into(),
            });
        }
        Ok(MoveState {
            records,
            headers: molecules
                .into_iter()
                .zip(headers)
                .filter_map(|(uuid, header)| header.map(|header| (uuid.clone(), header)))
                .collect(),
        })
    }

    fn ensure_moved_value(
        &self,
        request: &MoveRequest,
        source: &PerKeyRecord,
        state: &MoveState,
        updates: &mut Vec<SiblingUpdate>,
        origins: &mut Vec<SiblingOrigin>,
        index: &mut UpdateIndex,
    ) -> usize {
        let key = update_key(request);
        let existing = index.get(&key).copied();
        let unchanged = request
            .value
            .as_ref()
            .is_none_or(|uuid| uuid == &source.entry.atom_uuid);
        if let Some(existing) = existing {
            if unchanged {
                origins[existing].source.clone_from(&request.trigger);
                origins[existing].replacement =
                    Some((source.entry.atom_uuid.clone(), source.meta.clone()));
            }
            return existing;
        }
        let hash = request.new.hash.clone().unwrap_or_default();
        let range = request.new.range.clone().unwrap_or_default();
        let destination = state.records[request.new_slot].as_ref();
        let base = destination.map(|record| (record.entry.clone(), record.meta.clone()));
        let records = destination
            .map(|record| {
                vec![(
                    hash.clone(),
                    range.clone(),
                    record.entry.clone(),
                    record.meta.clone(),
                )]
            })
            .unwrap_or_default();
        let header = state.headers.get(&request.molecule);
        let mut data = MoleculeHashRange::from_write_records(
            request.molecule.clone(),
            header.map_or(0, |header| header.version),
            header.map_or_else(Utc::now, |header| header.updated_at),
            records,
        );
        data.set_atom_uuid_from_values(
            hash.clone(),
            range.clone(),
            source.entry.atom_uuid.clone(),
            self.signer.as_ref(),
        );
        let position = updates.len();
        updates.push((
            request.molecule.clone(),
            data,
            HashSet::from([ChangedKey::hash_range(hash, range)]),
        ));
        origins.push(SiblingOrigin {
            source: request.trigger.clone(),
            base,
            replacement: Some((source.entry.atom_uuid.clone(), source.meta.clone())),
        });
        index.insert(key, position);
        position
    }
}

fn move_requests(
    input: &ProteinMoveInput<'_>,
    slots: &mut BTreeMap<Slot, usize>,
) -> Result<Vec<MoveRequest>, SchemaError> {
    let mut requests = Vec::new();
    for &idx in input.fields.after.keys() {
        let before = &input.fields.before[&idx];
        let after = &input.fields.after[&idx];
        for field in input.schema.runtime_fields.values() {
            let Some(source) = field.common().molecule_uuid() else {
                continue;
            };
            let Some(owner) = input.metadata.0.get(source).and_then(Option::as_ref) else {
                continue;
            };
            let Some(protein) = input.metadata.1.get(owner).and_then(Option::as_ref) else {
                continue;
            };
            for member in &protein.members {
                let Some(old) = member.tip_coords_from_fields(before) else {
                    continue;
                };
                let Some(new) = member.tip_coords_from_fields(after) else {
                    continue;
                };
                if old == new {
                    continue;
                }
                let entry_layout = input.schema.key.as_ref().is_some_and(|layout| {
                    layout.hash_field.as_deref() == Some(member.hash_field.as_str())
                        && layout.range_field == member.range_field
                });
                if member.molecule_uuid == *source && entry_layout {
                    continue;
                }
                let Some(trigger) = move_trigger(
                    input,
                    idx,
                    &member.hash_field,
                    member.range_field.as_deref(),
                ) else {
                    continue;
                };
                let Some(source_key) = field.disk_slot_for_key(&input.keys[idx]) else {
                    continue;
                };
                if requests.len() >= 10_000 {
                    return Err(SchemaError::InvalidData(
                        "protein move exceeds its complete key limit".into(),
                    ));
                }
                let source_slot = slot_index(slots, (source.clone(), source_key.0, source_key.1));
                let old_slot = slot_index(
                    slots,
                    (member.molecule_uuid.clone(), old.0.clone(), old.1.clone()),
                );
                let new_slot = slot_index(
                    slots,
                    (member.molecule_uuid.clone(), new.0.clone(), new.1.clone()),
                );
                let association_slots = std::iter::once(&member.hash_field)
                    .chain(member.range_field.as_ref())
                    .filter_map(|name| input.schema.runtime_fields.get(name))
                    .filter_map(|field| {
                        Some((
                            field.common().molecule_uuid()?.clone(),
                            field.disk_slot_for_key(&input.keys[idx])?,
                        ))
                    })
                    .map(|(molecule, (hash, range))| slot_index(slots, (molecule, hash, range)))
                    .collect();
                let value = input
                    .eligible
                    .iter()
                    .find(|(index, _, molecule)| *index == idx && *molecule == source.as_str())
                    .map(|(_, atom, _)| atom.uuid().to_owned());
                requests.push(MoveRequest {
                    index: idx,
                    source_molecule: source.clone(),
                    molecule: member.molecule_uuid.clone(),
                    old: pair_key(old),
                    new: pair_key(new),
                    trigger,
                    value,
                    source_slot,
                    old_slot,
                    new_slot,
                    association_slots,
                });
            }
        }
    }
    Ok(requests)
}

fn move_trigger(
    input: &ProteinMoveInput<'_>,
    idx: usize,
    hash: &str,
    range: Option<&str>,
) -> Option<SourceId> {
    input.eligible.iter().find_map(|(index, atom, molecule)| {
        if *index != idx {
            return None;
        }
        let name = input
            .schema
            .runtime_fields
            .iter()
            .find_map(|(name, field)| {
                field
                    .common()
                    .molecule_uuid()
                    .is_some_and(|uuid| uuid.as_str() == *molecule)
                    .then_some(name)
            })?;
        let changed_key = (name == hash || range == Some(name.as_str()))
            && input.fields.before[&idx].get(name) != input.fields.after[&idx].get(name);
        changed_key.then(|| (idx, (*molecule).to_owned(), atom.uuid().to_owned()))
    })
}

fn slot_index(slots: &mut BTreeMap<Slot, usize>, key: Slot) -> usize {
    let next = slots.len();
    *slots.entry(key).or_insert(next)
}

fn pair_key((hash, range): (String, String)) -> KeyValue {
    KeyValue::new(Some(hash), Some(range))
}

fn update_key(request: &MoveRequest) -> (usize, String, String, String, String) {
    (
        request.index,
        request.source_molecule.clone(),
        request.molecule.clone(),
        request.new.hash.clone().unwrap_or_default(),
        request.new.range.clone().unwrap_or_default(),
    )
}

fn update_index(updates: &[SiblingUpdate], origins: &[SiblingOrigin]) -> UpdateIndex {
    let mut index = HashMap::new();
    for (position, ((molecule, _, changed), origin)) in updates.iter().zip(origins).enumerate() {
        for key in changed {
            index.insert(
                (
                    origin.source.0,
                    origin.source.1.clone(),
                    molecule.clone(),
                    key.disk_hash().to_owned(),
                    key.disk_range().to_owned(),
                ),
                position,
            );
        }
    }
    index
}
