//! A new protein key retains values and retires only a captured old association.

use super::super::cas::{current_association, source_identity_molecules};
use super::super::MutationManager;

mod requests;
use super::prepared::{SiblingMove, SiblingOrigin, SiblingUpdate, SourceId};
use super::protein_batch::{schema_storage_prefix, ProteinFoldMetadata};
use super::protein_key_fields::ProteinKeyFields;
use crate::atom::{molecule_key_codec, Atom, MoleculeHashRange};
use crate::db_operations::atom_store::{ChangedKey, MoleculeHeader, PerKeyRecord};
use crate::schema::types::field::build_storage_key;
use crate::schema::types::{KeyValue, Schema};
use crate::schema::SchemaError;
use chrono::Utc;
use requests::move_requests;
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
    old: Option<(KeyValue, usize)>,
    new: Option<(KeyValue, usize)>,
    trigger: SourceId,
    value: Option<String>,
    source_slot: usize,
    association_slots: Vec<usize>,
    ownership_pairs: Vec<(usize, usize)>,
    identity_slots: Vec<usize>,
}

type Slot = (String, String, String);
type UpdateIndex = HashMap<(usize, String, String, String, String), usize>;

struct MoveState {
    records: Vec<Option<PerKeyRecord>>,
    headers: HashMap<String, MoleculeHeader>,
    valid_sources: HashSet<usize>,
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
        if input
            .fields
            .after
            .iter()
            .all(|(index, after)| input.fields.before.get(index) == Some(after))
        {
            return Ok(Vec::new());
        }
        let mut slots = BTreeMap::new();
        let schemas = self.schema_manager.get_schemas()?;
        let requests = move_requests(input, &schemas, &mut slots)?;
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
            if request.new.is_some() {
                self.ensure_moved_value(&request, source, &state, updates, origins, &mut index);
            }
            // A previously absent peer key still needs the unchanged source
            // values. Only a captured old association can also be retired.
            let Some((old_key, old_slot)) = request.old.as_ref() else {
                continue;
            };
            let Some(old) = state.records[*old_slot].as_ref() else {
                continue;
            };
            // An equal body alone does not establish which record owns a
            // colliding destination. Require the old source association too.
            if !current_association(
                &state.records,
                &request.ownership_pairs,
                &request.association_slots,
                &request.identity_slots,
            ) || !state.valid_sources.contains(&request.index)
            {
                continue;
            }
            let mut header = state
                .headers
                .get(&request.molecule)
                .cloned()
                .ok_or_else(|| SchemaError::StorageRead {
                    detail: "protein old association has no molecule header".into(),
                })?;
            header.version = header.version.saturating_add(1);
            header.updated_at = Utc::now();
            movements.push(SiblingMove {
                source: request.trigger,
                molecule_uuid: request.molecule,
                old_key: old_key.clone(),
                header,
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
        let contracts: Vec<_> = slots
            .keys()
            .map(|(molecule, _, _)| molecule)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .cloned()
            .collect();
        atoms.load_molecule_key_bundles(&contracts).await?;
        let mut slot_keys = vec![String::new(); slots.len()];
        for ((molecule, hash, range), position) in slots {
            slot_keys[position] = atoms
                .key_codec_for_molecule(&molecule)?
                .api_hash_range_record_key(&molecule, &hash, &range)
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
        let indices: Vec<_> = requests
            .iter()
            .map(|request| request.index)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let keys: Vec<_> = indices
            .iter()
            .map(|&index| input.keys[index].clone())
            .collect();
        let (records, headers, valid) = tokio::try_join!(
            atoms.load_visible_per_key_records(prefix, &slot_keys),
            headers,
            self.source_coordinates_match(input.schema, &keys, prefix)
        )?;
        if records.len() != slot_keys.len() || headers.len() != molecules.len() {
            return Err(SchemaError::StorageRead {
                detail: "protein move winner or header count differs".into(),
            });
        }
        Ok(MoveState {
            records,
            valid_sources: indices
                .into_iter()
                .zip(valid)
                .filter_map(|(index, valid)| valid.then_some(index))
                .collect(),
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
        let (new, new_slot) = request.new.as_ref().expect("protein destination");
        let key = update_key(request, new);
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
        let hash = new.hash.clone().unwrap_or_default();
        let range = new.range.clone().unwrap_or_default();
        let destination = state.records[*new_slot].as_ref();
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

fn update_key(request: &MoveRequest, new: &KeyValue) -> (usize, String, String, String, String) {
    (
        request.index,
        request.source_molecule.clone(),
        request.molecule.clone(),
        new.hash.clone().unwrap_or_default(),
        new.range.clone().unwrap_or_default(),
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
                    key.hash_or_empty().to_owned(),
                    key.range_or_empty().to_owned(),
                ),
                position,
            );
        }
    }
    index
}
