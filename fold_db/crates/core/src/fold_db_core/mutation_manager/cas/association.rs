//! Current protein association proof for a move or a derived erasure.

use super::*;
use crate::db_operations::atom_store::PerKeyRecord;

type Slot = (String, String, String);

struct PeerProof {
    mutation: Mutation,
    pairs: Vec<(usize, usize)>,
    key_slots: Vec<usize>,
    identity_slots: Vec<usize>,
}

/// Record cleanup needs the source identity in the peer, not just equal
/// shared payload fields. Field names alone cannot establish that identity.
pub(in crate::fold_db_core::mutation_manager) fn source_identity_molecules(
    source: &Schema,
    peer: &Schema,
) -> Option<Vec<String>> {
    let key = source.key.as_ref()?;
    let mut molecules = Vec::new();
    for name in key.hash_field.iter().chain(key.range_field.iter()) {
        let identity = source
            .field_hashes
            .get(name)
            .filter(|hash| !hash.is_empty())?;
        if peer.field_hashes.get(name) != Some(identity) || !peer.runtime_fields.contains_key(name)
        {
            return None;
        }
        molecules.push(
            source
                .runtime_fields
                .get(name)?
                .common()
                .molecule_uuid()?
                .clone(),
        );
    }
    (!molecules.is_empty()).then_some(molecules)
}

/// A shared value belongs to this source only when its metadata and source
/// clock agree. An unchanged field may carry the clock of the key change
/// that copied it into this member. Equal atom identities alone prove nothing.
pub(in crate::fold_db_core::mutation_manager) fn same_association<'a>(
    peer: &PerKeyRecord,
    source: &PerKeyRecord,
    key_sources: impl Iterator<Item = &'a PerKeyRecord>,
) -> bool {
    !(peer.entry.logical_counter == 0 && peer.entry.mutation_uuid.is_empty())
        && peer.entry.atom_uuid == source.entry.atom_uuid
        && peer.meta == source.meta
        && (peer.entry.lww_key() == source.entry.lww_key()
            || key_sources.into_iter().any(|key| {
                let key = &key.entry;
                let peer = &peer.entry;
                key.written_at == peer.written_at
                    && key.logical_counter == peer.logical_counter
                    && key.lww_device() == peer.lww_device()
                    && key.mutation_uuid == peer.mutation_uuid
            }))
}

pub(in crate::fold_db_core::mutation_manager) fn current_association(
    records: &[Option<PerKeyRecord>],
    pairs: &[(usize, usize)],
    key_slots: &[usize],
    identity_slots: &[usize],
) -> bool {
    if identity_slots.is_empty()
        || !identity_slots.iter().all(|identity| {
            pairs
                .iter()
                .any(|(source, peer)| source == identity && records[*peer].is_some())
        })
    {
        return false;
    }
    let mut present = false;
    for &(source, peer) in pairs {
        let Some(peer) = records[peer].as_ref() else {
            continue;
        };
        present = true;
        let Some(source) = records[source].as_ref() else {
            return false;
        };
        if !same_association(
            peer,
            source,
            key_slots.iter().filter_map(|&slot| records[slot].as_ref()),
        ) {
            return false;
        }
    }
    present
}

impl MutationManager {
    /// Every candidate shares a protein with the source. Read all selected
    /// tips together, then retain only targets still owned by that source.
    pub(in crate::fold_db_core::mutation_manager) async fn confirmed_peer_erasures(
        &self,
        source: &Mutation,
        candidates: Vec<Mutation>,
        storage_prefix: Option<&str>,
    ) -> Result<Vec<Mutation>, SchemaError> {
        if candidates.is_empty() {
            return Ok(candidates);
        }
        // Peers can be cold after restart. Admit their existing typed profiles
        // before field_slots/read logic asks the codec for those molecules.
        self.prepare_batch_molecule_keys(&candidates).await?;
        let source_schema = self
            .schema_manager
            .get_schema_metadata(&source.schema_name)?
            .ok_or_else(|| {
                SchemaError::InvalidData("protein erasure source schema is absent".into())
            })?;
        if !self
            .source_coordinates_match(
                &source_schema,
                std::slice::from_ref(&source.key_value),
                storage_prefix,
            )
            .await?[0]
        {
            return Ok(Vec::new());
        }
        let proteins = self.proteins_of_schema_molecules(&source_schema).await?;
        let mut slots = BTreeMap::new();
        let source_slots = field_slots(&source_schema, &source.key_value, &mut slots);
        let mut proofs = Vec::new();
        for mutation in candidates {
            let peer = self
                .schema_manager
                .get_schema_metadata(&mutation.schema_name)?
                .ok_or_else(|| {
                    SchemaError::InvalidData("protein erasure peer schema is absent".into())
                })?;
            let Some(identity) = source_identity_molecules(&source_schema, &peer) else {
                continue;
            };
            let identity_slots = identity
                .iter()
                .filter_map(|molecule| source_slots.get(molecule).copied())
                .collect();
            let peer_slots = field_slots(&peer, &mutation.key_value, &mut slots);
            let pairs = shared_pairs(&source_slots, &peer_slots, &proteins);
            let key_slots = peer
                .key
                .as_ref()
                .into_iter()
                .flat_map(|key| key.hash_field.iter().chain(key.range_field.iter()))
                .filter_map(|name| source_schema.runtime_fields.get(name))
                .filter_map(|field| field.common().molecule_uuid())
                .filter_map(|molecule| source_slots.get(molecule).copied())
                .collect();
            proofs.push(PeerProof {
                mutation,
                pairs,
                key_slots,
                identity_slots,
            });
        }
        let atoms = self.db_ops.atoms();
        let contracts = slots
            .keys()
            .map(|(molecule, _, _)| molecule.clone())
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        atoms.load_molecule_key_bundles(&contracts).await?;
        let mut slot_keys = vec![String::new(); slots.len()];
        for ((molecule, hash, range), position) in slots {
            slot_keys[position] = atoms
                .key_codec_for_molecule(&molecule)?
                .api_hash_range_record_key(&molecule, &hash, &range)
                .map_err(|error| SchemaError::InvalidData(error.to_string()))?;
        }
        let records = atoms
            .load_visible_per_key_records(storage_prefix, &slot_keys)
            .await?;
        if records.len() != slot_keys.len() {
            return Err(SchemaError::StorageRead {
                detail: "protein erasure association count differs".into(),
            });
        }
        Ok(proofs
            .into_iter()
            .filter_map(|proof| {
                current_association(
                    &records,
                    &proof.pairs,
                    &proof.key_slots,
                    &proof.identity_slots,
                )
                .then_some(proof.mutation)
            })
            .collect())
    }
}

fn field_slots(
    schema: &Schema,
    key: &KeyValue,
    slots: &mut BTreeMap<Slot, usize>,
) -> HashMap<String, usize> {
    schema
        .runtime_fields
        .values()
        .filter_map(|field| {
            let molecule = field.common().molecule_uuid()?;
            let (hash, range) = field.slot_coordinates(key)?;
            let next = slots.len();
            let slot = *slots.entry((molecule.clone(), hash, range)).or_insert(next);
            Some((molecule.clone(), slot))
        })
        .collect()
}

fn shared_pairs(
    source: &HashMap<String, usize>,
    peer: &HashMap<String, usize>,
    proteins: &[crate::protein::Protein],
) -> Vec<(usize, usize)> {
    proteins
        .iter()
        .flat_map(|protein| {
            let source = protein
                .members
                .iter()
                .find_map(|member| source.get(&member.molecule_uuid).copied());
            protein
                .members
                .iter()
                .filter_map(move |member| Some((source?, *peer.get(&member.molecule_uuid)?)))
        })
        .collect()
}
