//! Exact old and optional new protein coordinates for a source change.

use super::*;

pub(super) fn move_requests(
    input: &ProteinMoveInput<'_>,
    schemas: &HashMap<String, Schema>,
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
                let old = member.tip_coords_from_fields(before);
                let new = member.tip_coords_from_fields(after);
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
                let Some(source_key) = field.slot_coordinates(&input.keys[idx]) else {
                    continue;
                };
                if requests.len() >= 10_000 {
                    return Err(SchemaError::InvalidData(
                        "protein move exceeds its complete key limit".into(),
                    ));
                }
                let source_slot = slot_index(slots, (source.clone(), source_key.0, source_key.1));
                let ownership_pairs = old.as_ref().map_or_else(Vec::new, |old| {
                    ownership_pairs(input, member, old, idx, slots)
                });
                let identity_slots = identity_slots(input, member, schemas, idx, slots);
                let old = old.map(|old| {
                    let slot = slot_index(
                        slots,
                        (member.molecule_uuid.clone(), old.0.clone(), old.1.clone()),
                    );
                    (pair_key(old), slot)
                });
                let new = new.map(|new| {
                    let slot = slot_index(
                        slots,
                        (member.molecule_uuid.clone(), new.0.clone(), new.1.clone()),
                    );
                    (pair_key(new), slot)
                });
                let association_slots = if old.is_some() {
                    association_slots(input, member, idx, slots)
                } else {
                    Vec::new()
                };
                let value = input
                    .eligible
                    .iter()
                    .find(|(index, _, molecule)| *index == idx && *molecule == source.as_str())
                    .map(|(_, atom, _)| atom.uuid().to_owned());
                requests.push(MoveRequest {
                    index: idx,
                    source_molecule: source.clone(),
                    molecule: member.molecule_uuid.clone(),
                    old,
                    new,
                    trigger,
                    value,
                    source_slot,
                    association_slots,
                    ownership_pairs,
                    identity_slots,
                });
            }
        }
    }
    Ok(requests)
}

fn identity_slots(
    input: &ProteinMoveInput<'_>,
    member: &crate::protein::ProteinMember,
    schemas: &HashMap<String, Schema>,
    idx: usize,
    slots: &mut BTreeMap<Slot, usize>,
) -> Vec<usize> {
    let identity = schemas.values().find_map(|peer| {
        let key = peer.key.as_ref()?;
        if key.hash_field.as_deref() != Some(member.hash_field.as_str())
            || key.range_field != member.range_field
            || !peer
                .runtime_fields
                .values()
                .any(|field| field.common().molecule_uuid() == Some(&member.molecule_uuid))
        {
            return None;
        }
        source_identity_molecules(input.schema, peer)
    });
    identity
        .unwrap_or_default()
        .into_iter()
        .map(|molecule| {
            slot_index(
                slots,
                (
                    molecule,
                    input.keys[idx].hash.clone().unwrap_or_default(),
                    input.keys[idx].range.clone().unwrap_or_default(),
                ),
            )
        })
        .collect()
}

fn ownership_pairs(
    input: &ProteinMoveInput<'_>,
    member: &crate::protein::ProteinMember,
    old: &(String, String),
    idx: usize,
    slots: &mut BTreeMap<Slot, usize>,
) -> Vec<(usize, usize)> {
    let mut pairs = Vec::new();
    for field in input.schema.runtime_fields.values() {
        let Some(source) = field.common().molecule_uuid() else {
            continue;
        };
        let Some(protein) = input
            .metadata
            .0
            .get(source)
            .and_then(Option::as_ref)
            .and_then(|owner| input.metadata.1.get(owner))
            .and_then(Option::as_ref)
        else {
            continue;
        };
        let Some((hash, range)) = field.slot_coordinates(&input.keys[idx]) else {
            continue;
        };
        let source_slot = slot_index(slots, (source.clone(), hash, range));
        for peer in protein.members.iter().filter(|peer| {
            peer.hash_field == member.hash_field && peer.range_field == member.range_field
        }) {
            let peer_slot = slot_index(
                slots,
                (peer.molecule_uuid.clone(), old.0.clone(), old.1.clone()),
            );
            pairs.push((source_slot, peer_slot));
        }
    }
    pairs
}

fn association_slots(
    input: &ProteinMoveInput<'_>,
    member: &crate::protein::ProteinMember,
    idx: usize,
    slots: &mut BTreeMap<Slot, usize>,
) -> Vec<usize> {
    std::iter::once(&member.hash_field)
        .chain(member.range_field.as_ref())
        .filter_map(|name| input.schema.runtime_fields.get(name))
        .filter_map(|field| {
            Some((
                field.common().molecule_uuid()?.clone(),
                field.slot_coordinates(&input.keys[idx])?,
            ))
        })
        .map(|(molecule, (hash, range))| slot_index(slots, (molecule, hash, range)))
        .collect()
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
