//! Copy current tip references, author clocks and metadata without rewriting atoms.

use super::*;
use crate::atom::molecule_key_codec;
use crate::db_operations::atom_store::{
    counter::{TipChange, TipChangeSet},
    PerKeyRecord,
};
use crate::schema::types::{key_value::scalar_key_coordinate, KeyValue};

pub(super) async fn keys(
    db: &FoldDB,
    target: &Target,
) -> Result<(Vec<KeyValue>, Option<String>), SchemaError> {
    let field = target
        .source
        .runtime_fields
        .get(primary(&target.source)?)
        .ok_or_else(|| invalid("source primary field is absent"))?;
    let molecule = field
        .common()
        .molecule_uuid()
        .ok_or_else(|| invalid("source primary molecule is absent"))?;
    let atoms = db.db_ops.atoms();
    let prefix = format!("mk:{molecule}:");
    let (mut start, end) = crate::kind_partition::exact_prefix_bounds(&prefix);
    if let Some(after) = &target.after {
        if !after.starts_with(&prefix) {
            return Err(invalid("source key cursor has another molecule"));
        }
        start = format!("{after}\0");
    }
    let limit = (1024 / target.source.runtime_fields.len().max(1)).min(32);
    let rows = atoms
        .raw()
        .inner()
        .scan_range_paged(start.as_bytes(), end.as_bytes(), limit)
        .await?;
    let records = rows
        .iter()
        .map(|(_, value)| {
            serde_json::from_slice::<PerKeyRecord>(value)
                .map_err(|_| invalid("source primary tip is unreadable"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let ids: Vec<_> = records
        .iter()
        .map(|record| record.entry.atom_uuid.as_str())
        .collect();
    let values = atoms.get_atoms(&ids, None).await?;
    if values.len() != rows.len() {
        return Err(invalid("source primary atom batch is incomplete"));
    }
    let codec = atoms.key_codec_for_molecule(molecule)?;
    let mut keys = Vec::with_capacity(rows.len());
    let mut after = None;
    for ((stored, _), value) in rows.into_iter().zip(values) {
        let stored = String::from_utf8(stored).map_err(|_| invalid("source key is not UTF-8"))?;
        let atom = value.ok_or(SchemaError::MissingSelectedAtom)?;
        let hash = scalar_key_coordinate(atom.content())
            .ok_or_else(|| invalid("source primary key is not scalar"))?
            .into_owned();
        let (_, encoded_range) = molecule_key_codec::decode_hash_range(&stored, molecule)
            .ok_or_else(|| invalid("source key cannot be decoded"))?;
        let range = codec
            .api_range(&encoded_range)
            .map_err(|_| invalid("source range cannot be decoded"))?;
        if codec
            .api_hash_range_record_key(molecule, &hash, &range)
            .map_err(|_| invalid("source coordinates are invalid"))?
            != stored
        {
            return Err(invalid("source primary field differs from its stored key"));
        }
        let ranged = target
            .source
            .key
            .as_ref()
            .is_some_and(|key| key.range_field.is_some());
        keys.push(KeyValue::new(Some(hash), ranged.then_some(range)));
        after = Some(stored);
    }
    Ok((keys, after))
}

pub(super) async fn copy(
    db: &FoldDB,
    store: &dyn KvStore,
    id: &str,
    target: &mut Target,
    keys: &[KeyValue],
) -> Result<(), SchemaError> {
    let atoms = db.db_ops.atoms();
    let selected = membership::mask(db, target, keys).await?;
    let mut sources = Vec::new();
    let mut destinations = Vec::new();
    for (ordinal, key) in keys.iter().enumerate() {
        for (name, source) in &target.source.runtime_fields {
            let destination = target
                .target
                .runtime_fields
                .get(name)
                .ok_or_else(|| invalid("target field is absent"))?;
            sources.push(slot(atoms, source, key)?);
            destinations.push((
                name.clone(),
                slot(atoms, destination, key)?,
                selected[ordinal],
            ));
        }
    }
    if sources.len() > 4096 {
        return Err(invalid("conversion page exceeds 4096 field slots"));
    }
    let records = atoms.load_visible_per_key_records(None, &sources).await?;
    if records.len() != destinations.len() {
        return Err(invalid("source tip batch is incomplete"));
    }
    let mut tips = Vec::new();
    let mut values = Vec::new();
    for ((field, key, selected), record) in destinations.into_iter().zip(records) {
        if let Some(record) = record {
            let identity = target
                .source
                .field_hashes
                .get(&field)
                .ok_or_else(|| invalid("source field identity is absent"))?;
            values.push((identity.clone(), record.entry.atom_uuid.clone(), selected));
            *target.observed.entry(field).or_default() += 1;
            if selected {
                tips.push(TipChange {
                    key,
                    new: Some(record),
                });
            }
        }
    }
    audit::observe(store, id, values).await?;
    let kept: Vec<_> = keys
        .iter()
        .zip(&selected)
        .filter(|(_, selected)| **selected)
        .map(|(key, _)| key.clone())
        .collect();
    target.selected += kept.len() as u64;
    atoms
        .commit_tip_changes(TipChangeSet {
            tips,
            ..TipChangeSet::default()
        })
        .await?;
    atoms
        .stage_alternate_changes(
            &target.target,
            &kept,
            &vec![BTreeMap::new(); kept.len()],
            None,
        )
        .await?;
    Ok(())
}

pub(super) fn slot(
    atoms: &crate::db_operations::atom_store::AtomStore,
    field: &crate::schema::types::field::FieldVariant,
    key: &KeyValue,
) -> Result<String, SchemaError> {
    let molecule = field
        .common()
        .molecule_uuid()
        .ok_or_else(|| invalid("conversion field has no molecule"))?;
    let (hash, range) = field
        .slot_coordinates(key)
        .ok_or_else(|| invalid("conversion key shape differs"))?;
    atoms
        .key_codec_for_molecule(molecule)?
        .api_hash_range_record_key(molecule, &hash, &range)
        .map_err(|_| invalid("conversion key has an invalid type"))
}
