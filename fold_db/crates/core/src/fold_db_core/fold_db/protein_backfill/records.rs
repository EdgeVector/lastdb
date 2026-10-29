//! Exact source reads and counter-safe target tip candidates, with no value rewrite.

use super::plan::{key_names, molecule, Plan};
use super::{invalid, FoldDB, KeyValue, SchemaError};
use crate::atom::MoleculeHashRange;
use crate::db_operations::atom_store::{ChangedKey, MoleculeData, MoleculeHeader, PerKeyRecord};
use crate::schema::types::key_value::scalar_key_coordinate;
use chrono::Utc;
use std::collections::{BTreeSet, HashMap, HashSet};

type Update = (String, MoleculeData, HashSet<ChangedKey>);
type Values = HashMap<(usize, String), String>;

struct SourceRows {
    rows: Vec<Option<PerKeyRecord>>,
    names: Vec<String>,
}

impl SourceRows {
    fn get(&self, index: usize, name: &str) -> Result<Option<&PerKeyRecord>, SchemaError> {
        let column = self
            .names
            .binary_search_by(|candidate| candidate.as_str().cmp(name))
            .map_err(|_| invalid("protein backfill source field was not selected"))?;
        Ok(self.rows[index * self.names.len() + column]
            .as_ref()
            .filter(|record| record.meta.as_ref().is_none_or(|meta| !meta.tombstoned)))
    }
}

pub(super) async fn prepare(db: &FoldDB, plan: &Plan) -> Result<Vec<Update>, SchemaError> {
    let source = load_sources(db, plan).await?;
    let values = routing_values(db, plan, &source).await?;
    let molecules: BTreeSet<_> = plan
        .fields
        .iter()
        .map(|field| field.target_molecule.clone())
        .collect();
    let molecules: Vec<_> = molecules.into_iter().collect();
    let keys: Vec<_> = molecules
        .iter()
        .map(|id| crate::atom::molecule_key_codec::header_key(id))
        .collect();
    let headers = db
        .db_ops()
        .atoms()
        .raw()
        .get_items::<MoleculeHeader>(&keys)
        .await?;
    if headers.len() != keys.len() {
        return Err(invalid(
            "protein backfill target header batch is incomplete",
        ));
    }
    let headers: HashMap<_, _> = molecules.into_iter().zip(headers).collect();
    let mut updates = Vec::new();
    let mut offered_slots = HashSet::new();
    for (index, source_key) in plan.keys.iter().enumerate() {
        let target_key = target_coordinates(db, plan, &values, index, source_key)?;
        let hash = target_key.hash.expect("validated target hash");
        let range = target_key.range.unwrap_or_default();
        for field in &plan.fields {
            let Some(record) = source.get(index, &field.source_field)? else {
                continue;
            };
            if !offered_slots.insert((field.target_molecule.clone(), hash.clone(), range.clone())) {
                return Err(invalid(
                    "protein backfill source keys derive an ambiguous target slot",
                ));
            }
            let version = headers
                .get(&field.target_molecule)
                .and_then(Option::as_ref)
                .map_or(0, |header| header.version);
            // Carry the entire original tip, including the signed author identity
            // and per-key metadata. The store's existing LWW/barrier planner is
            // the authority; this operation creates no atom or mutation clock.
            let data = MoleculeHashRange::from_write_records(
                field.target_molecule.clone(),
                version.saturating_add(1),
                Utc::now(),
                vec![(
                    hash.clone(),
                    range.clone(),
                    record.entry.clone(),
                    record.meta.clone(),
                )],
            );
            updates.push((
                field.target_molecule.clone(),
                data,
                HashSet::from([ChangedKey::hash_range(hash.clone(), range.clone())]),
            ));
        }
    }
    Ok(updates)
}

async fn load_sources(db: &FoldDB, plan: &Plan) -> Result<SourceRows, SchemaError> {
    let atoms = db.db_ops().atoms();
    let mut keys = Vec::new();
    for key in &plan.keys {
        for name in &plan.source_names {
            let field = plan
                .source
                .runtime_fields
                .get(name)
                .ok_or_else(|| invalid("protein backfill source field is absent"))?;
            let (hash, range) = field
                .slot_coordinates(key)
                .ok_or_else(|| invalid("protein backfill source field cannot address this key"))?;
            keys.push(
                atoms
                    .key_codec_for_molecule(&molecule(&plan.source, name)?)?
                    .api_hash_range_record_key(&molecule(&plan.source, name)?, &hash, &range)
                    .map_err(|error| invalid(&error.to_string()))?,
            );
        }
    }
    let rows = atoms.load_visible_per_key_records(None, &keys).await?;
    if rows.len() != keys.len() {
        return Err(invalid("protein backfill source tip batch is incomplete"));
    }
    Ok(SourceRows {
        rows,
        names: plan.source_names.clone(),
    })
}

async fn routing_values(
    db: &FoldDB,
    plan: &Plan,
    rows: &SourceRows,
) -> Result<Values, SchemaError> {
    let mut required: BTreeSet<_> = key_names(&plan.source)
        .into_iter()
        .map(str::to_owned)
        .collect();
    for target_name in key_names(&plan.target) {
        required.insert(
            plan.fields
                .iter()
                .find(|field| field.target_field == target_name)
                .ok_or_else(|| invalid("protein backfill target key identity is absent"))?
                .source_field
                .clone(),
        );
    }
    let mut selected = Vec::new();
    for index in 0..plan.keys.len() {
        for name in &required {
            let record = rows.get(index, name)?.ok_or_else(|| {
                invalid("protein backfill source key is absent or lacks a required key field")
            })?;
            selected.push(((index, name.clone()), record.entry.atom_uuid.as_str()));
        }
    }
    let ids: Vec<_> = selected.iter().map(|(_, id)| *id).collect();
    let atoms = db.db_ops().atoms().get_atoms(&ids, None).await?;
    if atoms.len() != ids.len() {
        return Err(invalid("protein backfill routing atom batch is incomplete"));
    }
    let mut values = HashMap::new();
    let mut bytes = 0usize;
    for (((index, name), _), atom) in selected.into_iter().zip(atoms) {
        let atom =
            atom.ok_or_else(|| invalid("protein backfill source key field has no atom body"))?;
        let value = scalar_key_coordinate(atom.content())
            .ok_or_else(|| invalid("protein backfill key field is not scalar"))?
            .into_owned();
        bytes = bytes.saturating_add(value.len());
        if bytes > 1024 * 1024 {
            return Err(invalid("protein backfill routing values exceed one MiB"));
        }
        values.insert((index, name), value);
    }
    Ok(values)
}

fn target_coordinates(
    db: &FoldDB,
    plan: &Plan,
    values: &Values,
    index: usize,
    supplied: &KeyValue,
) -> Result<KeyValue, SchemaError> {
    let coordinate = |name: &str| {
        values
            .get(&(index, name.to_owned()))
            .cloned()
            .ok_or_else(|| invalid("protein backfill required source coordinate is absent"))
    };
    let source_layout = plan.source.key.as_ref().expect("validated source layout");
    let source = KeyValue::new(
        Some(coordinate(
            source_layout.hash_field.as_deref().expect("hash"),
        )?),
        source_layout
            .range_field
            .as_deref()
            .map(coordinate)
            .transpose()?,
    );
    if db
        .db_ops()
        .molecule_keys()
        .canonical_key(&plan.source, source)?
        != *supplied
    {
        return Err(invalid(
            "protein backfill source record belongs to another declared key layout",
        ));
    }
    let target_layout = plan.target.key.as_ref().expect("validated target layout");
    let target = |name: &str| {
        let field = plan
            .fields
            .iter()
            .find(|field| field.target_field == name)
            .ok_or_else(|| invalid("protein backfill target key is not shared"))?;
        coordinate(&field.source_field)
    };
    db.db_ops().molecule_keys().canonical_key(
        &plan.target,
        KeyValue::new(
            Some(target(target_layout.hash_field.as_deref().expect("hash"))?),
            target_layout
                .range_field
                .as_deref()
                .map(target)
                .transpose()?,
        ),
    )
}
