//! Inbound canonical schema and local key closure.

use super::*;
use crate::sync::cloud_purge::memory::{self, Admission};

pub(super) async fn preserve_metadata(
    previous: &DbOperations,
    candidate: &DbOperations,
    molecules: &BTreeSet<String>,
    admission: &Admission,
) -> SyncResult<()> {
    if molecules.is_empty() {
        return Ok(());
    }
    admit_schemas(previous, admission)?;
    let mut covered = BTreeSet::new();
    let schemas = previous
        .schemas()
        .get_all_schemas_strict()
        .await
        .map_err(error)?;
    let selected: BTreeMap<_, _> = schemas
        .into_iter()
        .filter(|(_, schema)| {
            let ids = schema
                .runtime_fields
                .iter()
                .map(|(field, value)| {
                    value.common().molecule_uuid().cloned().unwrap_or_else(|| {
                        crate::atom::deterministic_molecule_uuid(&schema.name, field)
                    })
                })
                .collect::<BTreeSet<_>>();
            covered.extend(ids.intersection(molecules).cloned());
            !ids.is_disjoint(molecules)
        })
        .collect();
    if covered != *molecules {
        return Err(error(
            "prior inbound molecules lack their canonical schemas",
        ));
    }
    let definitions = prepare_definitions(candidate, &selected, admission).await?;
    preserve_bundles(previous, candidate, molecules, admission).await?;
    candidate
        .open_namespace("schemas")
        .await
        .map_err(error)?
        .batch_put(definitions)
        .await
        .map_err(error)?;
    preserve_states(previous, candidate, &selected, admission).await?;
    Ok(())
}

async fn prepare_definitions(
    candidate: &DbOperations,
    selected: &BTreeMap<String, crate::schema::Schema>,
    admission: &Admission,
) -> SyncResult<Vec<(Vec<u8>, Vec<u8>)>> {
    let existing = candidate.open_namespace("schemas").await.map_err(error)?;
    let schema_names: Vec<_> = selected.keys().cloned().collect();
    admission.check(memory::keys_peak(
        admission,
        candidate.namespaced_store().as_ref(),
        "schemas",
        &schema_names,
    )?)?;
    let names = selected
        .keys()
        .map(|name| name.as_bytes().to_vec())
        .collect();
    let values = existing.get_many(names).await.map_err(error)?;
    if values.len() != selected.len() {
        return Err(error("inbound schema comparison is incomplete"));
    }
    let mut definitions = Vec::new();
    for ((name, schema), current) in selected.iter().zip(values) {
        if let Some(current) = current {
            let mut current: crate::schema::Schema =
                serde_json::from_slice(&current).map_err(error)?;
            current.populate_runtime_fields().map_err(error)?;
            if serde_json::to_value(current).map_err(error)?
                != serde_json::to_value(schema).map_err(error)?
            {
                return Err(error(
                    "prior inbound schema conflicts with the adopted personal definition",
                ));
            }
        } else {
            definitions.push((
                name.as_bytes().to_vec(),
                serde_json::to_vec(schema).map_err(error)?,
            ));
        }
    }
    Ok(definitions)
}

async fn preserve_bundles(
    previous: &DbOperations,
    candidate: &DbOperations,
    molecules: &BTreeSet<String>,
    admission: &Admission,
) -> SyncResult<()> {
    let source_keys = previous
        .open_namespace("molecule_keys")
        .await
        .map_err(error)?;
    let bundle_keys: Vec<_> = molecules
        .iter()
        .flat_map(|id| {
            [
                crate::db_operations::molecule_key_store::metadata_key(id),
                crate::db_operations::molecule_key_store::wrap_key_for(id, "node"),
            ]
        })
        .collect();
    memory::reserve_pair(
        admission,
        previous.namespaced_store().as_ref(),
        candidate.namespaced_store().as_ref(),
        "molecule_keys",
        &bundle_keys,
    )?;
    candidate
        .molecule_keys()
        .preserve_local_node_bundles(source_keys.as_ref(), molecules)
        .await
        .map_err(error)?;
    Ok(())
}

async fn preserve_states(
    previous: &DbOperations,
    candidate: &DbOperations,
    selected: &BTreeMap<String, crate::schema::Schema>,
    admission: &Admission,
) -> SyncResult<()> {
    memory::reserve_pair(
        admission,
        previous.namespaced_store().as_ref(),
        candidate.namespaced_store().as_ref(),
        "schema_states",
        &selected.keys().cloned().collect::<Vec<_>>(),
    )?;
    let state_keys = selected
        .keys()
        .map(|name| name.as_bytes().to_vec())
        .collect::<Vec<_>>();
    let source_states = previous
        .open_namespace("schema_states")
        .await
        .map_err(error)?;
    let states = candidate
        .open_namespace("schema_states")
        .await
        .map_err(error)?;
    let (old, current) = futures::join!(
        source_states.get_many(state_keys.clone()),
        states.get_many(state_keys.clone())
    );
    let (old, current) = (old.map_err(error)?, current.map_err(error)?);
    let additions = state_keys
        .into_iter()
        .zip(old)
        .zip(current)
        .filter_map(|((key, old), current)| {
            current
                .is_none()
                .then(|| old.map(|value| (key, value)))
                .flatten()
        })
        .collect();
    states.batch_put(additions).await.map_err(error)?;
    Ok(())
}

fn admit_schemas(previous: &DbOperations, admission: &Admission) -> SyncResult<()> {
    let store = previous.namespaced_store();
    let raw = store
        .raw_last_store()
        .ok_or_else(|| error("inbound schema preservation requires LastStore"))?;
    let keys = crate::sync::cloud_purge::org_rows::physical_keys(&raw, "schemas", admission)?
        .into_keys()
        .collect::<Vec<_>>();
    admission.check(memory::keys_peak(
        admission,
        store.as_ref(),
        "schemas",
        &keys,
    )?)
}
