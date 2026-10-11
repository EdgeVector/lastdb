//! Retain only canonical protein members included in the isolated org source.

use super::org_rows::error;
use crate::protein::{member_backref_key, protein_record_key, Protein};
use crate::storage::KvStore;
use crate::sync::SyncResult;
use std::collections::{BTreeMap, BTreeSet};

pub(super) async fn scoped_protein_rows(
    main: &dyn KvStore,
    physical: &BTreeMap<String, Vec<u8>>,
    instance: &str,
    molecules: &BTreeSet<String>,
) -> SyncResult<BTreeMap<String, serde_json::Value>> {
    let keys: Vec<_> = molecules
        .iter()
        .map(|molecule| {
            let base = member_backref_key(molecule);
            let scoped = if instance == crate::db_operations::UNPREFIXED_INSTANCE_ID {
                base.clone()
            } else {
                format!("{instance}:{base}")
            };
            // Canonical binding metadata is global in legacy stores. This copies
            // definitions only; all tip/atom reads remain in the exact source instance.
            if physical.contains_key(&scoped) {
                scoped
            } else {
                base
            }
        })
        .collect();
    let values = main
        .get_many(keys.iter().map(|key| key.as_bytes().to_vec()).collect())
        .await
        .map_err(error)?;
    let mut owners = BTreeMap::new();
    for (molecule, value) in molecules.iter().zip(values) {
        if let Some(value) = value {
            let id: String = serde_json::from_slice(&value).map_err(error)?;
            owners.insert(molecule.clone(), id);
        }
    }
    let ids: BTreeSet<_> = owners.values().cloned().collect();
    let keys = ids
        .iter()
        .map(|id| {
            let base = protein_record_key(id);
            let scoped = format!("{instance}:{base}");
            if physical.contains_key(&scoped) {
                scoped.into_bytes()
            } else {
                base.into_bytes()
            }
        })
        .collect();
    let values = main.get_many(keys).await.map_err(error)?;
    let mut output = BTreeMap::new();
    for (id, value) in ids.iter().zip(values) {
        let mut protein: Protein =
            serde_json::from_slice(&value.ok_or_else(|| error("org protein definition absent"))?)
                .map_err(error)?;
        if protein.uuid != *id {
            return Err(error("org protein identity differs"));
        }
        protein
            .members
            .retain(|member| molecules.contains(&member.molecule_uuid));
        if protein.members.is_empty() {
            return Err(error("org protein has no selected members"));
        }
        for member in &protein.members {
            if owners.get(&member.molecule_uuid) != Some(id) {
                return Err(error("org protein membership is inconsistent"));
            }
        }
        output.insert(
            protein_record_key(id),
            serde_json::to_value(protein).map_err(error)?,
        );
    }
    for (molecule, id) in owners {
        output.insert(member_backref_key(&molecule), id.into());
    }
    Ok(output)
}
