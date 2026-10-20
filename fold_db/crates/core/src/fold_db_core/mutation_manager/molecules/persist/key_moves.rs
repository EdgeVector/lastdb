//! Retire only the old association of an accepted protein key move.

use super::*;
use crate::atom::{molecule_key_codec, MoleculeHashRange};
use crate::fold_db_core::mutation_manager::molecules::SiblingMove;
use std::collections::BTreeMap;

type RemovedMolecules = BTreeMap<String, (MoleculeData, Vec<(String, String)>)>;

impl MutationManager {
    /// The native caller owns the complete write gate across comparison and puts.
    /// Removal changes tip references only; shared Atom bodies remain untouched.
    pub(super) async fn persist_sibling_key_moves(
        &self,
        job: &DeferredPersistJob,
    ) -> Result<(), SchemaError> {
        if job.sibling_moves.is_empty() {
            return Ok(());
        }
        let Some(runtime) = self.db_ops.namespaced_store().disk_pack_runtime() else {
            return Ok(());
        };
        if !crate::storage::disk_pack::transaction::publication::active(Some(runtime)) {
            return Err(SchemaError::InvalidData(
                "protein key moves require a complete native write scope".into(),
            ));
        }
        if job.sibling_moves.len() > 10_000 {
            return Err(SchemaError::InvalidData(
                "protein key move exceeds 10000 slots".into(),
            ));
        }
        if job.sibling_moves.iter().any(|movement| {
            movement.expected_old.entry.logical_counter == 0
                && movement.expected_old.entry.mutation_uuid.is_empty()
        }) {
            return Err(SchemaError::InvalidData(
                "protein key move cannot certify an old association without an author clock or mutation identity".into(),
            ));
        }
        let atoms = self.db_ops.atoms();
        let groups = job
            .sibling_moves
            .iter()
            .map(|movement| {
                atoms
                    .key_codec_for_molecule(&movement.molecule_uuid)?
                    .api_hash_range_record_keys_for_read(
                        &movement.molecule_uuid,
                        movement.old_key.hash.as_deref().unwrap_or_default(),
                        movement.old_key.range.as_deref().unwrap_or_default(),
                    )
                    .map_err(|error| SchemaError::InvalidData(error.to_string()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let current = atoms
            .load_first_present_candidate(job.storage_prefix.as_deref(), &groups)
            .await?;
        if current.len() != groups.len() {
            return Err(SchemaError::StorageRead {
                detail: "protein old-key count differs".into(),
            });
        }
        let mut removals = BTreeMap::new();
        for ((movement, candidates), current) in job.sibling_moves.iter().zip(groups).zip(current) {
            if current.as_ref().is_none_or(|record| {
                record.entry != movement.expected_old.entry
                    || record.meta != movement.expected_old.meta
            }) {
                continue;
            }
            plan_removals(movement, &candidates, &mut removals)?;
        }
        for (_, slots) in removals.values_mut() {
            slots.sort();
            slots.dedup();
        }
        let removals: Vec<_> = removals
            .iter()
            .map(|(uuid, (data, slots))| {
                (
                    uuid.as_str(),
                    data,
                    slots.as_slice(),
                    job.storage_prefix.as_deref(),
                )
            })
            .collect();
        atoms
            .remove_molecules_keys_batch(&removals)
            .await
            .map(|_| ())
    }
}

fn plan_removals(
    movement: &SiblingMove,
    candidates: &[String],
    removals: &mut RemovedMolecules,
) -> Result<(), SchemaError> {
    for candidate in candidates {
        let uuid =
            molecule_key_codec::molecule_uuid_from_storage_key(candidate).ok_or_else(|| {
                SchemaError::InvalidData("protein move has an invalid molecule key".into())
            })?;
        let suffix = candidate
            .strip_prefix(&molecule_key_codec::molecule_record_prefix(uuid))
            .and_then(molecule_key_codec::decode_hash_range_suffix)
            .ok_or_else(|| {
                SchemaError::InvalidData("protein move has an invalid key coordinate".into())
            })?;
        let (_, slots) = removals.entry(uuid.to_owned()).or_insert_with(|| {
            (
                MoleculeHashRange::from_write_records(
                    uuid.to_owned(),
                    movement.header.version,
                    movement.header.updated_at,
                    Vec::new(),
                ),
                Vec::new(),
            )
        });
        slots.push(suffix);
    }
    Ok(())
}
