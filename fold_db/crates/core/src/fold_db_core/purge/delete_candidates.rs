//! Exact physical keys that the normal point reader accepts for one API key.

use super::*;
use crate::atom::delete_barrier::{DeleteBarrier, DeleteKind};
use crate::schema::types::field::FieldVariant;

#[derive(Clone)]
pub(crate) struct DeleteCandidate {
    pub(crate) mk_key: String,
    pub(crate) molecule_uuid: String,
    pub(crate) hash: String,
    pub(crate) range: String,
    pub(crate) storage_prefix: Option<String>,
}

pub(crate) fn delete_candidates_for_key(
    db_ops: &Arc<DbOperations>,
    field: &FieldVariant,
    key: &KeyValue,
) -> Result<Vec<DeleteCandidate>, SchemaError> {
    let Some(uuid) = field.common().molecule_uuid() else {
        return Ok(Vec::new());
    };
    let Some((hash, range)) = field.disk_slot_for_key(key) else {
        return Ok(Vec::new());
    };
    let codec = db_ops.atoms().key_codec_for_molecule(uuid);
    let keys = codec
        .api_hash_range_record_keys_for_read(uuid, &hash, &range)
        .map_err(|error| SchemaError::InvalidData(error.to_string()))?;
    keys.into_iter()
        .map(|base| {
            let molecule_uuid =
                crate::atom::molecule_key_codec::molecule_uuid_from_storage_key(&base)
                    .ok_or_else(|| {
                        SchemaError::InvalidData("Delete candidate lacks a molecule UUID".into())
                    })?
                    .to_string();
            let (hash, range) =
                crate::atom::molecule_key_codec::decode_hash_range(&base, &molecule_uuid)
                    .ok_or_else(|| {
                        SchemaError::InvalidData("Delete candidate has an invalid key".into())
                    })?;
            Ok(DeleteCandidate {
                mk_key: build_storage_key(field.common().storage_prefix(), &base),
                molecule_uuid,
                hash,
                range,
                storage_prefix: field.common().storage_prefix().map(str::to_string),
            })
        })
        .collect()
}

/// Preserve the signed source order for every finite readable physical key.
pub(crate) fn plan_normal_delete_barriers(
    db_ops: &Arc<DbOperations>,
    schema: &crate::schema::types::Schema,
    mutations: &[crate::schema::types::Mutation],
) -> Result<Vec<DeleteBarrier>, SchemaError> {
    let mut by_key: HashMap<String, DeleteBarrier> = HashMap::new();
    for mutation in mutations {
        let written_at = mutation.imported_written_at.ok_or_else(|| {
            SchemaError::InvalidData("normal Delete lacks the original device write time".into())
        })?;
        let device_id = if mutation.author_clock_writer_id.is_empty() {
            mutation.pub_key.clone()
        } else {
            mutation.author_clock_writer_id.clone()
        };
        for field in schema.runtime_fields.values() {
            for candidate in delete_candidates_for_key(db_ops, field, &mutation.key_value)? {
                let barrier = DeleteBarrier {
                    mk_key: candidate.mk_key.clone(),
                    written_at,
                    logical_counter: mutation.logical_counter,
                    device_id: device_id.clone(),
                    mutation_uuid: mutation.uuid.clone(),
                    kind: DeleteKind::Normal,
                    displaced_atom_uuid: None,
                    cloud_sequence: None,
                    tip_before_nanos: mutation.erase_tips_before_nanos,
                };
                match by_key.entry(candidate.mk_key) {
                    std::collections::hash_map::Entry::Vacant(slot) => {
                        slot.insert(barrier);
                    }
                    std::collections::hash_map::Entry::Occupied(mut slot) => {
                        if barrier.is_newer_than(slot.get()) {
                            slot.insert(barrier);
                        }
                    }
                }
            }
        }
    }
    let mut barriers: Vec<_> = by_key.into_values().collect();
    barriers.sort_unstable_by(|a, b| a.mk_key.cmp(&b.mk_key));
    Ok(barriers)
}
