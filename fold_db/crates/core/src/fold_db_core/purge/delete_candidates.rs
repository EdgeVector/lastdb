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

pub(crate) fn delete_candidate_for_key(
    db_ops: &Arc<DbOperations>,
    field: &FieldVariant,
    key: &KeyValue,
) -> Result<Option<DeleteCandidate>, SchemaError> {
    let Some(uuid) = field.common().molecule_uuid() else {
        return Ok(None);
    };
    let Some((hash, range)) = field.slot_coordinates(key) else {
        return Ok(None);
    };
    let codec = db_ops.atoms().key_codec_for_molecule(uuid)?;
    let hash = codec
        .storage_hash(uuid, &hash)
        .map_err(|error| SchemaError::InvalidData(error.to_string()))?;
    let range = codec
        .storage_range(uuid, &range)
        .map_err(|error| SchemaError::InvalidData(error.to_string()))?;
    let base = crate::atom::molecule_key_codec::hash_range_record_key(uuid, &hash, &range);
    Ok(Some(DeleteCandidate {
        mk_key: build_storage_key(field.common().storage_prefix(), &base),
        molecule_uuid: uuid.clone(),
        hash,
        range,
        storage_prefix: field.common().storage_prefix().map(str::to_string),
    }))
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
            if let Some(candidate) = delete_candidate_for_key(db_ops, field, &mutation.key_value)? {
                let barrier = DeleteBarrier {
                    mk_key: candidate.mk_key.clone(),
                    written_at,
                    logical_counter: mutation.logical_counter,
                    device_id: device_id.clone(),
                    mutation_uuid: mutation.uuid.clone(),
                    kind: DeleteKind::Normal,
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
