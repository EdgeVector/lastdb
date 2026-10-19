//! GC preserves current typed heads outside the purge's exact target slots.

use crate::db_operations::DbOperations;
use crate::schema::{Schema, SchemaError};
use std::collections::{HashMap, HashSet};

/// Preserve current heads outside the exact scoped slots this purge removes.
/// The core holds API keys; planned targets already hold encoded disk keys.
pub(crate) fn collect_core_live_atom_uuids(
    db_ops: &DbOperations,
    schema: &Schema,
    storage_slots: &[super::PlannedStorageSlot],
) -> Result<HashSet<String>, SchemaError> {
    let Some(runtime) = db_ops.namespaced_store().disk_pack_runtime() else {
        return Ok(HashSet::new());
    };
    let mut removed: HashMap<_, HashMap<String, HashSet<String>>> = HashMap::new();
    for slot in storage_slots {
        let field = schema.runtime_fields.get(&slot.field_name).ok_or_else(|| {
            SchemaError::InvalidData("purge target field disappeared before the core guard".into())
        })?;
        if field.common().molecule_uuid().map(String::as_str) != Some(slot.molecule_uuid.as_str()) {
            return Err(SchemaError::InvalidData(
                "purge target molecule changed before the core guard".into(),
            ));
        }
        removed
            .entry((
                field.common().storage_prefix().map(str::to_owned),
                slot.molecule_uuid.clone(),
            ))
            .or_default()
            .entry(slot.disk_hash.clone())
            .or_default()
            .insert(slot.disk_range.clone());
    }
    let bindings: HashSet<_> = schema
        .runtime_fields
        .values()
        .filter_map(|field| {
            Some((
                field.common().storage_prefix().map(str::to_owned),
                field.common().molecule_uuid()?.clone(),
            ))
        })
        .collect();
    let bindings: Vec<_> = bindings.into_iter().collect();
    let excluded: Vec<_> = bindings
        .iter()
        .map(|binding| removed.remove(binding))
        .collect();
    let codecs: Vec<_> = bindings
        .iter()
        .map(|(_, molecule)| db_ops.atoms().key_codec_for_molecule(molecule))
        .collect::<Result<Vec<_>, _>>()?;
    runtime.current_core_atom_uuids(&bindings, |index, hash, range| {
        let Some(targets) = &excluded[index] else {
            return Ok(true);
        };
        let molecule = &bindings[index].1;
        let hash = codecs[index]
            .storage_hash(molecule, hash)
            .map_err(|error| SchemaError::InvalidData(error.to_string()))?;
        let Some(ranges) = targets.get(&hash) else {
            return Ok(true);
        };
        let range = codecs[index]
            .storage_range(molecule, range)
            .map_err(|error| SchemaError::InvalidData(error.to_string()))?;
        Ok(!ranges.contains(&range))
    })
}
