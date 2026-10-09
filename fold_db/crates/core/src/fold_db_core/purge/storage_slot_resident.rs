//! Resolve storage-form purge targets to resident API-form keys.

use super::*;

/// Resolve storage-form purge targets to the API-form keys used by T0.
///
/// Resident tips deliberately stay in API form so point reads do not blind an
/// already-blinded hash. An owner storage scan carries the opposite form, and
/// BlindV1 cannot be decoded. Match the two by encoding the resident keys once
/// before any durable delete. The resident budget bounds this process-local
/// set independently of the molecule's durable cardinality.
pub(in crate::fold_db_core) fn resident_api_keys_for_storage_slots(
    db_ops: &DbOperations,
    targets: &HashSet<StorageSlotIdentity>,
) -> Result<HashMap<StorageSlotIdentity, ResidentApiKey>, SchemaError> {
    let molecule_uuids: HashSet<&str> = targets
        .iter()
        .map(|(molecule_uuid, _, _)| molecule_uuid.as_str())
        .collect();
    let codec = db_ops.atoms().key_codec();
    let mut resolved = HashMap::new();

    for molecule_uuid in molecule_uuids {
        for tip in db_ops.resident().tips_for_molecule(molecule_uuid) {
            let storage_hash = codec
                .storage_hash(molecule_uuid, &tip.hash)
                .map_err(|error| SchemaError::InvalidData(error.to_string()))?;
            let storage_range = codec
                .storage_range(molecule_uuid, &tip.range)
                .map_err(|error| SchemaError::InvalidData(error.to_string()))?;
            let storage_identity = (molecule_uuid.to_string(), storage_hash, storage_range);
            if targets.contains(&storage_identity) {
                resolved.insert(storage_identity, (tip.hash, tip.range));
            }
        }
    }

    Ok(resolved)
}
