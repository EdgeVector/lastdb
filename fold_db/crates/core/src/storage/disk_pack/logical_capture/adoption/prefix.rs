//! One bounded batch proves common hashes without reading any event bodies.

use super::super::{key, model::EventMetadata, peer, unhex, COLLECTION};
use super::{compare, invalid, SnapshotInclusionMap, StorageResult};
use crate::storage::disk_pack::map;
use laststore::disk_run::home::RunRead;

pub(super) fn verify(
    main: &mut RunRead,
    local: &SnapshotInclusionMap,
    candidate: &SnapshotInclusionMap,
) -> StorageResult<()> {
    let checks = candidate
        .positions
        .iter()
        .filter(|included| {
            compare::position(local, &included.origin).is_some_and(|known| {
                included.sequence.get() > 0 && included.sequence < known.sequence
            })
        })
        .collect::<Vec<_>>();
    let coordinates = checks
        .iter()
        .map(|included| {
            let key = if included.origin == local.producer {
                key("m", included.sequence.get())
            } else {
                peer::receipt_key(&included.origin, included.sequence.get())
            };
            (COLLECTION.to_string(), key)
        })
        .collect::<Vec<_>>();
    // At most 33 * 16 KiB of raw values coexist with two bounded maps, keys,
    // the report and one decoded metadata record in the reserved 1 MiB workspace.
    let values = main
        .get_many_bounded(&coordinates, peer::CAPTURE_RECORD_BYTES)
        .map_err(map)?;
    if values.len() != checks.len() {
        return Err(invalid("snapshot adoption common position count differs"));
    }
    for (included, bytes) in checks.into_iter().zip(values) {
        let bytes = bytes
            .map_err(map)?
            .ok_or_else(|| invalid("snapshot adoption common position is not retained"))?;
        if included.origin == local.producer {
            let metadata: EventMetadata = super::super::decode(&bytes)?;
            metadata.verify_prefix(&included.origin, included.sequence.get(), &included.sha256)?;
        } else if bytes.as_slice() != unhex::<32>(&included.sha256)?.as_slice() {
            return Err(invalid("snapshot adoption imported prefix differs"));
        }
    }
    Ok(())
}
