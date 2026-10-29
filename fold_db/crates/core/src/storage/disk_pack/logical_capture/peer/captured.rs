//! Read exact peer keys from the immutable native files of one held snapshot.

use super::*;
use laststore::disk_run::home::RunRead;

pub(in crate::storage::disk_pack::logical_capture) fn positions(
    read: &mut RunRead,
    roster_bytes: Option<&[u8]>,
    own_origin: &str,
) -> StorageResult<Vec<PeerPosition>> {
    let roster: Roster = roster_bytes.map(decode).transpose()?.unwrap_or_default();
    roster.validate(own_origin)?;
    let coordinates = roster
        .origins
        .iter()
        .map(|origin| (COLLECTION.to_string(), peer_key(origin)))
        .collect::<Vec<_>>();
    let values = read
        .get_many_bounded(&coordinates, CAPTURE_RECORD_BYTES)
        .map_err(map)?;
    let mut positions = Vec::with_capacity(values.len());
    let mut receipts = 0u64;
    for (origin, bytes) in roster.origins.iter().zip(values) {
        let bytes = bytes
            .map_err(map)?
            .ok_or_else(|| invalid("snapshot logical peer position is absent"))?;
        let peer: PeerPosition = decode(&bytes)?;
        peer.validate(origin)?;
        receipts = receipts
            .checked_add(peer.position.sequence)
            .ok_or_else(|| invalid("snapshot logical peer receipt count overflow"))?;
        positions.push(peer);
    }
    if receipts != roster.receipt_count {
        return Err(invalid("snapshot logical peer receipt count differs"));
    }
    Ok(positions)
}
