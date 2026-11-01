//! The atoms namespace of a format 7 home holds blob counters and their
//! queues. A format 7 write creates no atom body and no atom counter, so an
//! atom row in a backup comes from a format 6 home. This release refuses it.
use super::{files, invalid, rows::Rows, Proof, StorageResult};
use crate::storage::disk_pack::native_routing;

/// Validate every coordinate of the atoms namespace with the current-key law
/// and refuse an atom row. The count of proved atoms stays in the restore
/// receipt for its readers; it is zero for a format 7 home.
pub(super) fn verify(proof: &Proof<'_>) -> StorageResult<u64> {
    let mut read = super::rows::read(proof.main, proof.key_heads)?;
    let mut rows = Rows::new(proof.main, read.as_mut(), "atoms")?;
    while let Some((coordinate, body)) = rows.next()? {
        drop(body);
        files::validate_restored_atom_key(&coordinate)?;
        let raw = native_routing::decode(&coordinate)?;
        let raw = std::str::from_utf8(&raw)
            .map_err(|_| invalid("run backup atom coordinate encoding"))?;
        if crate::atom::atom_key_codec::uuid_of(raw).is_some() {
            return Err(invalid("run backup holds a format 6 atom row"));
        }
    }
    Ok(0)
}
