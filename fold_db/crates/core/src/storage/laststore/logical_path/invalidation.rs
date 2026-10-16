//! Exact-key invalidation for writes that bypass logical admission.

use super::*;

pub(super) fn logical_cache_key(id: &str) -> Option<crate::resident::ResidentKey> {
    if let Some(atom) = parse_atom_id(id) {
        return Some(crate::resident::ResidentKey::Atom(atom));
    }
    if let Some(coords) = parse_tip_coords(id) {
        return Some(crate::resident::ResidentKey::MoleculeTip {
            molecule: coords.molecule,
            hash: coords.hash,
            range: coords.range,
        });
    }
    None
}

pub(in crate::storage::laststore) fn invalidate_written_id(set: &mut LogicalResidentSet, id: &str) {
    if let Some(coords) = parse_tip_coords(id) {
        set.invalidate_tip(coords.molecule, &coords.hash, &coords.range);
    }
    if let Some(atom) = parse_atom_id(id) {
        set.drop_atom_body(&atom);
    }
    if let Some(key) = logical_cache_key(id) {
        set.clear_write_tokens(&key);
    }
}
