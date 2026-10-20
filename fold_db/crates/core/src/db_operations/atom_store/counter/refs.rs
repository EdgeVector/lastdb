//! The atoms that a conflict record holds references to.

use crate::atom::MutationEvent;
use std::collections::BTreeSet;

/// The distinct atoms that a history event names: the new atom, the old atom of
/// a replaced tip, and the loser of a conflict. A record holds one reference for
/// each of them for as long as it exists.
pub(crate) fn event_atoms(event: &MutationEvent) -> Vec<String> {
    let atoms: BTreeSet<&str> = std::iter::once(event.new_atom_uuid.as_str())
        .chain(event.old_atom_uuid.as_deref())
        .chain(event.conflict_loser_atom.as_deref())
        .collect();
    atoms.into_iter().map(str::to_string).collect()
}
