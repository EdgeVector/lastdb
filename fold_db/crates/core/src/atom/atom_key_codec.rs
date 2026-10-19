//! Atom body storage keys.
//!
//! The atom UUID identifies content. Every atom body in one storage scope has
//! one record in the `atoms` collection:
//!
//! | Record | Key |
//! |---|---|
//! | body | `atom:{uuid}\0b` |
//!
//! A key hashes on its first NUL, so the key places one atom in one hash group.
//! There is no locator record and no partition-prefix layout: a body is found
//! by one point read of its key.
//!
//! Callers add an optional storage scope with `build_storage_key`.

use crate::kind_partition;

/// Base prefix every atom record key starts with.
pub const ATOM_PREFIX: &str = "atom:";

/// The tag after the NUL of a body key.
const BODY_TAG: &str = "b";

/// Body key `atom:{uuid}\0b`.
#[must_use]
pub fn body_key(atom_uuid: &str) -> String {
    format!("{ATOM_PREFIX}{atom_uuid}\0{BODY_TAG}")
}

/// The atom UUID inside a body key, with or without a storage scope.
///
/// Returns `None` for a key that is not an atom body key.
#[must_use]
pub fn uuid_of(key: &str) -> Option<&str> {
    let rest = kind_partition::rest_of(key, "atom")?;
    let (uuid, tag) = rest.split_once('\0')?;
    (!uuid.is_empty() && tag == BODY_TAG).then_some(uuid)
}
