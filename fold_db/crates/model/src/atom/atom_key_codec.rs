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

/// The tag after the NUL of a counter key.
const COUNTER_TAG: &str = "c";

/// Body key `atom:{uuid}\0b`.
#[must_use]
pub fn body_key(atom_uuid: &str) -> String {
    format!("{ATOM_PREFIX}{atom_uuid}\0{BODY_TAG}")
}

/// Counter key `atom:{uuid}\0c`. The counter and the body of one atom share a hash group.
#[must_use]
pub fn counter_key(atom_uuid: &str) -> String {
    format!("{ATOM_PREFIX}{atom_uuid}\0{COUNTER_TAG}")
}

/// Split an atom record key into its UUID and tag, with or without a storage scope.
fn split(key: &str) -> Option<(&str, &str)> {
    let rest = kind_partition::rest_of(key, "atom")?;
    let (uuid, tag) = rest.split_once('\0')?;
    (!uuid.is_empty() && matches!(tag, BODY_TAG | COUNTER_TAG)).then_some((uuid, tag))
}

/// The atom UUID inside a body key or counter key, with or without a storage scope.
///
/// Returns `None` for a key that is not an atom record key.
#[must_use]
pub fn uuid_of(key: &str) -> Option<&str> {
    split(key).map(|(uuid, _)| uuid)
}

/// The atom UUID inside a body key. `None` for a counter key or any other key.
#[must_use]
pub fn body_uuid_of(key: &str) -> Option<&str> {
    split(key)
        .filter(|(_, tag)| *tag == BODY_TAG)
        .map(|(uuid, _)| uuid)
}

/// True when `key` is a counter key, with or without a storage scope.
#[must_use]
pub fn is_counter_key(key: &str) -> bool {
    split(key).is_some_and(|(_, tag)| tag == COUNTER_TAG)
}
