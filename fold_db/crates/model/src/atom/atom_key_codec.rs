//! Format 6 atom record keys.
//!
//! A format 6 home kept each atom body at `atom:{uuid}\0b` and its counter at
//! `atom:{uuid}\0c`. A format 7 write creates neither. The restore proof uses
//! [`uuid_of`] to name such a row before it refuses it. The key law refuses
//! the whole `atom` family.

use crate::kind_partition;

/// The tag after the NUL of a body key.
const BODY_TAG: &str = "b";

/// The tag after the NUL of a counter key.
const COUNTER_TAG: &str = "c";

/// The atom UUID inside a body key or counter key, with or without a storage scope.
///
/// Returns `None` for a key that is not an atom record key.
#[must_use]
pub fn uuid_of(key: &str) -> Option<&str> {
    let rest = kind_partition::rest_of(key, "atom")?;
    let (uuid, tag) = rest.split_once('\0')?;
    (!uuid.is_empty() && matches!(tag, BODY_TAG | COUNTER_TAG)).then_some(uuid)
}
