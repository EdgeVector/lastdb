//! A shared schema row changes only to a compatible later definition of its
//! name (`decision-2026-11-07-lastdb-schema-name-slot-definition-per-record`).

use super::invalid;
use crate::schema::Schema;
use crate::storage::error::StorageResult;

/// The later row keeps the name, the key, the alternate keys and every field
/// of the earlier row, and only adds fields. So the record space and the
/// alternate entries of the name stay where they are.
pub(super) fn require(earlier: &[u8], later: &[u8]) -> StorageResult<()> {
    let decode = |bytes: &[u8]| {
        serde_json::from_slice::<Schema>(bytes)
            .map(|schema| schema.to_wire())
            .map_err(|_| invalid("shared schema definition encoding"))
    };
    let extends = decode(later)?
        .extends(&decode(earlier)?)
        .map_err(|error| invalid(&error))?;
    if !extends {
        return Err(invalid("a shared schema definition cannot be replaced"));
    }
    Ok(())
}
