//! Temporary admission bridge until the history value encoders migrate.
//!
//! It cannot introduce record or alternate writes into the opaque batch.
//! The native store still validates complete key shapes and request limits.

use crate::schema::SchemaError;
use crate::storage::format::HISTORY_DATA_FAMILIES;
use crate::storage::KvMutation;

pub(super) fn append(
    batch: &mut Vec<KvMutation>,
    history: Vec<KvMutation>,
) -> Result<(), SchemaError> {
    for row in &history {
        let family = row
            .key()
            .split(|byte| matches!(*byte, b':' | b'\0'))
            .next()
            .unwrap_or_default();
        if !HISTORY_DATA_FAMILIES
            .iter()
            .any(|name| name.as_bytes() == family)
        {
            return Err(SchemaError::InvalidData(
                "the legacy history writer supplied a foreign key family".into(),
            ));
        }
    }
    batch.extend(history);
    Ok(())
}
