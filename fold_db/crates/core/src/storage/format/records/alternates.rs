//! The existing alternate-pointer wire type and its typed write inputs.

use super::AlternateAddress;
use crate::schema::types::KeyValue;
use crate::schema::SchemaError;
use crate::storage::KvMutation;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AlternateEntry {
    pub primary: KeyValue,
    pub matched: KeyValue,
}

/// A change producer supplies an entry value, never its serialized bytes.
pub(crate) enum AlternateChange {
    Put(AlternateAddress, AlternateEntry),
    Delete(AlternateAddress),
}

pub(super) fn encode(change: AlternateChange) -> Result<KvMutation, SchemaError> {
    Ok(match change {
        AlternateChange::Put(key, entry) => KvMutation::put(
            key.into_string(),
            serde_json::to_vec(&entry)
                .map_err(|_| SchemaError::InvalidData("encode alternate entry".into()))?,
        ),
        AlternateChange::Delete(key) => KvMutation::delete(key.into_string()),
    })
}
