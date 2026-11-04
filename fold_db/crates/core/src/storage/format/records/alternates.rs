//! The existing alternate-pointer wire type and its typed write inputs.

use super::{AlternateAddress, RecordAddress};
use crate::schema::types::KeyValue;
use crate::schema::SchemaError;
use crate::storage::KvMutation;
use lastdb_model::field_key_codec::FieldKeyCodec;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AlternateEntry {
    pub primary: KeyValue,
    pub matched: KeyValue,
}

/// The destination and payload are minted together and cannot be replaced.
/// Read-side AlternateEntry values cannot be submitted independently.
pub(crate) struct AlternateChange {
    key: AlternateAddress,
    entry: Option<AlternateEntry>,
}

impl AlternateChange {
    pub(crate) fn put(
        primary_codec: &FieldKeyCodec,
        record_space: &str,
        primary: KeyValue,
        alternate_codec: &FieldKeyCodec,
        field_key: &str,
        matched: KeyValue,
    ) -> Result<Self, SchemaError> {
        // The reader resolves this target with the primary key's codec,
        // whose range type may differ from the alternate's range type.
        let target = RecordAddress::new(primary_codec, record_space, &primary)?;
        Ok(Self {
            key: AlternateAddress::new(alternate_codec, field_key, &matched, &target)?,
            entry: Some(AlternateEntry { primary, matched }),
        })
    }

    pub(crate) fn delete(key: AlternateAddress) -> Self {
        Self { key, entry: None }
    }
}

pub(super) fn encode(change: AlternateChange) -> Result<KvMutation, SchemaError> {
    let key = change.key.into_string();
    Ok(match change.entry {
        Some(entry) => KvMutation::put(
            key,
            serde_json::to_vec(&entry)
                .map_err(|_| SchemaError::InvalidData("encode alternate entry".into()))?,
        ),
        None => KvMutation::delete(key),
    })
}
