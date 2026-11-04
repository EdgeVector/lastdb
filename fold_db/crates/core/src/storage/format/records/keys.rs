//! Addresses minted by the existing format's typed key codecs.

use crate::schema::types::KeyValue;
use crate::schema::SchemaError;
use lastdb_model::field_key_codec::FieldKeyCodec;
use lastdb_model::field_key_id::ALTERNATE_ENTRY_PREFIX;

/// A primary record address. No caller can supply an arbitrary storage string.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RecordAddress(String);

impl RecordAddress {
    pub(crate) fn new(
        codec: &FieldKeyCodec,
        space: &str,
        key: &KeyValue,
    ) -> Result<Self, SchemaError> {
        let hash = key
            .hash
            .as_deref()
            .ok_or_else(|| SchemaError::InvalidData("a record key requires a hash".into()))?;
        codec
            .api_record_key(space, hash, key.range.as_deref().unwrap_or_default())
            .map(Self)
            .map_err(|error| SchemaError::InvalidData(format!("invalid record key: {error}")))
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }

    pub(crate) fn into_string(self) -> String {
        self.0
    }
}

impl AsRef<str> for RecordAddress {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

/// An alternate entry address. The family prefix and coordinate encoding are
/// selected here rather than supplied by the change producer.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct AlternateAddress(String);

impl AlternateAddress {
    pub(crate) fn new(
        codec: &FieldKeyCodec,
        field_key: &str,
        key: &KeyValue,
    ) -> Result<Self, SchemaError> {
        let encoded = codec
            .api_hash_range_record_key(
                field_key,
                key.hash.as_deref().unwrap_or_default(),
                key.range.as_deref().unwrap_or_default(),
            )
            .map_err(|_| {
                SchemaError::InvalidData("alternate key coordinate has an invalid type".into())
            })?;
        Ok(Self(format!("{ALTERNATE_ENTRY_PREFIX}{encoded}")))
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }

    pub(crate) fn into_string(self) -> String {
        self.0
    }
}
