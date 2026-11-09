//! Addresses minted by the existing format's typed key codecs.

use crate::schema::types::{field::HashRangeFilter, KeyValue};
use crate::schema::SchemaError;
use lastdb_model::field_key_codec::FieldKeyCodec;
use lastdb_model::field_key_id::ALTERNATE_ENTRY_PREFIX;

/// A primary record address. No caller can supply an arbitrary storage string.
#[derive(Clone, Debug)]
pub(crate) struct RecordAddress {
    storage: String,
    api: KeyValue,
}

impl PartialEq for RecordAddress {
    fn eq(&self, other: &Self) -> bool {
        self.storage == other.storage
    }
}
impl Eq for RecordAddress {}

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
            .map(|storage| Self {
                storage,
                api: key.clone(),
            })
            .map_err(|error| SchemaError::InvalidData(format!("invalid record key: {error}")))
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.storage
    }

    pub(crate) fn into_string(self) -> String {
        self.storage
    }

    pub(crate) fn api_key(&self) -> &KeyValue {
        &self.api
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
        primary: &RecordAddress,
    ) -> Result<Self, SchemaError> {
        use sha2::{Digest, Sha256};
        let base = alternate_range_key(codec, field_key, key)?;
        Ok(Self(format!(
            "{base}\0{:x}",
            Sha256::digest(primary.as_str().as_bytes())
        )))
    }

    pub(crate) fn into_string(self) -> String {
        self.0
    }
}

/// Read-only range position. Only a primary-bound AlternateAddress can be written.
pub(crate) fn alternate_range_key(
    codec: &FieldKeyCodec,
    field_key: &str,
    key: &KeyValue,
) -> Result<String, SchemaError> {
    let encoded = codec
        .api_hash_range_member_prefix(
            field_key,
            key.hash.as_deref().unwrap_or_default(),
            key.range.as_deref().unwrap_or_default(),
        )
        .map_err(|_| {
            SchemaError::InvalidData("alternate key coordinate has an invalid type".into())
        })?;
    Ok(format!("{ALTERNATE_ENTRY_PREFIX}{encoded}"))
}

/// Storage bounds for a declared alternate key under exactly one hash.
pub(crate) fn alternate_bounds(
    codec: &FieldKeyCodec,
    field_key: &str,
    filter: &HashRangeFilter,
) -> Result<(String, String), SchemaError> {
    use lastdb_model::kind_partition::exact_prefix_bounds;
    let invalid = |message: &str| SchemaError::InvalidData(message.into());
    let encode = |hash: &str, range: &str| {
        alternate_range_key(
            codec,
            field_key,
            &KeyValue::new(Some(hash.to_owned()), Some(range.to_owned())),
        )
    };
    match filter {
        HashRangeFilter::HashRangeKey { hash, range } => {
            let key = encode(hash, range)?;
            Ok(exact_prefix_bounds(&format!("{key}\0")))
        }
        HashRangeFilter::HashKey(hash) => {
            let prefix = codec
                .api_hash_range_scan_prefix_for_hash(field_key, hash)
                .map_err(|_| invalid("invalid alternate hash"))?;
            Ok(exact_prefix_bounds(&format!(
                "{ALTERNATE_ENTRY_PREFIX}{prefix}"
            )))
        }
        HashRangeFilter::HashRangePrefix { hash, prefix } => {
            codec
                .require_text_range_prefix()
                .map_err(|_| invalid("alternate range prefix requires a text field"))?;
            Ok(exact_prefix_bounds(&encode(hash, prefix)?))
        }
        HashRangeFilter::HashRangeRange { hash, start, end } => {
            Ok((encode(hash, start)?, encode(hash, end)?))
        }
        _ => Err(invalid(
            "alternate reads require an exact key or a range under one hash",
        )),
    }
}
