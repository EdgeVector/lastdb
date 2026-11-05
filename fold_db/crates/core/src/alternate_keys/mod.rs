//! Explicit named addresses point to complete primary records.
use crate::schema::types::{KeyValue, Schema};
use crate::schema::SchemaError;
use crate::storage::format::records::{alternate_range_key, AlternateAddress};

pub(crate) mod read;

pub(crate) const PREFIX: &str = lastdb_model::field_key_id::ALTERNATE_ENTRY_PREFIX;
pub use crate::storage::format::records::{AlternateEntry, MAX_ALTERNATE_ENTRIES};
pub use lastdb_model::field_key_id::alternate_field_key_id as field_key;

pub(crate) fn storage_key(
    records: &crate::db_operations::record_store::RecordStore,
    schema: &Schema,
    name: &str,
    key: &KeyValue,
    primary: &KeyValue,
) -> Result<String, SchemaError> {
    storage_address(records, schema, name, key, primary).map(AlternateAddress::into_string)
}

pub(crate) fn range_key(
    records: &crate::db_operations::record_store::RecordStore,
    schema: &Schema,
    name: &str,
    key: &KeyValue,
) -> Result<String, SchemaError> {
    let id = field_key(schema, name);
    alternate_range_key(&records.key_codec_for_field_key(&id)?, &id, key)
}

pub(crate) fn storage_address(
    records: &crate::db_operations::record_store::RecordStore,
    schema: &Schema,
    name: &str,
    key: &KeyValue,
    primary: &KeyValue,
) -> Result<AlternateAddress, SchemaError> {
    let id = field_key(schema, name);
    let target = records.record_address(schema, primary)?;
    AlternateAddress::new(&records.key_codec_for_field_key(&id)?, &id, key, &target)
}

pub(crate) fn invalid(message: &str) -> SchemaError {
    SchemaError::InvalidData(message.into())
}
