//! Alternate molecules select primary records. Canonical field reuse is independent.

use crate::schema::types::field::build_storage_key;
use crate::schema::types::key_value::scalar_key_coordinate;
use crate::schema::types::{KeyValue, Schema};
use crate::schema::SchemaError;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

mod derivation;
mod fields;
pub(crate) mod read;
mod write;

pub const MAX_ALTERNATE_ENTRIES: usize = 4096;
pub(crate) const PREFIX: &str = lastdb_model::molecule_uuid::ALTERNATE_ENTRY_PREFIX;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AlternateEntry {
    pub primary: KeyValue,
    pub matched: KeyValue,
}

pub(crate) type RecordFields = BTreeMap<String, Value>;

/// The stable identity of one named alternate in its exact declaration revision.
/// The molecule key store names it too, so it lives in the molecule model.
pub use lastdb_model::molecule_uuid::alternate_molecule_uuid as molecule;

pub(crate) fn derived(
    schema: &Schema,
    fields: &RecordFields,
) -> Result<BTreeMap<String, KeyValue>, SchemaError> {
    let mut keys = BTreeMap::new();
    let primary = schema
        .key
        .as_ref()
        .and_then(|k| k.hash_field.as_deref())
        .ok_or_else(|| invalid("alternate schema has no primary field"))?;
    if !fields.contains_key(primary) {
        return Ok(keys);
    }
    for (name, definition) in &schema.alternate_keys {
        if let Some(key) = derive_key(schema, definition, fields)? {
            keys.insert(name.clone(), key);
        }
    }
    Ok(keys)
}

pub(crate) fn derive_key(
    schema: &Schema,
    definition: &schema_types::AlternateKey,
    fields: &RecordFields,
) -> Result<Option<KeyValue>, SchemaError> {
    if definition.omit_empty_hash
        && fields.get(&definition.hash_field).and_then(Value::as_str) == Some("")
    {
        return Ok(None);
    }
    let mut coordinates = BTreeMap::new();
    for field in definition.fields() {
        match fields.get(field) {
            None | Some(Value::Null)
                if definition.missing == schema_types::MissingAlternateKey::Omit =>
            {
                return Ok(None);
            }
            Some(value) => {
                schema
                    .get_field_type(field)
                    .validate(value)
                    .map_err(|_| invalid("alternate key field has an invalid type"))?;
                coordinates.insert(
                    field,
                    scalar_key_coordinate(value)
                        .ok_or_else(|| invalid("alternate key field requires a scalar value"))?
                        .into_owned(),
                );
            }
            None => return Err(invalid("required alternate key field is absent")),
        }
    }
    derivation::key(definition, fields, &coordinates).map(Some)
}

pub(crate) fn storage_key(
    records: &crate::db_operations::record_store::RecordStore,
    schema: &Schema,
    name: &str,
    key: &KeyValue,
) -> Result<String, SchemaError> {
    let molecule = molecule(schema, name);
    let encoded = records
        .key_codec_for_molecule(&molecule)?
        .api_hash_range_record_key(
            &molecule,
            key.hash.as_deref().unwrap_or_default(),
            key.range.as_deref().unwrap_or_default(),
        )
        .map_err(|_| invalid("alternate key coordinate has an invalid type"))?;
    Ok(format!(
        "{PREFIX}{}",
        encoded.strip_prefix("mk:").expect("typed molecule key")
    ))
}

pub(crate) fn invalid(message: &str) -> SchemaError {
    SchemaError::InvalidData(message.into())
}
