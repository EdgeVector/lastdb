//! Alternate molecules select primary records. Canonical field reuse is independent.

use crate::schema::types::field::build_storage_key;
use crate::schema::types::key_value::scalar_key_coordinate;
use crate::schema::types::{KeyValue, Schema};
use crate::schema::SchemaError;
use base64::Engine as _;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

mod fields;
mod preparation;
pub(crate) use preparation::affected_keys;
pub(crate) mod read;
mod write;

pub const MAX_ALTERNATE_ENTRIES: usize = 4096;
pub(crate) const PREFIX: &str = "ak:";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AlternateEntry {
    pub primary: KeyValue,
    pub matched: KeyValue,
}

pub(crate) type RecordFields = BTreeMap<String, Value>;

/// The stable identity of one named alternate in its exact declaration revision.
pub fn molecule(schema: &Schema, name: &str) -> String {
    let revision = schema.alternate_key_revision();
    let mut hash = Sha256::new();
    hash.update(b"lastdb:alternate-molecule:v1");
    for part in [schema.name.as_str(), revision.as_str(), name] {
        hash.update(
            u64::try_from(part.len())
                .expect("identity length fits u64")
                .to_be_bytes(),
        );
        hash.update(part.as_bytes());
    }
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(hash.finalize())
}

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
    let mut coordinates = Vec::new();
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
                coordinates.push(
                    scalar_key_coordinate(value)
                        .ok_or_else(|| invalid("alternate key field requires a scalar value"))?
                        .into_owned(),
                );
            }
            None => return Err(invalid("required alternate key field is absent")),
        }
    }
    Ok(Some(KeyValue::new(
        Some(coordinates.remove(0)),
        coordinates.pop(),
    )))
}

pub(crate) fn storage_key(
    atoms: &crate::db_operations::atom_store::AtomStore,
    schema: &Schema,
    name: &str,
    key: &KeyValue,
) -> Result<String, SchemaError> {
    let molecule = molecule(schema, name);
    let encoded = atoms
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
