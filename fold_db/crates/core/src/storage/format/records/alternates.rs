//! Alternate pointers are derived from the record transition in this write.

use super::{AlternateAddress, RecordChange};
use crate::schema::types::{KeyValue, Schema};
use crate::schema::SchemaError;
use crate::storage::KvMutation;
use lastdb_model::field_key_codec::FieldKeyCodec;
use lastdb_model::field_key_id::alternate_field_key_id;
use serde::{Deserialize, Serialize};

pub const MAX_ALTERNATE_ENTRIES: usize = 4096;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AlternateEntry {
    pub primary: KeyValue,
    pub matched: KeyValue,
}

pub(super) fn encode(
    schema: &Schema,
    changes: &[RecordChange],
    mut resolve: impl FnMut(&str) -> Result<FieldKeyCodec, SchemaError>,
) -> Result<Vec<KvMutation>, SchemaError> {
    let definitions = schema.require_record_keys()?;
    if definitions.alternates.is_empty() {
        return Ok(Vec::new());
    }
    let mut pointers = Vec::new();
    for change in changes {
        let before = change.before.as_ref().filter(|record| record.is_live());
        let after = change.after.is_live().then_some(&change.after);
        for name in definitions.alternates.keys() {
            let old = before.and_then(|record| record.alternates.get(name)?.as_ref());
            let new = after.and_then(|record| record.alternates.get(name)?.as_ref());
            if old == new {
                continue;
            }
            let field_key = alternate_field_key_id(schema, name);
            let codec = resolve(&field_key)?;
            if let Some(old) = old {
                let key = AlternateAddress::new(&codec, &field_key, old, &change.key)?;
                pointers.push(KvMutation::delete(key.into_string()));
            }
            if let Some(new) = new {
                let key = AlternateAddress::new(&codec, &field_key, new, &change.key)?;
                let entry = AlternateEntry {
                    primary: change.key.api_key().clone(),
                    matched: new.clone(),
                };
                let bytes = serde_json::to_vec(&entry)
                    .map_err(|_| SchemaError::InvalidData("encode alternate entry".into()))?;
                pointers.push(KvMutation::put(key.into_string(), bytes));
            }
            if pointers.len() > MAX_ALTERNATE_ENTRIES {
                return Err(SchemaError::InvalidData(
                    "alternate change batch exceeds 4096 entries".into(),
                ));
            }
        }
    }
    Ok(pointers)
}
