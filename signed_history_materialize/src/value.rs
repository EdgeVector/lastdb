//! Encode and decode shared-history record value payloads.

use std::collections::{BTreeMap, HashMap};

use fold_db::fold_db_core::FoldDB;
use fold_db::record_value::InlineWrite;
use fold_db::schema::types::{KeyValue, Schema};
use serde_json::Value;

use crate::attachments::{self, FileSection};

/// Named alternate key map carried by a record target.
pub type Alternates = BTreeMap<String, Option<KeyValue>>;

const RECORD_WRAP_FIELDS: &str = "lastdb:record";
const RECORD_WRAP_ALTS: &str = "lastdb:alternates";

pub(crate) struct Stored {
    pub(crate) revision: String,
    pub(crate) created_by: Option<String>,
    pub(crate) updated_by: Option<String>,
    pub(crate) fields: HashMap<String, Value>,
    pub(crate) alternates: Alternates,
    pub(crate) file: Option<fold_db::record_value::FileMarker>,
    pub(crate) inline_file: Option<fold_db::record_value::FileMarker>,
}

/// Encode record fields, alternates, and attachment sections as UTF-8 JSON.
///
/// When `alternates` is empty and both attachment sections are Keep/absent,
/// encodes plain `fields` JSON so schemas without alternates or attachments
/// produce identical bytes. Field names in LastDB cannot contain `:`, so the
/// `lastdb:*` wrap keys never collide with user fields.
pub fn encode_record_value(
    fields: &Value,
    alternates: &Alternates,
    file: &FileSection,
    inline: Option<&InlineWrite>,
) -> Result<Vec<u8>, String> {
    let plain = alternates.is_empty() && matches!(file, FileSection::Keep) && inline.is_none();
    if plain {
        return serde_json::to_vec(fields).map_err(|e| format!("cannot encode record fields: {e}"));
    }
    let mut map = serde_json::Map::with_capacity(4);
    map.insert(RECORD_WRAP_FIELDS.to_owned(), fields.clone());
    let alts = serde_json::to_value(alternates)
        .map_err(|e| format!("cannot encode record alternates: {e}"))?;
    map.insert(RECORD_WRAP_ALTS.to_owned(), alts);
    attachments::insert_file(&mut map, file)?;
    attachments::insert_inline(&mut map, inline)?;
    serde_json::to_vec(&Value::Object(map))
        .map_err(|e| format!("cannot encode record envelope: {e}"))
}

/// Decoded record fields, alternate keys, and attachment sections.
pub struct DecodedRecord {
    pub fields: HashMap<String, Value>,
    pub alternates: Alternates,
    pub file: FileSection,
    pub inline: Option<InlineWrite>,
}

/// Decode non-empty record value JSON into fields, alternates, and attachments.
pub fn decode_record_value(value: &[u8]) -> Result<Option<DecodedRecord>, String> {
    if value.is_empty() {
        return Ok(None);
    }
    let Value::Object(mut map) = serde_json::from_slice::<Value>(value)
        .map_err(|e| format!("record value is not JSON: {e}"))?
    else {
        return Err("record value is not a JSON object".into());
    };
    let file = attachments::take_file(&mut map)?;
    let inline = attachments::take_inline(&mut map)?;
    if map.contains_key(RECORD_WRAP_FIELDS) && map.contains_key(RECORD_WRAP_ALTS) {
        let Some(Value::Object(fields_map)) = map.remove(RECORD_WRAP_FIELDS) else {
            return Err("wrapped record fields is not a JSON object".into());
        };
        let alts_val = map.remove(RECORD_WRAP_ALTS).unwrap_or(Value::Null);
        let alternates: Alternates = serde_json::from_value(alts_val)
            .map_err(|e| format!("wrapped record alternates is invalid: {e}"))?;
        if !map.is_empty() {
            return Err("wrapped record value has unknown keys".into());
        }
        return Ok(Some(DecodedRecord {
            fields: fields_map.into_iter().collect(),
            alternates,
            file,
            inline,
        }));
    }
    if matches!(file, FileSection::Keep) && inline.is_none() {
        return Ok(Some(DecodedRecord {
            fields: map.into_iter().collect(),
            alternates: BTreeMap::new(),
            file,
            inline,
        }));
    }
    Err("attachment sections require the lastdb:record wrap".into())
}

/// Normalize `supplied` alternates against `namespace`'s declared alternate keys,
/// filling `None` for omitted `allow_absent` keys and canonicalizing range coordinates.
pub fn resolve_alternates(
    store: &FoldDB,
    namespace: &str,
    supplied: Alternates,
) -> Result<Alternates, String> {
    let Some(schema) = store
        .schema_manager()
        .get_schema_metadata(namespace)
        .map_err(|e| e.to_string())?
    else {
        return Ok(supplied);
    };
    resolve_schema_alternates(&schema, supplied)
}

pub(crate) fn resolve_schema_alternates(
    schema: &Schema,
    mut supplied: Alternates,
) -> Result<Alternates, String> {
    let keys = schema.require_record_keys().map_err(|e| e.to_string())?;
    for (name, def) in &keys.alternates {
        match supplied.get_mut(name) {
            Some(Some(kv)) if kv.range.is_some() => {
                let raw_range = kv.range.as_deref().unwrap_or_default();
                let profile = def.range_profile()?;
                let encoded = profile.encode_value(raw_range)?;
                kv.range = Some(profile.decode_value(&encoded)?);
            }
            None if def.allow_absent => {
                supplied.insert(name.clone(), None);
            }
            Some(_) | None => {}
        }
    }
    Ok(supplied)
}
