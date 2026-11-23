//! Upsert or delete each LWW winner of a `VerifiedHistory`.

use std::collections::{BTreeMap, HashMap};

use fold_db::fold_db_core::FoldDB;
use fold_db::record_replacement::RecordTarget;
use fold_db::schema::types::{CasExpectation, KeyValue, Mutation, MutationType};
use mutation_log::{MaterializedRecord, VerifiedHistory};
use serde_json::Value;

use crate::key::key_value;
use crate::read::read_at;

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
}

/// Encode record fields (and non-empty alternates, when present) as UTF-8 JSON bytes.
///
/// When `alternates` is empty, encodes plain `fields` JSON so schemas without
/// alternate keys produce identical bytes. Field names in LastDB cannot contain
/// `:`, so `lastdb:record` and `lastdb:alternates` never collide with user fields.
pub fn encode_record_value(fields: &Value, alternates: &Alternates) -> Result<Vec<u8>, String> {
    if alternates.is_empty() {
        return serde_json::to_vec(fields).map_err(|e| format!("cannot encode record fields: {e}"));
    }
    let mut map = serde_json::Map::with_capacity(2);
    map.insert(RECORD_WRAP_FIELDS.to_owned(), fields.clone());
    let alts = serde_json::to_value(alternates)
        .map_err(|e| format!("cannot encode record alternates: {e}"))?;
    map.insert(RECORD_WRAP_ALTS.to_owned(), alts);
    serde_json::to_vec(&Value::Object(map))
        .map_err(|e| format!("cannot encode record envelope: {e}"))
}

/// Decoded record fields and alternate keys from a history value payload.
pub struct DecodedRecord {
    pub fields: HashMap<String, Value>,
    pub alternates: Alternates,
}

/// Decode non-empty record value JSON into fields and alternate keys.
pub fn decode_record_value(value: &[u8]) -> Result<Option<DecodedRecord>, String> {
    if value.is_empty() {
        return Ok(None);
    }
    let Value::Object(mut map) = serde_json::from_slice::<Value>(value)
        .map_err(|e| format!("record value is not JSON: {e}"))?
    else {
        return Err("record value is not a JSON object".into());
    };
    if map.len() == 2 && map.contains_key(RECORD_WRAP_FIELDS) && map.contains_key(RECORD_WRAP_ALTS)
    {
        let Some(Value::Object(fields_map)) = map.remove(RECORD_WRAP_FIELDS) else {
            return Err("wrapped record fields is not a JSON object".into());
        };
        let alts_val = map.remove(RECORD_WRAP_ALTS).unwrap_or(Value::Null);
        let alternates: Alternates = serde_json::from_value(alts_val)
            .map_err(|e| format!("wrapped record alternates is invalid: {e}"))?;
        return Ok(Some(DecodedRecord {
            fields: fields_map.into_iter().collect(),
            alternates,
        }));
    }
    Ok(Some(DecodedRecord {
        fields: map.into_iter().collect(),
        alternates: BTreeMap::new(),
    }))
}

/// Normalize `supplied` alternates against `namespace`'s declared alternate keys,
/// filling `None` for omitted `allow_absent` keys and canonicalizing range coordinates.
pub fn resolve_alternates(
    store: &FoldDB,
    namespace: &str,
    mut supplied: Alternates,
) -> Result<Alternates, String> {
    let Some(schema) = store
        .schema_manager()
        .get_schema_metadata(namespace)
        .map_err(|e| e.to_string())?
    else {
        return Ok(supplied);
    };
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

/// Make FoldDB hold exactly the winners of `history`.
///
/// A winner already stored with equal fields, equal alternates, and the same
/// authors is left alone. History records are whole-record last-writer-wins, so
/// the winner's writer is both `created_by` and `updated_by` on every node. An
/// empty value deletes the record. A live winner whose namespace has no declared
/// schema fails closed. A tombstone for an undeclared namespace is skipped so a
/// tombstone-only namespace does not block restore.
pub async fn apply_history(store: &FoldDB, history: &VerifiedHistory) -> Result<(), String> {
    for ((namespace, record_key), winner) in history.records() {
        apply_winner(store, namespace, record_key, winner).await?;
    }
    Ok(())
}

async fn apply_winner(
    store: &FoldDB,
    namespace: &str,
    record_key: &str,
    winner: &MaterializedRecord,
) -> Result<(), String> {
    if !schema_declared(store, namespace)? {
        if winner.value.is_empty() {
            return Ok(());
        }
        return Err(format!(
            "schema {namespace} has no declaration in signed history"
        ));
    }
    let key = key_value(store, namespace, record_key)?;
    let current = read_at(store, namespace, &key).await?;
    let decoded = decode_record_value(&winner.value)?;
    let author = winner.author.to_hex();
    let (kind, expected, fields, alternates) = match (&current, decoded) {
        (None, None) => return Ok(()),
        (None, Some(rec)) => {
            let alts = resolve_alternates(store, namespace, rec.alternates)?;
            (
                MutationType::Create,
                CasExpectation::Absent,
                rec.fields,
                alts,
            )
        }
        (Some(old), Some(rec)) => {
            let alts = resolve_alternates(store, namespace, rec.alternates)?;
            if old.fields == rec.fields
                && old.alternates == alts
                && old.created_by.as_deref() == Some(author.as_str())
                && old.updated_by.as_deref() == Some(author.as_str())
            {
                return Ok(());
            }
            (MutationType::Replace, revision_of(old), rec.fields, alts)
        }
        (Some(old), None) => (
            MutationType::Delete,
            revision_of(old),
            HashMap::new(),
            old.alternates.clone(),
        ),
    };
    let target = RecordTarget {
        schema: namespace.to_owned(),
        key,
        alternates,
    };
    let carries_authors = !matches!(kind, MutationType::Delete);
    let mut mutation = Mutation::with_uuid(
        uuid::Uuid::new_v4().to_string(),
        target,
        fields,
        kind,
        expected,
    );
    mutation.writer = Some(author.clone());
    if carries_authors {
        mutation.copied_authorship = Some((Some(author.clone()), Some(author)));
    }
    store
        .mutation_manager()
        .write_mutations_receipt(vec![mutation])
        .await
        .map(|_| ())
        .map_err(|e| format!("write {namespace}/{record_key}: {e}"))
}

fn revision_of(stored: &Stored) -> CasExpectation {
    CasExpectation::Revision {
        revision: stored.revision.clone(),
    }
}

fn schema_declared(store: &FoldDB, namespace: &str) -> Result<bool, String> {
    Ok(store
        .schema_manager()
        .get_schema_metadata(namespace)
        .map_err(|e| e.to_string())?
        .is_some())
}

/// Make the store hold what the history holds at each of `keys`: the winner's
/// record, or no record when no write counts there.
///
/// This is the one way a store is brought back to its history at given keys. It
/// undoes a write whose signed entry could not be signed. A key of an
/// undeclared schema is skipped.
pub async fn reconcile_keys<'a>(
    store: &FoldDB,
    history: &VerifiedHistory,
    keys: impl IntoIterator<Item = &'a (String, String)>,
) -> Result<(), String> {
    for key in keys {
        let (namespace, record_key) = key;
        if !schema_declared(store, namespace)? {
            continue;
        }
        if let Some(winner) = history.records().get(key) {
            apply_winner(store, namespace, record_key, winner).await?;
        } else {
            let at = key_value(store, namespace, record_key)?;
            delete_record(store, namespace, at).await?;
        }
    }
    Ok(())
}

async fn delete_record(store: &FoldDB, namespace: &str, key: KeyValue) -> Result<(), String> {
    let Some(current) = read_at(store, namespace, &key).await? else {
        return Ok(());
    };
    let target = RecordTarget {
        schema: namespace.to_owned(),
        key,
        alternates: current.alternates.clone(),
    };
    let mutation = Mutation::with_uuid(
        uuid::Uuid::new_v4().to_string(),
        target,
        HashMap::new(),
        MutationType::Delete,
        revision_of(&current),
    );
    store
        .mutation_manager()
        .write_mutations_receipt(vec![mutation])
        .await
        .map(|_| ())
        .map_err(|e| format!("delete {namespace}: {e}"))
}
