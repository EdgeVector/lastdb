//! Upsert or delete each LWW winner of a `VerifiedHistory`.

use std::collections::{BTreeMap, HashMap};

use fold_db::fold_db_core::FoldDB;
use fold_db::record_replacement::RecordTarget;
use fold_db::schema::types::{CasExpectation, KeyValue, Mutation, MutationType, Schema};
use mutation_log::{MaterializedRecord, VerifiedHistory};
use serde_json::Value;

use crate::key::key_value_for_schema;
use crate::read::read_at;
use crate::validate::validate_record;

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

fn resolve_schema_alternates(
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

/// Make FoldDB hold the valid winners of `history`.
///
/// Winners are applied in deterministic descending `RecordPosition` order. A
/// winner already stored with equal fields, equal alternates, and the same
/// `created_by` and `updated_by` is left alone. An empty value deletes the
/// record. A malformed or schema-invalid winner logs a warning and removes any
/// previously materialized record at that key so every node converges on the
/// same state (record absent), while storage/CAS errors always propagate.
pub async fn apply_history(store: &FoldDB, history: &VerifiedHistory) -> Result<(), String> {
    let mut winners: Vec<_> = history.records().iter().collect();
    winners.sort_by(|(key_a, rec_a), (key_b, rec_b)| {
        let pos_a = (
            rec_a.position.lamport,
            rec_a.position.node_key,
            rec_a.position.device_id,
        );
        let pos_b = (
            rec_b.position.lamport,
            rec_b.position.node_key,
            rec_b.position.device_id,
        );
        pos_b.cmp(&pos_a).then_with(|| key_a.cmp(key_b))
    });
    for ((namespace, record_key), winner) in winners {
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
    let Some(schema) = store
        .schema_manager()
        .get_schema_metadata(namespace)
        .map_err(|e| e.to_string())?
    else {
        if !winner.value.is_empty() {
            tracing::warn!(
                namespace = %namespace,
                "skipping record winner whose schema has no declaration in signed history"
            );
        }
        return Ok(());
    };
    let key = match key_value_for_schema(&schema, namespace, record_key) {
        Ok(key) => key,
        Err(error) => {
            tracing::warn!(
                namespace = %namespace,
                %error,
                "skipping record winner with invalid primary key encoding"
            );
            return Ok(());
        }
    };
    let current = read_at(store, namespace, &key).await?;
    let Some(mutation) = plan_winner(&schema, namespace, &key, current.as_ref(), winner) else {
        return Ok(());
    };
    store
        .mutation_manager()
        .write_mutations_receipt(vec![mutation])
        .await
        .map(|_| ())
        .map_err(|error| format!("write {namespace}/{record_key}: {error}"))
}

fn plan_winner(
    schema: &Schema,
    namespace: &str,
    key: &KeyValue,
    current: Option<&Stored>,
    winner: &MaterializedRecord,
) -> Option<Mutation> {
    if winner.value.is_empty() {
        return current.map(|old| delete_mutation(namespace, key.clone(), old));
    }
    let creator = winner.creator.to_hex();
    let author = winner.author.to_hex();
    let (target, fields) =
        match decode_and_validate(schema, namespace, key, &winner.value, &creator, &author) {
            Ok(valid) => valid,
            Err(reason) => {
                tracing::warn!(
                    namespace = %namespace,
                    %reason,
                    "dropping invalid record winner in signed history"
                );
                return current.map(|old| delete_mutation(namespace, key.clone(), old));
            }
        };
    let (kind, expected) = match current {
        None => (MutationType::Create, CasExpectation::Absent),
        Some(old) => {
            if old.fields == fields
                && old.alternates == target.alternates
                && old.created_by.as_deref() == Some(creator.as_str())
                && old.updated_by.as_deref() == Some(author.as_str())
            {
                return None;
            }
            (MutationType::Replace, revision_of(old))
        }
    };
    let mut mutation = Mutation::with_uuid(
        uuid::Uuid::new_v4().to_string(),
        target,
        fields,
        kind,
        expected,
    );
    mutation.writer = Some(author.clone());
    mutation.copied_authorship = Some((Some(creator), Some(author)));
    Some(mutation)
}

fn decode_and_validate(
    schema: &Schema,
    namespace: &str,
    key: &KeyValue,
    raw_value: &[u8],
    creator: &str,
    author: &str,
) -> Result<(RecordTarget, HashMap<String, Value>), String> {
    let rec = decode_record_value(raw_value)?
        .ok_or_else(|| "live record winner unexpectedly decoded as empty".to_owned())?;
    let alternates = resolve_schema_alternates(schema, rec.alternates)?;
    let target = RecordTarget {
        schema: namespace.to_owned(),
        key: key.clone(),
        alternates,
    };
    validate_record(schema, &target, &rec.fields, creator, author)?;
    Ok((target, rec.fields))
}

fn delete_mutation(namespace: &str, key: KeyValue, stored: &Stored) -> Mutation {
    let target = RecordTarget {
        schema: namespace.to_owned(),
        key,
        alternates: stored.alternates.clone(),
    };
    Mutation::with_uuid(
        uuid::Uuid::new_v4().to_string(),
        target,
        HashMap::new(),
        MutationType::Delete,
        revision_of(stored),
    )
}

fn revision_of(stored: &Stored) -> CasExpectation {
    CasExpectation::Revision {
        revision: stored.revision.clone(),
    }
}

/// Make the store hold what the history holds at each of `keys`: the winner's
/// record, or no record when no write counts there.
pub async fn reconcile_keys<'a>(
    store: &FoldDB,
    history: &VerifiedHistory,
    keys: impl IntoIterator<Item = &'a (String, String)>,
) -> Result<(), String> {
    for key in keys {
        let (namespace, record_key) = key;
        let Some(schema) = store
            .schema_manager()
            .get_schema_metadata(namespace)
            .map_err(|e| e.to_string())?
        else {
            continue;
        };
        if let Some(winner) = history.records().get(key) {
            apply_winner(store, namespace, record_key, winner).await?;
        } else {
            let Ok(at) = key_value_for_schema(&schema, namespace, record_key) else {
                continue;
            };
            delete_record(store, namespace, at).await?;
        }
    }
    Ok(())
}

async fn delete_record(store: &FoldDB, namespace: &str, key: KeyValue) -> Result<(), String> {
    let Some(current) = read_at(store, namespace, &key).await? else {
        return Ok(());
    };
    delete_stored(store, namespace, key, &current).await
}

async fn delete_stored(
    store: &FoldDB,
    namespace: &str,
    key: KeyValue,
    current: &Stored,
) -> Result<(), String> {
    let mutation = delete_mutation(namespace, key, current);
    store
        .mutation_manager()
        .write_mutations_receipt(vec![mutation])
        .await
        .map(|_| ())
        .map_err(|e| format!("delete {namespace}: {e}"))
}
