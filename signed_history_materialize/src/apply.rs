//! Upsert or delete each LWW winner of a `VerifiedHistory`.

use std::collections::{BTreeMap, HashMap};

use fold_db::fold_db_core::FoldDB;
use fold_db::record_replacement::RecordTarget;
use fold_db::schema::types::{CasExpectation, KeyValue, Mutation, MutationType};
use mutation_log::{MaterializedRecord, VerifiedHistory};
use serde_json::Value;

use crate::read::read_stored;

pub(crate) struct Stored {
    pub(crate) revision: String,
    pub(crate) fields: HashMap<String, Value>,
}

/// Make FoldDB hold exactly the winners of `history`.
///
/// A winner already stored with equal fields is left alone. An empty value
/// deletes the record. A live winner whose namespace has no declared schema
/// fails closed. A tombstone for an undeclared namespace is skipped so a
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
    let current = read_stored(store, namespace, record_key).await?;
    let fields = decode_fields(&winner.value)?;
    let (kind, expected) = match (&current, &fields) {
        (None, None) => return Ok(()),
        (None, Some(_)) => (MutationType::Create, CasExpectation::Absent),
        (Some(old), Some(new)) if old.fields == *new => return Ok(()),
        (Some(old), Some(_)) => (MutationType::Replace, revision_of(old)),
        (Some(old), None) => (MutationType::Delete, revision_of(old)),
    };
    let target = RecordTarget {
        schema: namespace.to_owned(),
        key: KeyValue::new(Some(record_key.to_owned()), None),
        alternates: BTreeMap::new(),
    };
    let mut mutation = Mutation::with_uuid(
        uuid::Uuid::new_v4().to_string(),
        target,
        fields.unwrap_or_default(),
        kind,
        expected,
    );
    // Content author: signed writer, or the proposal member on incorporation.
    mutation.writer = Some(winner.author.to_hex());
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

fn decode_fields(value: &[u8]) -> Result<Option<HashMap<String, Value>>, String> {
    if value.is_empty() {
        return Ok(None);
    }
    match serde_json::from_slice::<Value>(value) {
        Ok(Value::Object(map)) => Ok(Some(map.into_iter().collect())),
        Ok(_) => Err("record value is not a JSON object".into()),
        Err(e) => Err(format!("record value is not JSON: {e}")),
    }
}

fn schema_declared(store: &FoldDB, namespace: &str) -> Result<bool, String> {
    Ok(store
        .schema_manager()
        .get_schema_metadata(namespace)
        .map_err(|e| e.to_string())?
        .is_some())
}
