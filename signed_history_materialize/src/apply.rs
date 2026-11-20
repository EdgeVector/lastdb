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
/// deletes the record. The namespace's schema must already be declared.
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
    let mutation = Mutation::with_uuid(
        uuid::Uuid::new_v4().to_string(),
        target,
        fields.unwrap_or_default(),
        kind,
        expected,
    );
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
