//! Upsert or delete each LWW winner of a `VerifiedHistory`.

use std::collections::{BTreeMap, HashMap};

use fold_db::fold_db_core::FoldDB;
use fold_db::record_replacement::RecordTarget;
use fold_db::schema::types::{CasExpectation, KeyValue, Mutation, MutationType};
use mutation_log::{MaterializedRecord, VerifiedHistory};
use serde_json::Value;

use crate::key::key_value;
use crate::read::read_at;

pub(crate) struct Stored {
    pub(crate) revision: String,
    pub(crate) created_by: Option<String>,
    pub(crate) updated_by: Option<String>,
    pub(crate) fields: HashMap<String, Value>,
}

/// Make FoldDB hold exactly the winners of `history`.
///
/// A winner already stored with equal fields and the same authors is left
/// alone. History records are whole-record last-writer-wins, so the winner's
/// writer is both `created_by` and `updated_by` on every node. An empty value
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
    let key = key_value(store, namespace, record_key)?;
    let current = read_at(store, namespace, &key).await?;
    let fields = decode_fields(&winner.value)?;
    let author = winner.author.to_hex();
    let (kind, expected) = match (&current, &fields) {
        (None, None) => return Ok(()),
        (None, Some(_)) => (MutationType::Create, CasExpectation::Absent),
        (Some(old), Some(new))
            if old.fields == *new
                && old.created_by.as_deref() == Some(author.as_str())
                && old.updated_by.as_deref() == Some(author.as_str()) =>
        {
            return Ok(());
        }
        (Some(old), Some(_)) => (MutationType::Replace, revision_of(old)),
        (Some(old), None) => (MutationType::Delete, revision_of(old)),
    };
    let target = RecordTarget {
        schema: namespace.to_owned(),
        key,
        alternates: BTreeMap::new(),
    };
    let carries_authors = !matches!(kind, MutationType::Delete);
    let mut mutation = Mutation::with_uuid(
        uuid::Uuid::new_v4().to_string(),
        target,
        fields.unwrap_or_default(),
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

/// Make the store hold what the history holds at each of `keys`: the winner's
/// record, or no record when no write counts there.
///
/// This is the one way a store is brought back to its history at given keys. It
/// undoes a write whose signed entry was not confirmed, and a write that a later
/// role reduction voided. A key of an undeclared schema is skipped.
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
        match history.records().get(key) {
            Some(winner) => apply_winner(store, namespace, record_key, winner).await?,
            None => {
                let at = key_value(store, namespace, record_key)?;
                delete_record(store, namespace, at).await?;
            }
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
        alternates: BTreeMap::new(),
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
