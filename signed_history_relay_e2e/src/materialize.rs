//! Materialize the restored history into a fresh FoldDB in large native batches,
//! then check it against the generator.

use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::Arc;

use fold_db::record_replacement::RecordTarget;
use fold_db::schema::types::{CasExpectation, KeyValue, Mutation, MutationType};
use mutation_log::{NodeKey, Role, VerifiedHistory};
use serde_json::Value;
use signed_history_materialize::{
    encode_string_field, ensure_hash_schema, open_proof_store, read_field_map, FoldDB, STRING_FIELD,
};

use crate::blob::{record_key, record_text, record_value, NAMESPACE};
use crate::grants::{MEMBER_KEY, MEMBER_NAMESPACE, MEMBER_TEXT};
use crate::identity::random_32;

const BATCH_RECORDS: usize = 256;
const SPOT_CHECKS: u64 = 128;

/// What the restored history must hold after the upload.
pub struct Expect {
    /// Generated bulk records.
    pub bulk: u64,
    /// Member that was revoked after one accepted write.
    pub member: NodeKey,
    /// Node holding the `read` grant that performed the restore.
    pub reader: NodeKey,
}

pub struct Materialized {
    pub store: Arc<FoldDB>,
    pub records: u64,
}

/// Create one record per live winner. The store is fresh, so every write is a create.
pub async fn materialize(dir: &Path, history: &VerifiedHistory) -> Result<Materialized, String> {
    let store = open_proof_store(dir, &random_32()).await?;
    for namespace in [NAMESPACE, MEMBER_NAMESPACE] {
        ensure_hash_schema(&store, namespace).await?;
    }
    let mut batch = Vec::with_capacity(BATCH_RECORDS);
    let mut records = 0u64;
    for ((namespace, key), winner) in history.records() {
        if winner.value.is_empty() {
            continue;
        }
        batch.push(create_mutation(namespace, key, &winner.value)?);
        if batch.len() == BATCH_RECORDS {
            flush(&store, &mut batch, &mut records).await?;
        }
    }
    flush(&store, &mut batch, &mut records).await?;
    Ok(Materialized { store, records })
}

async fn flush(store: &FoldDB, batch: &mut Vec<Mutation>, records: &mut u64) -> Result<(), String> {
    if batch.is_empty() {
        return Ok(());
    }
    let mutations = std::mem::take(batch);
    *records += mutations.len() as u64;
    store
        .mutation_manager()
        .write_mutations_receipt(mutations)
        .await
        .map(|_| ())
        .map_err(|e| format!("write record batch: {e}"))
}

fn create_mutation(schema: &str, key: &str, value: &[u8]) -> Result<Mutation, String> {
    let parsed: Value =
        serde_json::from_slice(value).map_err(|e| format!("record {key} is not JSON: {e}"))?;
    let Value::Object(map) = parsed else {
        return Err(format!("record {key} is not a JSON object"));
    };
    let target = RecordTarget {
        schema: schema.to_owned(),
        key: KeyValue::new(Some(key.to_owned()), None),
        alternates: BTreeMap::new(),
    };
    Ok(Mutation::with_uuid(
        uuid::Uuid::new_v4().to_string(),
        target,
        map.into_iter().collect::<HashMap<String, Value>>(),
        MutationType::Create,
        CasExpectation::Absent,
    ))
}

/// Every history winner equals the generated record, the revoked member's one
/// write survived, the folded grants show the revoke, and sampled keyed reads of
/// FoldDB (first, last and evenly spaced) hold the generated text.
pub async fn verify(
    store: &FoldDB,
    history: &VerifiedHistory,
    expect: &Expect,
) -> Result<(), String> {
    let total = expect.bulk;
    if history.records().len() as u64 != total + 1 {
        return Err(format!(
            "history holds {} records, expected {total} bulk plus the member's write",
            history.records().len()
        ));
    }
    verify_member(store, history, expect).await?;
    for index in 0..total {
        let winner = history
            .records()
            .get(&(NAMESPACE.to_owned(), record_key(index)))
            .ok_or_else(|| format!("history has no record {}", record_key(index)))?;
        if winner.value != record_value(index) {
            return Err(format!(
                "history record {} differs from the source",
                record_key(index)
            ));
        }
    }
    let samples = SPOT_CHECKS.min(total);
    for n in 0..samples {
        let index = if samples > 1 {
            n * (total - 1) / (samples - 1)
        } else {
            0
        };
        let fields = read_field_map(store, NAMESPACE, &record_key(index))
            .await?
            .ok_or_else(|| format!("FoldDB has no record {}", record_key(index)))?;
        let held = fields.get(STRING_FIELD).and_then(Value::as_str);
        if held != Some(record_text(index).as_str()) {
            return Err(format!(
                "FoldDB record {} differs from the source",
                record_key(index)
            ));
        }
    }
    Ok(())
}

async fn verify_member(
    store: &FoldDB,
    history: &VerifiedHistory,
    expect: &Expect,
) -> Result<(), String> {
    let winner = history
        .records()
        .get(&(MEMBER_NAMESPACE.to_owned(), MEMBER_KEY.to_owned()))
        .ok_or("history lost the member's write that preceded its revoke")?;
    if winner.value != encode_string_field(MEMBER_TEXT) {
        return Err("history holds a different value for the member's write".into());
    }
    if winner.creator != expect.member {
        return Err("the member's record was not created by the member's proposal".into());
    }
    if history.grants().role_of(&expect.member).is_some() {
        return Err("the member still holds a role after the revoke".into());
    }
    if history.grants().role_of(&expect.reader) != Some(Role::Read) {
        return Err("the restoring node does not hold the read role".into());
    }
    let fields = read_field_map(store, MEMBER_NAMESPACE, MEMBER_KEY)
        .await?
        .ok_or("FoldDB has no record for the member's write")?;
    if fields.get(STRING_FIELD).and_then(Value::as_str) != Some(MEMBER_TEXT) {
        return Err("FoldDB holds a different value for the member's write".into());
    }
    Ok(())
}
