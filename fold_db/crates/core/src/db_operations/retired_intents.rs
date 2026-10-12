//! Encrypted, exact-key retired-authority outcomes and authenticated objects.

use super::schema_store::RetiredReplayProof;
use crate::schema::SchemaError;
use crate::storage::traits::{KvStore, NamespacedStore};
use crate::sync::engine::{PinLogRecord, RetiredReplaySource};
use crate::sync::snapshot_log::MutationLogSegmentId;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::sync::Arc;

pub const RETIRED_INTENT_NAMESPACE: &str = "retired_schema_intent_outcomes";
pub const RETIRED_SOURCE_NAMESPACE: &str = "retired_schema_intent_sources";
const MAX_OUTCOME_BYTES: usize = 32 * 1024 * 1024;

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct SourceObjectRef {
    segment: MutationLogSegmentId,
    ciphertext_sha256: String,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct PreservedIntent {
    version: u8,
    outcome: String,
    source_id: String,
    original_record: PinLogRecord,
    record_index: usize,
    transaction_group: bool,
    source_objects: Vec<SourceObjectRef>,
    retirement_proofs: Vec<RetiredReplayProof>,
}

/// The payload is deliberately excluded: changed content must conflict.
pub fn retired_intent_source_id(record: &PinLogRecord) -> String {
    let bytes = crate::canonical::CanonicalWriter::new()
        .field(b"lastdb-retired-intent-v1")
        .field(record.target_id.as_bytes())
        .field(record.target_prefix.as_bytes())
        .field(record.writer_id.as_bytes())
        .u64(record.frontier_after)
        .finish();
    format!("{:x}", Sha256::digest(bytes))
}

pub(crate) struct PreparedRetiredIntent {
    outcomes: Arc<dyn KvStore>,
    objects: Arc<dyn KvStore>,
    outcome_key: Vec<u8>,
    outcome: Vec<u8>,
    object_items: Vec<(Vec<u8>, Vec<u8>)>,
    missing_objects: Vec<(Vec<u8>, Vec<u8>)>,
}

impl PreparedRetiredIntent {
    /// Called under the permanent dependency fence, before cursor advance.
    pub(crate) async fn commit(self) -> Result<(), SchemaError> {
        self.objects
            .batch_put(self.missing_objects)
            .await
            .map_err(storage_error)?;
        self.objects.flush().await.map_err(storage_error)?;
        verify_exact(&self.objects, &self.object_items).await?;
        self.outcomes
            .batch_put(vec![(self.outcome_key.clone(), self.outcome.clone())])
            .await
            .map_err(storage_error)?;
        self.outcomes.flush().await.map_err(storage_error)?;
        verify_exact(&self.outcomes, &[(self.outcome_key, self.outcome)]).await
    }
}

pub(crate) async fn prepare_retired_intent(
    store: &Arc<dyn NamespacedStore>,
    source: &RetiredReplaySource,
    proofs: Vec<RetiredReplayProof>,
) -> Result<PreparedRetiredIntent, SchemaError> {
    if source.objects.is_empty()
        || source.record.target_id != "personal"
        || !source.record.target_prefix.is_empty()
        || source.record.entry.seq != source.record.frontier_after
        || source.record.entry.device_id != source.record.writer_id
    {
        return Err(refused("unsupported retired replay source scope"));
    }
    let source_id = retired_intent_source_id(&source.record);
    let mut object_items = std::collections::BTreeMap::new();
    let mut source_objects = Vec::with_capacity(source.objects.len());
    for object in &source.objects {
        let digest = format!("{:x}", Sha256::digest(&object.ciphertext));
        let key = digest.as_bytes().to_vec();
        if let Some(previous) = object_items.insert(key, object.ciphertext.to_vec()) {
            if previous != object.ciphertext.as_ref() {
                return Err(refused("retired replay source digest has different bytes"));
            }
        }
        source_objects.push(SourceObjectRef {
            segment: object.segment.clone(),
            ciphertext_sha256: digest,
        });
    }
    let value = serde_json::to_value(PreservedIntent {
        version: 1,
        outcome: "retired-authority-preserved".into(),
        source_id: source_id.clone(),
        original_record: source.record.clone(),
        record_index: source.record_index,
        transaction_group: source.transaction_group,
        source_objects,
        retirement_proofs: proofs,
    })
    .map_err(|_| refused("retired intent serialization failed"))?;
    // Value maps use sorted keys, including the original envelope field maps.
    let outcome =
        serde_json::to_vec(&value).map_err(|_| refused("retired intent serialization failed"))?;
    if outcome.len() > MAX_OUTCOME_BYTES {
        return Err(refused("retired intent exceeds the outcome byte cap"));
    }
    let outcomes = store
        .open_namespace(RETIRED_INTENT_NAMESPACE)
        .await
        .map_err(storage_error)?;
    let objects = store
        .open_namespace(RETIRED_SOURCE_NAMESPACE)
        .await
        .map_err(storage_error)?;
    let object_items: Vec<_> = object_items.into_iter().collect();
    let outcome_key = source_id.into_bytes();
    let outcome_items = [(outcome_key.clone(), outcome.clone())];
    let (missing_objects, _) = tokio::try_join!(
        check_conflicts(&objects, &object_items),
        check_conflicts(&outcomes, &outcome_items),
    )?;
    Ok(PreparedRetiredIntent {
        outcomes,
        objects,
        outcome_key,
        outcome,
        object_items,
        missing_objects,
    })
}

async fn strict_values(
    store: &Arc<dyn KvStore>,
    keys: Vec<Vec<u8>>,
) -> Result<Vec<Option<Vec<u8>>>, SchemaError> {
    let (exists, values) = tokio::try_join!(
        store.exists_many(keys.clone()),
        store.get_many(keys.clone()),
    )
    .map_err(storage_error)?;
    if exists.len() != keys.len()
        || values.len() != keys.len()
        || exists
            .iter()
            .zip(&values)
            .any(|(found, value)| *found != value.is_some())
    {
        return Err(refused("retired journal presence or decode differs"));
    }
    Ok(values)
}

async fn check_conflicts(
    store: &Arc<dyn KvStore>,
    items: &[(Vec<u8>, Vec<u8>)],
) -> Result<Vec<(Vec<u8>, Vec<u8>)>, SchemaError> {
    let values = strict_values(store, items.iter().map(|(key, _)| key.clone()).collect()).await?;
    if values
        .iter()
        .zip(items)
        .any(|(value, (_, expected))| value.as_ref().is_some_and(|value| value != expected))
    {
        return Err(refused("retired journal identity has different content"));
    }
    Ok(values
        .into_iter()
        .zip(items)
        .filter_map(|(value, item)| value.is_none().then(|| item.clone()))
        .collect())
}

async fn verify_exact(
    store: &Arc<dyn KvStore>,
    items: &[(Vec<u8>, Vec<u8>)],
) -> Result<(), SchemaError> {
    let values = strict_values(store, items.iter().map(|(key, _)| key.clone()).collect()).await?;
    if values
        .iter()
        .zip(items)
        .any(|(value, (_, expected))| value.as_ref() != Some(expected))
    {
        return Err(refused("retired journal durable readback differs"));
    }
    Ok(())
}

/// Exact-key recovery returns the private journal bytes without a census.
pub async fn read_preserved_retired_intent(
    store: &Arc<dyn NamespacedStore>,
    source_id: &str,
) -> Result<Option<Vec<u8>>, SchemaError> {
    read_exact(store, RETIRED_INTENT_NAMESPACE, source_id).await
}

/// The bytes are the original authenticated ciphertext, never reconstructed JSON.
pub async fn read_preserved_retired_source(
    store: &Arc<dyn NamespacedStore>,
    ciphertext_sha256: &str,
) -> Result<Option<Vec<u8>>, SchemaError> {
    let value = read_exact(store, RETIRED_SOURCE_NAMESPACE, ciphertext_sha256).await?;
    if value
        .as_ref()
        .is_some_and(|bytes| format!("{:x}", Sha256::digest(bytes)) != ciphertext_sha256)
    {
        return Err(refused("retired source ciphertext digest differs"));
    }
    Ok(value)
}

async fn read_exact(
    store: &Arc<dyn NamespacedStore>,
    namespace: &str,
    key: &str,
) -> Result<Option<Vec<u8>>, SchemaError> {
    if key.len() != 64
        || !key
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(refused("invalid retired journal exact key"));
    }
    let kv = store
        .open_namespace(namespace)
        .await
        .map_err(storage_error)?;
    Ok(strict_values(&kv, vec![key.as_bytes().to_vec()])
        .await?
        .pop()
        .flatten())
}

fn storage_error(_: crate::storage::StorageError) -> SchemaError {
    refused("retired journal storage operation failed")
}

fn refused(message: &str) -> SchemaError {
    SchemaError::InvalidData(message.into())
}
