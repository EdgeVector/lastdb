//! Encrypted, exact-key retired-authority outcomes and authenticated objects.

use super::schema_store::RetiredReplayProof;
use crate::schema::SchemaError;
use crate::storage::traits::{KvStore, NamespacedStore};
use crate::sync::engine::PinLogRecord;
use crate::sync::snapshot_log::MutationLogSegmentId;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::sync::Arc;

pub const RETIRED_INTENT_NAMESPACE: &str = "retired_schema_intent_outcomes";
pub const RETIRED_SOURCE_NAMESPACE: &str = "retired_schema_intent_sources";
const MAX_OUTCOME_BYTES: usize = 32 * 1024 * 1024;
const MAX_SOURCE_BYTES: usize = 64 * 1024 * 1024;
const MAX_SOURCE_OBJECTS: usize = 1024;

mod pointer;
mod probe;
pub use pointer::read_last_preserved_retired_source_id;
pub use probe::{probe_preserved_retired_intent, PreservedRetiredIntentProof};

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct SourceObjectRef {
    segment: MutationLogSegmentId,
    ciphertext_sha256: String,
    ciphertext_bytes: usize,
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
