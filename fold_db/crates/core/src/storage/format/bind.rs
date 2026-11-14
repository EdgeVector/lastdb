//! Bind physical namespaces into format-level record, schema, and metadata capabilities.

use super::metadata::Metadata;
use super::records::RecordFormat;
use super::schemas::SchemaFormat;
use super::{
    content_key_proof, control, CURRENT_KEY_FORMAT_KEY, DELETE_REPLAY_CUTOFF_KEY, MAIN_NAMESPACE,
    METADATA_NAMESPACE, SCHEMAS_NAMESPACE, SCHEMA_STATES_NAMESPACE,
};
use crate::schema::types::FileWriteIssuer;
use crate::storage::disk_pack::{transaction, PackNamespacedStore, PackRuntime};
use crate::storage::error::{StorageError, StorageResult};
use std::sync::Arc;

pub(crate) struct BoundFormat {
    pub(crate) runtime: Arc<PackRuntime>,
    pub(crate) schemas: SchemaFormat,
    pub(crate) records: RecordFormat,
    pub(crate) metadata: Metadata,
}

pub(crate) async fn open(
    store: PackNamespacedStore,
    content_key: [u8; 32],
) -> StorageResult<BoundFormat> {
    let runtime = Arc::clone(&store.runtime);
    let (main, metadata_kv, schemas_kv, states_kv) = tokio::try_join!(
        store.open_namespace(MAIN_NAMESPACE),
        store.open_namespace(METADATA_NAMESPACE),
        store.open_namespace(SCHEMAS_NAMESPACE),
        store.open_namespace(SCHEMA_STATES_NAMESPACE),
    )?;
    let schemas = SchemaFormat::new(schemas_kv, states_kv, Arc::clone(&runtime));
    let metadata = Metadata::new(metadata_kv.clone(), Arc::clone(&runtime));
    transaction::scope(Arc::clone(&runtime), async {
        let [fmt_key, cutoff_key] = [CURRENT_KEY_FORMAT_KEY, DELETE_REPLAY_CUTOFF_KEY];
        let rows = main
            .get_many(vec![
                fmt_key.as_bytes().to_vec(),
                cutoff_key.as_bytes().to_vec(),
            ])
            .await?;
        let [fmt, cutoff]: [_; 2] = rows
            .try_into()
            .map_err(|_| control::invalid("incomplete fence read"))?;
        control::validate_frozen_replay_floor(fmt.as_deref(), cutoff.as_deref())?;
        let missing = "content key proof is missing; this program opens only a home it created";
        let stored = metadata_kv.get(content_key_proof::KEY).await?;
        let stored = stored.ok_or_else(|| StorageError::EncryptionError(missing.into()))?;
        content_key_proof::verify(&content_key, &stored)
    })
    .await?;
    let records = RecordFormat {
        main,
        runtime: Arc::clone(&runtime),
        content_key,
        issuer: FileWriteIssuer::default(),
        metadata: metadata.clone(),
    };
    runtime.finish_fresh_initialization().await?;
    Ok(BoundFormat {
        runtime,
        schemas,
        records,
        metadata,
    })
}
