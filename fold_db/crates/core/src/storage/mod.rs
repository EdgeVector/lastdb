//! Native local storage modules, error surface, and internal format capabilities.

pub mod disk_backup;
pub(crate) mod disk_cleanup;
pub mod disk_pack;
pub mod error;
pub(crate) mod format;
pub(crate) mod local_file_ref;

pub(crate) use disk_pack::transaction::traits::KvMutation;
pub use error::{config, StorageError};
pub(crate) use format::{content_key_proof, current_keys};
pub use local_file_ref::MAX_RECORD_FILE_BYTES;

pub(crate) mod bind {
    use super::disk_pack::{transaction, PackNamespacedStore, PackRuntime};
    use super::error::{StorageError, StorageResult};
    use super::format::metadata::Metadata;
    use super::format::records::RecordFormat;
    use super::format::schemas::SchemaFormat;
    use super::format::{
        content_key_proof, CURRENT_KEY_FORMAT_KEY, MAIN_NAMESPACE, METADATA_NAMESPACE,
        SCHEMAS_NAMESPACE, SCHEMA_STATES_NAMESPACE,
    };
    use crate::schema::types::FileWriteIssuer;
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
        let main = store.open_namespace(MAIN_NAMESPACE)?;
        let metadata_kv = store.open_namespace(METADATA_NAMESPACE)?;
        let schemas_kv = store.open_namespace(SCHEMAS_NAMESPACE)?;
        let states_kv = store.open_namespace(SCHEMA_STATES_NAMESPACE)?;
        let schemas = SchemaFormat::new(schemas_kv, states_kv, Arc::clone(&runtime));
        let metadata = Metadata::new(metadata_kv.clone(), Arc::clone(&runtime));
        transaction::scope(Arc::clone(&runtime), async {
            let fmt = main.get(CURRENT_KEY_FORMAT_KEY.as_bytes()).await?;
            if !fmt
                .as_deref()
                .is_some_and(lastdb_model::field_key_codec::current_key_format)
            {
                return Err(StorageError::invalid(
                    "the format marker is not the node format",
                ));
            }
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
        };
        runtime.finish_fresh_initialization().await?;
        Ok(BoundFormat {
            runtime,
            schemas,
            records,
            metadata,
        })
    }
}
