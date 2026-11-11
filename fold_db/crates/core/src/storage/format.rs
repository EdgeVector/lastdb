//! Product-owned definitions of the format 15 whole-record home.
//!
//! Creation, admission, namespace opening and native routing share these
//! definitions. LastStore's internal run/block grammar remains owned by LastStore.

pub(crate) mod metadata;
pub(crate) mod records;
pub(crate) mod schemas;

/// The home format: one stored record per primary key and record-owned files.
pub const HOME_FORMAT: u32 = 15;
/// Exact clear node marker bytes. Preserve spelling and order on extraction.
pub const RECORD_NODE_BYTES: &[u8] = b"format=15\nhome=disk-run\ncas=disk-cas\n";
/// Backup profile of this home format.
pub(in crate::storage) const RECORD_PROFILE: u16 = 14;
pub(in crate::storage) const SHARED_RECORD_PROFILE: u16 = 15;
pub const SHARED_NODE_BYTES: &[u8] = b"format=15\nhome=disk-run\ncas=disk-cas\nscope=shared\n";
pub(in crate::storage) const PRIVATE_KEY_LABEL: &[u8] = b"lastdb/run-home/v2/private";

/// Clear node marker file inside the data directory.
pub const NODE_MARKER: &str = "laststore-pack-node-v1";
/// Main run home inside the data directory.
pub const RUN_DIR: &str = "disk-run";
/// Record-file run home inside the data directory.
pub const CAS_DIR: &str = "disk-cas";
/// Device-private run home beside the shared record and file homes.
pub(in crate::storage) const PRIVATE_DIR: &str = "disk-private";
pub(in crate::storage) const CREATION_INTENT: &str = "laststore-run-initializing";
pub(in crate::storage) const STAGED_NODE_MARKER: &str = ".laststore-pack-node-v1.tmp";
pub(in crate::storage) const HOME_KEY_LABEL: &[u8] = b"lastdb/run-home/v2";
pub(in crate::storage) const CAS_KEY_LABEL: &[u8] = b"lastdb/run-home/v2/cas";

pub(crate) const MAIN_NAMESPACE: &str = "main";
pub(crate) const METADATA_NAMESPACE: &str = "metadata";
pub(in crate::storage::format) use crate::schema::types::author_clock::mutation_author_clock_key;
pub(crate) const SCHEMAS_NAMESPACE: &str = "schemas";
pub(crate) const SCHEMA_STATES_NAMESPACE: &str = "schema_states";
/// Record files use a separate native home; namespace and collection coincide.
pub const CAS_COLLECTION: &str = "cas_blobs";
/// Every logical main key routes to this native collection.
pub(in crate::storage) const RECORDS_COLLECTION: &str = "records";

pub(in crate::storage) const REGISTRY: &str = "__native_home";
pub(in crate::storage) const NAMESPACES: &str = "namespaces\0";

/// Main families admitted by the current-key check.
pub(in crate::storage) const CURRENT_FAMILIES: &[&str] = &[
    "rk",
    "ak",
    records::applied_deletes::APPLIED_DELETE_FAMILY,
    records::applied_deletes::DELETE_AGE_FAMILY,
    records::inline_files::INLINE_FILE_FAMILY,
    DELETE_AUDIT_FAMILY,
];

pub(crate) const RECORD_FILE_PREFIX: &str = "fb:";
/// Marks a record file row whose bytes are already in the cloud.
pub(crate) const RECORD_FILE_UPLOADED_PREFIX: &str = "fu:";
/// A record file version this device created and has not uploaded yet.
pub(crate) const PENDING_UPLOAD_PREFIX: &str = "fp:";
/// A dropped record file version whose cloud object the next personal backup releases.
pub(crate) const RELEASED_FILE_PREFIX: &str = "fr:";
pub(crate) const RETENTION_POLICY_KEY_PREFIX: &str = "\0retention_policy\0";
pub(crate) const RETENTION_HASH_PARTITION_KEY_PREFIX: &str = "\0retention_hash_partition\0";
pub(crate) const RETENTION_AGE_KEY_PREFIX: &str = "\0retention_age\0";
pub(crate) const RETENTION_AGE_LATEST_PREFIX: &str = "\0retention_age_latest\0";

pub(crate) const CURRENT_KEY_FORMAT_KEY: &str = "layout:keys";
pub(crate) const DELETE_REPLAY_CUTOFF_KEY: &str = "layout:delete-replay-cutoff";
pub(in crate::storage::format) const REPLAY_CUTOFF_VERSION: u8 = 1;
pub(crate) const DELETE_AUDIT_FAMILY: &str = "dellog";
pub(in crate::storage::format) const FILE_KEY_SALT: &[u8] = b"lastdb-file-blob-account-v2";
pub(in crate::storage::format) const FILE_NONCE_SALT: &[u8] = b"lastdb-file-blob-account-v2:nonce";
pub(in crate::storage::format) const FILE_DEK_LABEL: &[u8] = b"file-dek\0";
pub(in crate::storage::format) const FILE_NONCE_LABEL: &[u8] = b"aes-256-gcm-nonce";
pub(in crate::storage::format) use crate::crypto::envelope::{
    ENVELOPE_VERSION as FILE_ENVELOPE_VERSION, NONCE_SIZE as FILE_NONCE_SIZE,
    TAG_SIZE as FILE_TAG_SIZE,
};

pub(crate) const SCHEMA_RELEASE_ROWS: usize = 256;

pub(crate) const CONTENT_KEY_PROOF_KEY: &[u8] = b"__content_key_proof_v1";
pub(in crate::storage) const CONTENT_KEY_PROOF_VALUE: &[u8] = b"lastdb-content-key-proof-v1";
pub(in crate::storage) const CONTENT_KEY_PROOF_MAX_BYTES: u64 = 4096;

pub(in crate::storage) use lastdb_model::field_key_codec::KEY_FORMAT as CURRENT_KEY_FORMAT_VALUE;

pub(crate) mod content_key_proof {
    use crate::crypto::at_rest::{
        open_at_rest_bounded, seal_at_rest_raw, AT_REST_ENC_BINARY_PREFIX,
    };
    use crate::storage::disk_pack::PackNamespacedStore;
    use crate::storage::StorageError;

    pub(crate) use super::CONTENT_KEY_PROOF_KEY as KEY;
    pub(crate) use super::METADATA_NAMESPACE as NAMESPACE;
    use super::{CONTENT_KEY_PROOF_MAX_BYTES, CONTENT_KEY_PROOF_VALUE as VALUE};

    pub(crate) fn create(key: &[u8; 32]) -> Result<Vec<u8>, StorageError> {
        seal_at_rest_raw(key, VALUE)
            .map_err(|error| StorageError::EncryptionError(format!("content key proof: {error}")))
    }

    pub(crate) fn verify(key: &[u8; 32], stored: &[u8]) -> Result<(), StorageError> {
        if !stored.starts_with(AT_REST_ENC_BINARY_PREFIX.as_bytes()) {
            return Err(StorageError::EncryptionError(
                "content key proof is not a binary encrypted value".into(),
            ));
        }
        let plain =
            open_at_rest_bounded(key, stored, CONTENT_KEY_PROOF_MAX_BYTES).map_err(|error| {
                StorageError::EncryptionError(format!("content key proof: {error}"))
            })?;
        if plain != VALUE {
            return Err(StorageError::EncryptionError(
                "content key proof differs; refusing to replace it".into(),
            ));
        }
        Ok(())
    }

    pub(crate) async fn verify_stored(
        store: &PackNamespacedStore,
        key: &[u8; 32],
    ) -> Result<(), StorageError> {
        let kv = store.open_namespace(NAMESPACE).await?;
        match kv.get(KEY).await? {
            Some(stored) => verify(key, &stored),
            None => Err(StorageError::EncryptionError(
                "content key proof is missing; this program opens only a home it created".into(),
            )),
        }
    }
}

pub(crate) mod bind {
    use crate::storage::disk_pack::{transaction, PackNamespacedStore, PackRuntime};
    use crate::storage::error::StorageResult;
    use crate::storage::format::{
        metadata::Metadata, records::RecordFormat, schemas::SchemaFormat, MAIN_NAMESPACE,
        METADATA_NAMESPACE, SCHEMAS_NAMESPACE, SCHEMA_STATES_NAMESPACE,
    };
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
        let runtime = store.disk_pack_runtime();
        let main = store.open_namespace(MAIN_NAMESPACE).await?;
        let metadata = store.open_namespace(METADATA_NAMESPACE).await?;
        let states = store.open_namespace(SCHEMA_STATES_NAMESPACE).await?;
        let schemas = store.open_namespace(SCHEMAS_NAMESPACE).await?;
        let schemas = SchemaFormat::new(schemas, states, Arc::clone(&runtime));
        let metadata = Metadata::new(metadata, Arc::clone(&runtime));
        let records = RecordFormat::new(
            main,
            store.clone(),
            Arc::clone(&runtime),
            content_key,
            metadata.clone(),
        );
        transaction::scope(Arc::clone(&runtime), async {
            records.require_current_key_format().await?;
            crate::storage::content_key_proof::verify_stored(&store, &content_key).await?;
            Ok::<_, crate::storage::StorageError>(())
        })
        .await?;
        runtime.finish_fresh_initialization().await?;
        Ok(BoundFormat {
            schemas,
            records,
            metadata,
            runtime,
        })
    }
}

pub(crate) mod control {
    use crate::storage::error::StorageResult;
    use crate::storage::format::REPLAY_CUTOFF_VERSION;
    use crate::storage::StorageError;
    use serde::{Deserialize, Serialize};

    pub(crate) use crate::storage::format::{CURRENT_KEY_FORMAT_KEY, DELETE_REPLAY_CUTOFF_KEY};

    #[derive(Serialize, Deserialize)]
    pub(crate) struct Cutoff {
        pub(crate) v: u8,
        pub(crate) minimum_written_at_nanos: u64,
    }

    pub(in crate::storage::format) fn invalid(text: impl std::fmt::Display) -> StorageError {
        StorageError::InvalidOperation(format!("delete replay cutoff: {text}"))
    }

    pub(crate) fn validate_frozen_replay_floor(
        marker: Option<&[u8]>,
        cutoff: Option<&[u8]>,
    ) -> StorageResult<u64> {
        if !marker.is_some_and(lastdb_model::field_key_codec::current_key_format) {
            return Err(invalid("the format marker is not the node format"));
        }
        let Some(bytes) = cutoff else {
            return Ok(0);
        };
        let floor: Cutoff = serde_json::from_slice(bytes).map_err(invalid)?;
        if floor.v != REPLAY_CUTOFF_VERSION || floor.minimum_written_at_nanos == 0 {
            return Err(invalid("invalid floor record"));
        }
        Ok(floor.minimum_written_at_nanos)
    }
}

pub(crate) mod read {
    use crate::storage::disk_pack::PackKvStore;
    use crate::storage::error::{StorageError, StorageResult};
    use crate::storage::traits::YieldedScan;
    use serde::de::DeserializeOwned;

    #[derive(Clone)]
    pub(crate) struct ReadStore {
        store: PackKvStore,
    }

    impl ReadStore {
        pub(in crate::storage::format) fn new(store: PackKvStore) -> Self {
            Self { store }
        }

        fn decode_rows<T: DeserializeOwned>(
            results: Vec<(Vec<u8>, Vec<u8>)>,
        ) -> StorageResult<Vec<(String, T)>> {
            let mut items = Vec::with_capacity(results.len());
            for (key_bytes, value_bytes) in results {
                let key = String::from_utf8_lossy(&key_bytes).into_owned();
                let value = serde_json::from_slice(&value_bytes).map_err(|e| {
                    StorageError::SerializationError(format!("Failed to deserialize {key}: {e}"))
                })?;
                items.push((key, value));
            }
            Ok(items)
        }

        pub(crate) async fn get(&self, key: &[u8]) -> StorageResult<Option<Vec<u8>>> {
            self.store.get(key).await
        }

        pub(crate) async fn get_many(
            &self,
            keys: Vec<Vec<u8>>,
        ) -> StorageResult<Vec<Option<Vec<u8>>>> {
            self.store.get_many(keys).await
        }

        pub(crate) async fn scan_prefix(
            &self,
            prefix: &[u8],
        ) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
            self.store.scan_prefix(prefix).await
        }

        pub(crate) async fn scan_range_yielding(
            &self,
            start: &[u8],
            end: &[u8],
            limit: usize,
            future_reads: usize,
        ) -> StorageResult<YieldedScan> {
            self.store
                .scan_range_yielding(start, end, limit, future_reads)
                .await
        }

        pub(crate) async fn scan_range_paged(
            &self,
            start: &[u8],
            end: &[u8],
            limit: usize,
        ) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
            self.store.scan_range_paged(start, end, limit).await
        }

        pub(crate) async fn get_item<T: DeserializeOwned + Send + Sync>(
            &self,
            key: &str,
        ) -> StorageResult<Option<T>> {
            let raw = self.store.get(key.as_bytes()).await?;
            raw.map(|bytes| {
                serde_json::from_slice(&bytes)
                    .map_err(|error| StorageError::SerializationError(error.to_string()))
            })
            .transpose()
        }

        pub(crate) async fn get_items<T: DeserializeOwned + Send + Sync>(
            &self,
            keys: &[String],
        ) -> StorageResult<Vec<Option<T>>> {
            if keys.is_empty() {
                return Ok(Vec::new());
            }
            let primary = keys.iter().map(|key| key.as_bytes().to_vec()).collect();
            let raw = self.store.get_many(primary).await?;
            let mut out = Vec::with_capacity(keys.len());
            for (key, bytes) in keys.iter().zip(raw) {
                match bytes {
                    Some(bytes) => {
                        let item = serde_json::from_slice(&bytes).map_err(|e| {
                            StorageError::SerializationError(format!(
                                "Failed to deserialize {key}: {e}"
                            ))
                        })?;
                        out.push(Some(item));
                    }
                    None => out.push(None),
                }
            }
            Ok(out)
        }

        pub(crate) async fn list_keys_with_prefix(
            &self,
            prefix: &str,
        ) -> StorageResult<Vec<String>> {
            let rows = self.store.scan_prefix(prefix.as_bytes()).await?;
            Ok(rows
                .into_iter()
                .map(|(k, _)| String::from_utf8_lossy(&k).into_owned())
                .collect())
        }

        pub(crate) async fn scan_items_with_prefix<T: DeserializeOwned + Send + Sync>(
            &self,
            prefix: &str,
        ) -> StorageResult<Vec<(String, T)>> {
            let rows = self.store.scan_prefix(prefix.as_bytes()).await?;
            Self::decode_rows(rows)
        }

        pub(crate) async fn scan_items_in_range_paged<T: DeserializeOwned + Send + Sync>(
            &self,
            start: &str,
            end: &str,
            limit: usize,
        ) -> StorageResult<Vec<(String, T)>> {
            Self::decode_rows(
                self.store
                    .scan_range_paged(start.as_bytes(), end.as_bytes(), limit)
                    .await?,
            )
        }
    }
}
