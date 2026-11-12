//! Product-owned definitions of the format 15 whole-record home.

pub(crate) mod metadata;
pub(crate) mod records;
pub(crate) mod schemas;

pub const HOME_FORMAT: u32 = 15;
pub const RECORD_NODE_BYTES: &[u8] = b"format=15\nhome=disk-run\ncas=disk-cas\n";
pub const RETIRED_SHARED_NODE_BYTES: &[u8] =
    b"format=15\nhome=disk-run\ncas=disk-cas\nscope=shared\n";
pub const RETIRED_SHARED_DIR: &str = "disk-private";
pub(in crate::storage) const RECORD_PROFILE: u16 = 14;

pub const NODE_MARKER: &str = "laststore-pack-node-v1";
pub const RUN_DIR: &str = "disk-run";
pub const CAS_DIR: &str = "disk-cas";
pub(in crate::storage) const CREATION_INTENT: &str = "laststore-run-initializing";
pub(in crate::storage) const STAGED_NODE_MARKER: &str = ".laststore-pack-node-v1.tmp";
pub(in crate::storage) const HOME_KEY_LABEL: &[u8] = b"lastdb/run-home/v2";
pub(in crate::storage) const CAS_KEY_LABEL: &[u8] = b"lastdb/run-home/v2/cas";

pub(crate) const MAIN_NAMESPACE: &str = "main";
pub(crate) const METADATA_NAMESPACE: &str = "metadata";
pub(in crate::storage::format) use crate::schema::types::author_clock::mutation_author_clock_key;
pub(crate) const SCHEMAS_NAMESPACE: &str = "schemas";
pub(crate) const SCHEMA_STATES_NAMESPACE: &str = "schema_states";
pub const CAS_COLLECTION: &str = "cas_blobs";
pub(in crate::storage) const RECORDS_COLLECTION: &str = "records";
pub(in crate::storage) const REGISTRY: &str = "__native_home";
pub(in crate::storage) const NAMESPACES: &str = "namespaces\0";

pub(in crate::storage) const CURRENT_FAMILIES: &[&str] = &[
    "rk",
    "ak",
    records::applied_deletes::APPLIED_DELETE_FAMILY,
    records::inline_files::INLINE_FILE_FAMILY,
    DELETE_AUDIT_FAMILY,
];

pub(crate) const RECORD_FILE_PREFIX: &str = "fb:";
pub(crate) const RECORD_FILE_UPLOADED_PREFIX: &str = "fu:";
pub(crate) const PENDING_UPLOAD_PREFIX: &str = "fp:";
pub(crate) const RELEASED_FILE_PREFIX: &str = "fr:";
pub(crate) const RETENTION_POLICY_KEY_PREFIX: &str = "\0retention_policy\0";
pub(crate) const RETENTION_HASH_PARTITION_KEY_PREFIX: &str = "\0retention_hash_partition\0";
pub(crate) const RETENTION_AGE_KEY_PREFIX: &str = "\0retention_age\0";
pub(crate) const RETENTION_AGE_LATEST_PREFIX: &str = "\0retention_age_latest\0";

pub(crate) const CURRENT_KEY_FORMAT_KEY: &str = "layout:keys";
pub(crate) const DELETE_REPLAY_CUTOFF_KEY: &str = "layout:delete-replay-cutoff";
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
pub(in crate::storage) use lastdb_model::field_key_codec::KEY_FORMAT as CURRENT_KEY_FORMAT_VALUE;

pub(crate) mod retired {
    use super::{
        MAIN_NAMESPACE, METADATA_NAMESPACE, RETIRED_SHARED_DIR, RETIRED_SHARED_NODE_BYTES,
    };
    use crate::storage::disk_pack::{map, native_routing};
    use crate::storage::error::{StorageError, StorageResult};
    use laststore::disk_run::home::RunRead;
    use std::path::Path;

    pub(in crate::storage) const JOURNAL_RETIRED: &str = "a portable journal key is retired";
    const METADATA_PREFIXES: &[&str] = &[
        "portable-entry:",
        "portable-inbox:",
        "portable-inbox-index:",
        "portable-progress:",
        "portable-schema:",
        "portable-schema-versions:",
        "portable-replay-state",
        "portable-restore-owner",
        "portable-last-applied-peer",
        "native-shared-schema-scope",
    ];

    pub(in crate::storage) fn refuse_shared_home(root: &Path, marker: &[u8]) -> StorageResult<()> {
        if marker == RETIRED_SHARED_NODE_BYTES
            || std::fs::symlink_metadata(root.join(RETIRED_SHARED_DIR)).is_ok_and(|m| m.is_dir())
        {
            return Err(StorageError::RetiredFormat(
                "the dedicated shared home format is retired",
            ));
        }
        Ok(())
    }

    pub(in crate::storage) fn is_journal_key(namespace: &str, key: &[u8]) -> bool {
        let Ok(key) = std::str::from_utf8(key) else {
            return false;
        };
        match namespace {
            METADATA_NAMESPACE => METADATA_PREFIXES.iter().any(|p| key.starts_with(p)),
            MAIN_NAMESPACE => key.starts_with("ade:"),
            _ => false,
        }
    }

    pub(in crate::storage) fn refuse_journal(main: &mut RunRead) -> StorageResult<()> {
        for (ns, prefix) in METADATA_PREFIXES
            .iter()
            .map(|&p| (METADATA_NAMESPACE, p))
            .chain([(MAIN_NAMESPACE, "ade:")])
        {
            let mut end = prefix.as_bytes().to_vec();
            if let Some(last) = end.last_mut() {
                *last += 1;
            }
            let mut range = main
                .administrative_range(
                    native_routing::collection_for_namespace(ns),
                    prefix.as_bytes(),
                    &end,
                )
                .map_err(map)?;
            if !range.next_page(1).map_err(map)?.records.is_empty() {
                return Err(StorageError::RetiredFormat(JOURNAL_RETIRED));
            }
        }
        Ok(())
    }
}

pub(crate) mod content_key_proof {
    use crate::crypto::at_rest::{
        open_at_rest_bounded, seal_at_rest_raw, AT_REST_ENC_BINARY_PREFIX,
    };
    use crate::storage::StorageError;

    pub(crate) const KEY: &[u8] = b"__content_key_proof_v1";
    pub(crate) use super::METADATA_NAMESPACE as NAMESPACE;
    const VALUE: &[u8] = b"lastdb-content-key-proof-v1";

    pub(crate) fn create(key: &[u8; 32]) -> Result<Vec<u8>, StorageError> {
        seal_at_rest_raw(key, VALUE)
            .map_err(|e| StorageError::EncryptionError(format!("content key proof: {e}")))
    }

    pub(crate) fn verify(key: &[u8; 32], stored: &[u8]) -> Result<(), StorageError> {
        if !stored.starts_with(AT_REST_ENC_BINARY_PREFIX.as_bytes()) {
            return Err(StorageError::EncryptionError(
                "content key proof is not a binary encrypted value".into(),
            ));
        }
        let plain = open_at_rest_bounded(key, stored, 4096)
            .map_err(|e| StorageError::EncryptionError(format!("content key proof: {e}")))?;
        (plain == VALUE).then_some(()).ok_or_else(|| {
            StorageError::EncryptionError(
                "content key proof differs; refusing to replace it".into(),
            )
        })
    }
}

pub(crate) mod bind {
    use super::{
        content_key_proof, control, metadata::Metadata, records::RecordFormat,
        schemas::SchemaFormat, CURRENT_KEY_FORMAT_KEY, DELETE_REPLAY_CUTOFF_KEY, MAIN_NAMESPACE,
        METADATA_NAMESPACE, SCHEMAS_NAMESPACE, SCHEMA_STATES_NAMESPACE,
    };
    use crate::storage::disk_pack::{transaction, PackNamespacedStore, PackRuntime};
    use crate::storage::error::{StorageError, StorageResult};
    use lastdb_model::field_key_codec::current_key_format;
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
        let main = store.open_namespace(MAIN_NAMESPACE).await?;
        let meta_kv = store.open_namespace(METADATA_NAMESPACE).await?;
        let states = store.open_namespace(SCHEMA_STATES_NAMESPACE).await?;
        let schemas = SchemaFormat::new(
            store.open_namespace(SCHEMAS_NAMESPACE).await?,
            states,
            Arc::clone(&runtime),
        );
        let metadata = Metadata::new(meta_kv.clone(), Arc::clone(&runtime));
        transaction::scope(Arc::clone(&runtime), async {
            let keys =
                [CURRENT_KEY_FORMAT_KEY, DELETE_REPLAY_CUTOFF_KEY].map(|k| k.as_bytes().to_vec());
            let controls = main.get_many(keys.into()).await?;
            if controls.len() != 2 {
                return Err(control::invalid("incomplete fence read"));
            }
            if !controls[0].as_deref().is_some_and(current_key_format) {
                return Err(StorageError::InvalidOperation(
                    "the logical key format does not match this node home".into(),
                ));
            }
            control::validate_frozen_replay_floor(controls[0].as_deref(), controls[1].as_deref())?;
            let stored = meta_kv.get(content_key_proof::KEY).await?.ok_or_else(|| {
                StorageError::EncryptionError(
                    "content key proof is missing; this program opens only a home it created"
                        .into(),
                )
            })?;
            content_key_proof::verify(&content_key, &stored)
        })
        .await?;
        let records = RecordFormat::new(main, Arc::clone(&runtime), content_key, metadata.clone());
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
    pub(crate) use super::{CURRENT_KEY_FORMAT_KEY, DELETE_REPLAY_CUTOFF_KEY};
    use crate::storage::{error::StorageResult, StorageError};
    use serde::Deserialize;

    pub(in crate::storage::format) fn invalid(text: impl std::fmt::Display) -> StorageError {
        StorageError::InvalidOperation(format!("delete replay cutoff: {text}"))
    }

    pub(crate) fn validate_frozen_replay_floor(
        marker: Option<&[u8]>,
        cutoff: Option<&[u8]>,
    ) -> StorageResult<u64> {
        #[derive(Deserialize)]
        struct Cutoff {
            v: u8,
            minimum_written_at_nanos: u64,
        }
        if !marker.is_some_and(lastdb_model::field_key_codec::current_key_format) {
            return Err(invalid("the format marker is not the node format"));
        }
        let Some(bytes) = cutoff else { return Ok(0) };
        let floor: Cutoff = serde_json::from_slice(bytes).map_err(invalid)?;
        (floor.v == 1 && floor.minimum_written_at_nanos > 0)
            .then_some(floor.minimum_written_at_nanos)
            .ok_or_else(|| invalid("invalid floor record"))
    }
}

pub(crate) mod read {
    use crate::storage::disk_pack::PackKvStore;
    use crate::storage::error::{StorageError, StorageResult};
    use serde::de::DeserializeOwned;

    pub(crate) type ReadStore = PackKvStore;

    fn parse_item<T: DeserializeOwned>(key: &str, bytes: &[u8]) -> StorageResult<T> {
        serde_json::from_slice(bytes).map_err(|e| {
            StorageError::SerializationError(format!("Failed to deserialize {key}: {e}"))
        })
    }

    fn decode_rows<T: DeserializeOwned>(
        rows: Vec<(Vec<u8>, Vec<u8>)>,
    ) -> StorageResult<Vec<(String, T)>> {
        rows.into_iter()
            .map(|(k, v)| {
                let key = String::from_utf8_lossy(&k).into_owned();
                parse_item(&key, &v).map(|val| (key, val))
            })
            .collect()
    }

    impl PackKvStore {
        pub(crate) async fn get_item<T: DeserializeOwned + Send + Sync>(
            &self,
            key: &str,
        ) -> StorageResult<Option<T>> {
            self.get(key.as_bytes())
                .await?
                .map(|b| parse_item(key, &b))
                .transpose()
        }
        pub(crate) async fn get_items<T: DeserializeOwned + Send + Sync>(
            &self,
            keys: &[String],
        ) -> StorageResult<Vec<Option<T>>> {
            if keys.is_empty() {
                return Ok(Vec::new());
            }
            self.get_many(keys.iter().map(|k| k.as_bytes().to_vec()).collect())
                .await?
                .into_iter()
                .zip(keys)
                .map(|(raw, key)| raw.map(|b| parse_item(key, &b)).transpose())
                .collect()
        }
        pub(crate) async fn list_keys_with_prefix(&self, p: &str) -> StorageResult<Vec<String>> {
            Ok(self
                .scan_prefix(p.as_bytes())
                .await?
                .into_iter()
                .map(|(k, _)| String::from_utf8_lossy(&k).into_owned())
                .collect())
        }
        pub(crate) async fn scan_items_with_prefix<T: DeserializeOwned + Send + Sync>(
            &self,
            p: &str,
        ) -> StorageResult<Vec<(String, T)>> {
            decode_rows(self.scan_prefix(p.as_bytes()).await?)
        }
        pub(crate) async fn scan_items_in_range_paged<T: DeserializeOwned + Send + Sync>(
            &self,
            s: &str,
            e: &str,
            n: usize,
        ) -> StorageResult<Vec<(String, T)>> {
            decode_rows(self.scan_range_paged(s.as_bytes(), e.as_bytes(), n).await?)
        }
    }
}
