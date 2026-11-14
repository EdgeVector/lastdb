//! Product-owned definitions of the format 15 whole-record home.

pub(crate) mod bind;
pub(crate) mod current_keys;
pub(crate) mod metadata;
pub(crate) mod records;
pub(crate) mod schemas;

pub(crate) use current_keys::families::RECORD_FILE_PREFIX;

use crate::schema::types::{field::HashRangeFilter, KeyValue, SchemaError};
use crate::storage::{error::StorageResult, StorageError};
use lastdb_model::field_key_codec::FieldKeyCodec;
use lastdb_model::field_key_id::ALTERNATE_ENTRY_PREFIX;
use lastdb_model::kind_partition::exact_prefix_bounds;
use lastdb_model::record_space::RECORD_PREFIX;
use lastdb_model::record_value::MAX_RECORD_BYTES;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

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
pub(crate) const CAS_COLLECTION: &str = "cas_blobs";
pub(in crate::storage) const RECORDS_COLLECTION: &str = "records";
pub(in crate::storage) const REGISTRY: &str = "__native_home";
pub(in crate::storage) const NAMESPACES: &str = "namespaces\0";

pub(crate) const RETENTION_POLICY_KEY_PREFIX: &str = "\0retention_policy\0";
pub(crate) const RETENTION_HASH_PARTITION_KEY_PREFIX: &str = "\0retention_hash_partition\0";
pub(crate) const RETENTION_AGE_KEY_PREFIX: &str = "\0retention_age\0";
pub(crate) const RETENTION_AGE_LATEST_PREFIX: &str = "\0retention_age_latest\0";

pub(crate) const CURRENT_KEY_FORMAT_KEY: &str = "layout:keys";
pub(crate) const DELETE_REPLAY_CUTOFF_KEY: &str = "layout:delete-replay-cutoff";
pub(crate) const DELETE_AUDIT_FAMILY: &str = "dellog";
pub(crate) const SCHEMA_RELEASE_ROWS: usize = 256;
pub const MAX_ALTERNATE_ENTRIES: usize = 4096;
pub(in crate::storage) use lastdb_model::field_key_codec::KEY_FORMAT as CURRENT_KEY_FORMAT_VALUE;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordDeleteLedgerEntry {
    pub verb: String,
    pub at: String,
    pub caller: String,
    pub schema: String,
    pub key_fingerprint: String,
    pub records_erased: u64,
    pub file_records_erased: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AlternateEntry {
    pub primary: KeyValue,
    pub matched: KeyValue,
}

#[derive(Clone, Debug)]
pub(crate) struct RecordAddress {
    pub(in crate::storage::format) encoded: String,
    pub(in crate::storage::format) key: KeyValue,
}

impl RecordAddress {
    pub(crate) fn new(
        codec: &FieldKeyCodec,
        space: &str,
        key: &KeyValue,
    ) -> Result<Self, SchemaError> {
        let hash = key
            .hash
            .as_deref()
            .ok_or_else(|| SchemaError::InvalidData("a record key requires a hash".into()))?;
        codec
            .api_record_key(space, hash, key.range.as_deref().unwrap_or_default())
            .map(|encoded| Self {
                encoded,
                key: key.clone(),
            })
            .map_err(|e| SchemaError::InvalidData(format!("invalid record key: {e}")))
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.encoded
    }

    pub(crate) fn into_string(self) -> String {
        self.encoded
    }
}

impl AsRef<str> for RecordAddress {
    fn as_ref(&self) -> &str {
        &self.encoded
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct AlternateAddress {
    pub(in crate::storage::format) encoded: String,
}

impl AlternateAddress {
    pub(crate) fn new(
        codec: &FieldKeyCodec,
        field_key: &str,
        key: &KeyValue,
        primary: &RecordAddress,
    ) -> Result<Self, SchemaError> {
        let prefix = alternate_range_key(codec, field_key, key)?;
        Ok(Self {
            encoded: format!("{prefix}\0{:x}", Sha256::digest(primary.encoded.as_bytes())),
        })
    }

    pub(crate) fn into_string(self) -> String {
        self.encoded
    }
}

fn alternate_range_key(
    codec: &FieldKeyCodec,
    field_key: &str,
    key: &KeyValue,
) -> Result<String, SchemaError> {
    let hash = key.hash.as_deref().unwrap_or_default();
    let range = key.range.as_deref().unwrap_or_default();
    codec
        .api_hash_range_member_prefix(field_key, hash, range)
        .map(|suffix| format!("{ALTERNATE_ENTRY_PREFIX}{suffix}"))
        .map_err(|_| {
            SchemaError::InvalidData("alternate key coordinate has an invalid type".into())
        })
}

pub(crate) fn alternate_bounds(
    codec: &FieldKeyCodec,
    field_key: &str,
    filter: &HashRangeFilter,
) -> Result<(String, String), SchemaError> {
    let invalid = |msg: &str| SchemaError::InvalidData(msg.into());
    let encode = |hash: &str, range: &str| {
        alternate_range_key(
            codec,
            field_key,
            &KeyValue::new(Some(hash.into()), Some(range.into())),
        )
    };
    match filter {
        HashRangeFilter::HashRangeKey { hash, range } => {
            Ok(exact_prefix_bounds(&format!("{}\0", encode(hash, range)?)))
        }
        HashRangeFilter::HashKey(hash) => codec
            .api_hash_range_scan_prefix_for_hash(field_key, hash)
            .map(|prefix| exact_prefix_bounds(&format!("{ALTERNATE_ENTRY_PREFIX}{prefix}")))
            .map_err(|_| invalid("invalid alternate hash")),
        HashRangeFilter::HashRangePrefix { hash, prefix } => {
            codec
                .require_text_range_prefix()
                .map_err(|_| invalid("prefix needs text field"))?;
            Ok(exact_prefix_bounds(&encode(hash, prefix)?))
        }
        HashRangeFilter::HashRangeRange { hash, start, end } => {
            Ok((encode(hash, start)?, encode(hash, end)?))
        }
        HashRangeFilter::HashRangeKeys(_) => Err(invalid(
            "alternate reads require an exact key or a range under one hash",
        )),
    }
}

pub(in crate::storage::format) mod inline_files {
    use crate::storage::{error::StorageResult, KvMutation, StorageError};
    use lastdb_model::record_value::{FileMarker, InlineFile};

    pub(in crate::storage::format) const INLINE_FILE_FAMILY: &str = "fi";

    pub(in crate::storage::format) fn put(
        rec: &str,
        file: &InlineFile,
    ) -> StorageResult<KvMutation> {
        Ok(KvMutation::put(
            address(rec),
            file.encode().map_err(StorageError::invalid)?,
        ))
    }

    pub(in crate::storage::format) fn delete(rec: &str) -> KvMutation {
        KvMutation::delete(address(rec))
    }

    pub(in crate::storage::format) fn address(rec: &str) -> String {
        format!("fi:{rec}")
    }

    pub(in crate::storage::format) fn decode_for(
        bytes: &[u8],
        marker: &FileMarker,
    ) -> StorageResult<InlineFile> {
        let file = InlineFile::decode(bytes).map_err(StorageError::invalid)?;
        (&file.marker == marker).then_some(file).ok_or_else(|| {
            StorageError::invalid("the inline file row differs from its record marker")
        })
    }
}

pub(in crate::storage) fn require_final_record_sizes<'a>(
    max_body_bytes: usize,
    records: impl Iterator<Item = (&'a str, &'a str, usize)>,
) -> StorageResult<()> {
    if max_body_bytes > MAX_RECORD_BYTES {
        for (collection, key, size) in records {
            if collection == RECORDS_COLLECTION
                && key.starts_with(RECORD_PREFIX)
                && size > MAX_RECORD_BYTES
            {
                return Err(StorageError::RecordTooLarge {
                    size,
                    limit: MAX_RECORD_BYTES,
                });
            }
        }
    }
    Ok(())
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
        (plain == VALUE)
            .then_some(())
            .ok_or_else(|| StorageError::EncryptionError("content key proof differs".into()))
    }
}

pub(crate) mod control {
    pub(crate) use super::DELETE_REPLAY_CUTOFF_KEY;
    use crate::storage::{error::StorageResult, StorageError};
    use serde::Deserialize;

    pub(in crate::storage::format) fn invalid(text: impl std::fmt::Display) -> StorageError {
        StorageError::invalid(format!("delete replay cutoff: {text}"))
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
    pub(crate) type ReadStore = crate::storage::disk_pack::PackKvStore;
}
