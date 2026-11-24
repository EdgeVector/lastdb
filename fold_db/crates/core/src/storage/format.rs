//! Product-owned definitions of the format 15 whole-record home.

pub(crate) mod metadata;
pub(crate) mod records;
pub(crate) mod schemas;

pub(crate) use current_keys::families::RECORD_FILE_PREFIX;

use crate::schema::types::{field::HashRangeFilter, KeyValue, SchemaError};
use crate::storage::{error::StorageResult, StorageError};
use lastdb_model::field_key_codec::FieldKeyCodec;
use lastdb_model::field_key_id::ALTERNATE_ENTRY_PREFIX;
use lastdb_model::hex::sha256_hex;
use lastdb_model::kind_partition::exact_prefix_bounds;
use lastdb_model::record_space::RECORD_PREFIX;
use lastdb_model::record_value::MAX_RECORD_BYTES;
use serde::{Deserialize, Serialize};

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
            encoded: format!("{prefix}\0{}", sha256_hex(&primary.encoded)),
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
    use lastdb_model::record_value::InlineFile;

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

    fn address(rec: &str) -> String {
        format!("fi:{rec}")
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

pub(crate) mod current_keys {
    use crate::storage::{error::StorageResult, StorageError};
    use lastdb_model::field_key_codec::decode_hash_range_suffix;
    use lastdb_model::field_key_id::{is_canonical_field_key_id, ALTERNATE_ENTRY_PREFIX};
    use lastdb_model::record_space::RECORD_PREFIX;

    pub(in crate::storage) const JOURNAL_RETIRED: &str = "a portable journal key is retired";
    pub(in crate::storage) const META_PFX: &[&str] = &[
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
    const FAMILIES: &[&str] = &["rk", "ak", "ad", "fi", "dellog"];

    pub(crate) mod families {
        use lastdb_model::{
            hex::sha256_hex, portable_mutation::Position, record_space::RECORD_PREFIX,
        };

        pub(crate) const RECORD_FILE_PREFIX: &str = "fb:";

        pub(crate) fn record_file_path(
            record: &str,
            version: &Position,
        ) -> super::StorageResult<String> {
            let space = record
                .strip_prefix(RECORD_PREFIX)
                .and_then(|rest| rest.split(':').next())
                .filter(|space| !space.is_empty())
                .ok_or_else(|| super::unsupported("a record file path needs a record key"))?;
            let hash = sha256_hex(record);
            Ok(format!(
                "{RECORD_FILE_PREFIX}{space}:{hash}:{}:{:020}",
                version.source_stream, version.sequence
            ))
        }
    }

    pub(crate) fn validate(namespace: &str, key: &[u8]) -> StorageResult<()> {
        let is_journal = std::str::from_utf8(key).is_ok_and(|text| match namespace {
            "metadata" => META_PFX.iter().any(|prefix| text.starts_with(prefix)),
            "main" => text.starts_with("ade:"),
            _ => false,
        });
        if is_journal {
            return Err(StorageError::RetiredFormat(JOURNAL_RETIRED));
        }
        if namespace != "main" {
            return Ok(());
        }
        let key = std::str::from_utf8(key).map_err(|_| unsupported("non-UTF-8 main key"))?;
        if matches!(key, "layout:keys" | "layout:delete-replay-cutoff") {
            return Ok(());
        }
        if !FAMILIES.contains(&key.split([':', '\0']).next().unwrap_or_default()) {
            return Err(unsupported("unknown key family"));
        }
        let rest = if let Some(rec) = key.strip_prefix("ad:").or_else(|| key.strip_prefix("fi:")) {
            Some(
                rec.strip_prefix(RECORD_PREFIX)
                    .ok_or_else(|| unsupported("invalid ad/fi key"))?,
            )
        } else {
            key.strip_prefix(ALTERNATE_ENTRY_PREFIX)
                .or_else(|| key.strip_prefix(RECORD_PREFIX))
        };
        if let Some(rest) = rest {
            let (field_key, suffix) = rest
                .split_once(':')
                .ok_or_else(|| unsupported("invalid field key"))?;
            if !is_canonical_field_key_id(field_key) || decode_hash_range_suffix(suffix).is_none() {
                return Err(unsupported("invalid field key"));
            }
        }
        Ok(())
    }

    fn unsupported(reason: &str) -> StorageError {
        StorageError::invalid(format!(
            "unsupported key format: {reason}; start with a fresh home"
        ))
    }
}

pub(crate) mod read {
    pub(crate) type ReadStore = crate::storage::disk_pack::PackKvStore;
}
