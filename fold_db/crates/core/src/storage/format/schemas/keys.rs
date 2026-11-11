//! Catalog key framing, retention row shapes, delete encoding, and JSON decoding.

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use super::require_name;
use crate::schema::types::KeyValue;
use crate::schema::Schema;
use crate::storage::error::StorageResult;
use crate::storage::StorageError;

pub(crate) const CATALOG_PAGE_ROWS: usize = 256;
pub(crate) const CATALOG_SCAN_END: &[u8] = &[0xff];
pub(crate) const CATALOG_STATE_SCAN_START: &[u8] = &[0x01];

pub(crate) use crate::storage::format::{
    RETENTION_AGE_KEY_PREFIX, RETENTION_AGE_LATEST_PREFIX, RETENTION_HASH_PARTITION_KEY_PREFIX,
    RETENTION_POLICY_KEY_PREFIX,
};

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub(crate) struct SchemaRetentionAgeEntry {
    pub(crate) key: KeyValue,
    pub(crate) written_at: u64,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub(crate) struct SchemaRetentionHashPartition {
    pub(crate) hash: String,
}

#[derive(Clone, Copy)]
pub(crate) enum StateKey {
    State,
    Policy,
}

#[derive(Clone, Copy)]
pub(crate) enum Tracking {
    Age,
    Latest,
    Partition,
}

pub(crate) enum StateDelete<'a> {
    Key {
        schema: &'a str,
        kind: StateKey,
    },
    Selected {
        schema: &'a str,
        kind: Tracking,
        keys: Vec<String>,
    },
}

pub(super) fn encode_deletes(changes: Vec<StateDelete<'_>>) -> StorageResult<Vec<Vec<u8>>> {
    let mut output = Vec::new();
    for change in changes {
        match change {
            StateDelete::Key { schema, kind } => {
                require_name(schema)?;
                let raw = match kind {
                    StateKey::State => schema.to_owned(),
                    StateKey::Policy => retention_policy_key(schema),
                };
                output.push(raw.into_bytes());
            }
            StateDelete::Selected { schema, kind, keys } => {
                require_name(schema)?;
                let prefix = match kind {
                    Tracking::Age => retention_age_prefix(schema),
                    Tracking::Latest => retention_age_latest_prefix(schema),
                    Tracking::Partition => retention_hash_partition_prefix(schema),
                };
                for key in keys {
                    let suffix = key.strip_prefix(&prefix).ok_or_else(invalid_delete)?;
                    let digest = if matches!(kind, Tracking::Age) {
                        let (clock, digest) = suffix.split_once('\0').ok_or_else(invalid_delete)?;
                        if clock.len() != 20
                            || !clock.bytes().all(|byte| byte.is_ascii_digit())
                            || clock.parse::<u64>().is_err()
                        {
                            return Err(invalid_delete());
                        }
                        digest
                    } else {
                        suffix
                    };
                    if digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                        return Err(invalid_delete());
                    }
                    output.push(key.into_bytes());
                }
            }
        }
    }
    Ok(output)
}

fn invalid_delete() -> StorageError {
    StorageError::InvalidOperation("retention delete key differs from its owner/family".into())
}

pub(crate) fn retention_policy_key(schema_name: &str) -> String {
    format!("{RETENTION_POLICY_KEY_PREFIX}{schema_name}")
}

pub(crate) fn retention_fingerprint(value: &str) -> String {
    lastdb_model::hex::sha256_hex(value)
}

pub(crate) fn retention_age_prefix(schema_name: &str) -> String {
    format!(
        "{RETENTION_AGE_KEY_PREFIX}{}\0",
        retention_fingerprint(schema_name)
    )
}

pub(crate) fn retention_age_latest_prefix(schema_name: &str) -> String {
    format!(
        "{RETENTION_AGE_LATEST_PREFIX}{}\0",
        retention_fingerprint(schema_name)
    )
}

pub(crate) fn retention_key_fingerprint(key: &KeyValue) -> String {
    retention_fingerprint(&key.to_storage_key())
}

pub(crate) fn retention_age_key(schema_name: &str, written_at: u64, key: &KeyValue) -> String {
    format!(
        "{}{written_at:020}\0{}",
        retention_age_prefix(schema_name),
        retention_key_fingerprint(key)
    )
}

pub(crate) fn retention_age_latest_key(schema_name: &str, key: &KeyValue) -> String {
    format!(
        "{}{}",
        retention_age_latest_prefix(schema_name),
        retention_key_fingerprint(key)
    )
}

pub(crate) fn retention_hash_partition_prefix(schema_name: &str) -> String {
    format!(
        "{RETENTION_HASH_PARTITION_KEY_PREFIX}{}\0",
        retention_fingerprint(schema_name)
    )
}

pub(crate) fn retention_hash_partition_key(schema_name: &str, hash: &str) -> String {
    format!(
        "{}{}",
        retention_hash_partition_prefix(schema_name),
        retention_fingerprint(hash)
    )
}

pub(crate) fn durable_schema_eq(a: &Schema, b: &Schema) -> bool {
    a == b
}

pub(crate) fn decode_catalog_json<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, String> {
    serde_json::from_slice::<T>(bytes).map_err(|error| error.to_string())
}

pub(crate) fn decode_catalog_schema(bytes: &[u8]) -> Result<Schema, String> {
    decode_catalog_json(bytes)
}
