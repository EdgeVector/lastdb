//! Catalog key framing, retention row shapes and JSON decoding for
//! the typed schema format component.

use crate::schema::types::KeyValue;
use crate::schema::Schema;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

/// Rows in one page of a catalog read.
pub(crate) const CATALOG_PAGE_ROWS: usize = 256;
/// Schema names are UTF-8, so this byte is above every valid first byte.
pub(crate) const CATALOG_SCAN_END: &[u8] = &[0xff];
/// The first schema-state key. The node-local families in `schema_states`
/// start with a zero byte, and a schema name never does.
pub(crate) const CATALOG_STATE_SCAN_START: &[u8] = &[0x01];

// Root-owned names are shared by admission, writers and readers.
pub(crate) use crate::storage::format::{
    NAME_CLAIM_KEY_PREFIX, RETENTION_AGE_KEY_PREFIX, RETENTION_AGE_LATEST_PREFIX,
    RETENTION_ANCHOR_KEY_PREFIX, RETENTION_HASH_PARTITION_KEY_PREFIX, RETENTION_POLICY_KEY_PREFIX,
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

/// The schema that holds the retention state for the name the row is keyed by.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub(crate) struct SchemaRetentionAnchor {
    pub(crate) anchor: String,
}

pub(crate) fn retention_anchor_key(schema_name: &str) -> String {
    format!("{RETENTION_ANCHOR_KEY_PREFIX}{schema_name}")
}

pub(crate) fn retention_policy_key(schema_name: &str) -> String {
    format!("{RETENTION_POLICY_KEY_PREFIX}{schema_name}")
}

pub(crate) fn name_claim_key(schema_name: &str) -> String {
    format!("{NAME_CLAIM_KEY_PREFIX}{schema_name}")
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

/// Compare two schemas on their durable catalog payload only.
///
/// `Schema` (`DeclarativeSchemaDefinition`) already implements `PartialEq`
/// that **excludes** runtime-only fields (`runtime_fields`, etc.), so this is
/// exactly the "would this put change durable catalog bytes?" check.
pub(crate) fn durable_schema_eq(a: &Schema, b: &Schema) -> bool {
    a == b
}

/// Decode a catalog value. Plain JSON is the only catalog row form.
pub(crate) fn decode_catalog_json<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, String> {
    serde_json::from_slice::<T>(bytes).map_err(|error| error.to_string())
}

pub(crate) fn decode_catalog_schema(bytes: &[u8]) -> Result<Schema, String> {
    decode_catalog_json(bytes)
}
