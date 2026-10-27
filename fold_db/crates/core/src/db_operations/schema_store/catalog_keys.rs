//! Catalog key framing, retention row shapes and tolerant JSON decoding for
//! [`super::SchemaStore`].

use crate::schema::types::KeyValue;
use crate::schema::Schema;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

/// Maximum backend response for one strict repair catalog read.
pub(super) const REPAIR_SCHEMA_PAGE_ROWS: usize = 32;
/// Schema names are UTF-8, so this byte is above every valid first byte.
pub(super) const REPAIR_SCHEMA_SCAN_END: &[u8] = &[0xff];

/// Reserved internal key prefix for retention policies in `schema_states`.
///
/// Existing schema-state rows remain keyed by the bare schema name. The NUL
/// framed prefix keeps policy records disjoint from valid schema names while
/// retaining one node-local storage namespace.
pub(super) const RETENTION_POLICY_KEY_PREFIX: &str = "\0retention_policy\0";
/// Reserved internal key prefix for retention anchors in `schema_states`.
///
/// One row names the schema that holds the retention state (policy, age index,
/// HashRange partition registry) for an app-qualified schema name. Same
/// framing and rationale as [`RETENTION_POLICY_KEY_PREFIX`].
pub(super) const RETENTION_ANCHOR_KEY_PREFIX: &str = "\0retention_anchor\0";
/// Node-local HashRange partition registry for retained schemas.
///
/// A retention policy does not define the data partitions. Successful writes
/// add their hash to this registry, so a TTL sweep can query each observed
/// partition without a cross-partition product scan.
pub(super) const RETENTION_HASH_PARTITION_KEY_PREFIX: &str = "\0retention_hash_partition\0";
/// Reserved internal key prefix for name-claim records in `schema_states`.
///
/// Same framing and same rationale as [`RETENTION_POLICY_KEY_PREFIX`]: this is
/// node-local operational state about an installed schema, so it lives in the
/// node-local namespace rather than in the published schema artifact.
pub(super) const NAME_CLAIM_KEY_PREFIX: &str = "\0name_claim\0";
pub(super) const RETENTION_AGE_KEY_PREFIX: &str = "\0retention_age\0";
pub(super) const RETENTION_AGE_LATEST_PREFIX: &str = "\0retention_age_latest\0";
pub(super) const SCHEMA_DROP_RECEIPT_KEY_PREFIX: &str = "\0schema_drop_receipt\0";

pub(super) fn schema_drop_receipt_key(schema_name: &str) -> String {
    format!("{SCHEMA_DROP_RECEIPT_KEY_PREFIX}{schema_name}")
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub(super) struct SchemaRetentionAgeEntry {
    pub(super) key: KeyValue,
    pub(super) written_at: u64,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub(super) struct SchemaRetentionHashPartition {
    pub(super) hash: String,
}

/// The schema that holds the retention state for the name the row is keyed by.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub(crate) struct SchemaRetentionAnchor {
    pub(crate) anchor: String,
}

pub(super) fn retention_anchor_key(schema_name: &str) -> String {
    format!("{RETENTION_ANCHOR_KEY_PREFIX}{schema_name}")
}

pub(super) fn retention_policy_key(schema_name: &str) -> String {
    format!("{RETENTION_POLICY_KEY_PREFIX}{schema_name}")
}

pub(super) fn name_claim_key(schema_name: &str) -> String {
    format!("{NAME_CLAIM_KEY_PREFIX}{schema_name}")
}

pub(super) fn retention_fingerprint(value: &str) -> String {
    crate::hex::sha256_hex(value)
}

pub(super) fn retention_age_prefix(schema_name: &str) -> String {
    format!(
        "{RETENTION_AGE_KEY_PREFIX}{}\0",
        retention_fingerprint(schema_name)
    )
}

pub(super) fn retention_age_latest_prefix(schema_name: &str) -> String {
    format!(
        "{RETENTION_AGE_LATEST_PREFIX}{}\0",
        retention_fingerprint(schema_name)
    )
}

pub(super) fn retention_key_fingerprint(key: &KeyValue) -> String {
    retention_fingerprint(&key.to_storage_key())
}

pub(super) fn retention_age_key(schema_name: &str, written_at: u64, key: &KeyValue) -> String {
    format!(
        "{}{written_at:020}\0{}",
        retention_age_prefix(schema_name),
        retention_key_fingerprint(key)
    )
}

pub(super) fn retention_age_latest_key(schema_name: &str, key: &KeyValue) -> String {
    format!(
        "{}{}",
        retention_age_latest_prefix(schema_name),
        retention_key_fingerprint(key)
    )
}

pub(super) fn retention_hash_partition_prefix(schema_name: &str) -> String {
    format!(
        "{RETENTION_HASH_PARTITION_KEY_PREFIX}{}\0",
        retention_fingerprint(schema_name)
    )
}

pub(super) fn retention_hash_partition_key(schema_name: &str, hash: &str) -> String {
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
pub(super) fn durable_schema_eq(a: &Schema, b: &Schema) -> bool {
    a == b
}

/// Decode a catalog value. Plain JSON is the only catalog row form.
/// Never panics; callers must not turn a decode miss into a 400 that
/// bricks list/query or boot.
pub(super) fn decode_catalog_json<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, String> {
    serde_json::from_slice::<T>(bytes).map_err(|error| error.to_string())
}

pub(super) fn decode_catalog_schema(bytes: &[u8]) -> Result<Schema, String> {
    decode_catalog_json(bytes)
}
