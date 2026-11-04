//! Product-owned definitions of the existing format 11 home.
//!
//! Moving a definition here does not change its encoded bytes. Creation,
//! admission, namespace opening and native routing share these definitions.
//! LastStore's internal run/block grammar remains owned by LastStore.
//!
//! The record writer composes the existing model codecs through typed inputs
//! and an opaque batch. Other domain encoders, file/backup sidecars and raw
//! writer handles still need their later migrations. This partial boundary
//! introduces no startup key audit.

pub(crate) mod history;
pub(crate) mod metadata;
pub(crate) mod records;

/// The home format: one stored record per primary key and record-owned files.
pub const HOME_FORMAT: u32 = 11;
/// Exact clear node marker bytes. Preserve spelling and order on extraction.
pub const RECORD_NODE_BYTES: &[u8] = b"format=11\nhome=disk-run\ncas=disk-cas\n";
/// Backup profile of this home format.
pub(in crate::storage) const RECORD_PROFILE: u16 = 9;

/// Clear node marker file inside the data directory.
pub const NODE_MARKER: &str = "laststore-pack-node-v1";
/// Main run home inside the data directory.
pub const RUN_DIR: &str = "disk-run";
/// Record-file run home inside the data directory.
pub const CAS_DIR: &str = "disk-cas";
pub(in crate::storage) const CREATION_INTENT: &str = "laststore-run-initializing";
pub(in crate::storage) const STAGED_NODE_MARKER: &str = ".laststore-pack-node-v1.tmp";
pub(in crate::storage) const HOME_KEY_LABEL: &[u8] = b"lastdb/run-home/v2";
pub(in crate::storage) const CAS_KEY_LABEL: &[u8] = b"lastdb/run-home/v2/cas";

pub(crate) const MAIN_NAMESPACE: &str = "main";
pub(crate) const METADATA_NAMESPACE: &str = "metadata";
pub(in crate::storage::format) const RETRY_CACHE_PREFIX: &str = "retry-cache:";
pub(in crate::storage::format) const RETRY_CACHE_BUCKETS: usize = 256;
pub(in crate::storage::format) const RETRY_CACHE_ENTRIES: usize = 32;
pub(in crate::storage::format) const RETRY_CACHE_BUCKET_BYTES: usize = 8 * 1024;
pub(in crate::storage::format) const RETRY_CACHE_ID_BYTES: usize = 128;
// The model owns the author-clock wire type and its exact key definition.
pub(in crate::storage::format) use crate::schema::types::author_clock::mutation_author_clock_key;
pub(crate) const SCHEMAS_NAMESPACE: &str = "schemas";
pub(crate) const SCHEMA_STATES_NAMESPACE: &str = "schema_states";
pub(crate) const FIELD_KEYS_NAMESPACE: &str = "field_keys";
pub(crate) const CHANGE_FEED_NAMESPACE: &str = "change_feed";
/// Record files use a separate native home; namespace and collection coincide.
pub const CAS_COLLECTION: &str = "cas_blobs";
/// Every logical main key routes to this native collection.
pub(in crate::storage) const RECORDS_COLLECTION: &str = "records";

pub(in crate::storage) const REGISTRY: &str = "__native_home";
pub(in crate::storage) const NAMESPACES: &str = "namespaces\0";

/// History family/configuration facts. Encoders and native admission compose
/// these definitions; readers reuse the same key builders.
pub(in crate::storage::format) use crate::records::field_history::RECORD_HISTORY_FAMILY as HISTORY_RECORD_FAMILY;
pub(in crate::storage::format) const HISTORY_COUNT_FAMILY: &str = "ahk";
pub(in crate::storage::format) const HISTORY_QUEUE_FAMILY: &str = "ahq";
pub(in crate::storage::format) const HISTORY_BUCKET_FAMILY: &str = "ahb";
pub(in crate::storage::format) const HISTORY_POLICY_FAMILY: &str = "ahp";
pub(in crate::storage::format) const HISTORY_SLICE_NANOS: u64 = 3_600_000_000_000;
/// Same bound for a written history entry and its existing restore proof.
pub(in crate::storage) const MAX_HISTORY_ENTRY_BYTES: usize =
    lastdb_model::record_value::MAX_RECORD_BYTES + 4 * 1024;
pub(in crate::storage) const HISTORY_DATA_FAMILIES: &[&str] = &[
    HISTORY_RECORD_FAMILY,
    HISTORY_COUNT_FAMILY,
    HISTORY_QUEUE_FAMILY,
    HISTORY_BUCKET_FAMILY,
];

/// Main families admitted by the existing current-key check. Grouping the
/// history child avoids a second definition of its family names.
pub(in crate::storage) const CURRENT_FAMILIES: &[&[&str]] = &[
    &["rk", "ak", HISTORY_POLICY_FAMILY, "dellog", "layout"],
    HISTORY_DATA_FAMILIES,
];

pub(crate) const RECORD_FILE_PREFIX: &str = "fb:";
pub(crate) const RETENTION_POLICY_KEY_PREFIX: &str = "\0retention_policy\0";
pub(crate) const RETENTION_ANCHOR_KEY_PREFIX: &str = "\0retention_anchor\0";
pub(crate) const RETENTION_HASH_PARTITION_KEY_PREFIX: &str = "\0retention_hash_partition\0";
pub(crate) const RETENTION_AGE_KEY_PREFIX: &str = "\0retention_age\0";
pub(crate) const RETENTION_AGE_LATEST_PREFIX: &str = "\0retention_age_latest\0";
