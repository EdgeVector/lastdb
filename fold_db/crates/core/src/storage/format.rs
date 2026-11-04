//! Product-owned definitions of the format 12 whole-record home.
//!
//! Creation,
//! admission, namespace opening and native routing share these definitions.
//! LastStore's internal run/block grammar remains owned by LastStore.
//!
//! The record writer composes the existing model codecs through typed inputs
//! and an opaque batch. Domain providers have read-only views and typed
//! writers, assembled once by bind. Raw child constructors stay inside this
//! format; ordinary logical sources cannot open raw namespaces. LastStore
//! keeps physical grammar and authenticated byte-copy/replay admission. No
//! startup key census or runtime format registry is introduced.

pub(crate) mod bind;
pub(crate) mod change_feed;
pub(crate) mod control;
pub(crate) mod field_keys;
pub(crate) mod history;
pub(crate) mod metadata;
pub(crate) mod read;
pub(crate) mod records;
pub(crate) mod schemas;

/// The home format: one stored record per primary key and record-owned files.
pub const HOME_FORMAT: u32 = 12;
/// Exact clear node marker bytes. Preserve spelling and order on extraction.
pub const RECORD_NODE_BYTES: &[u8] = b"format=12\nhome=disk-run\ncas=disk-cas\n";
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
pub(in crate::storage::format) const RETRY_CACHE_DIGEST_BYTES: usize = 32;
/// Fingerprint, complete-request intent and acknowledged revision, in that order.
pub(in crate::storage::format) const RETRY_CACHE_ENTRY_BYTES: usize = 3 * RETRY_CACHE_DIGEST_BYTES;
// The model owns the author-clock wire type and its exact key definition.
pub(in crate::storage::format) use crate::schema::types::author_clock::mutation_author_clock_key;
pub(crate) const SCHEMAS_NAMESPACE: &str = "schemas";
pub(crate) const SCHEMA_STATES_NAMESPACE: &str = "schema_states";
pub(crate) const FIELD_KEYS_NAMESPACE: &str = "field_keys";
pub(in crate::storage::format) const FIELD_KEY_BUNDLE_VERSION: u8 = 1;
pub(crate) const FIELD_KEY_METADATA_VERSION: u8 = 2;
pub(in crate::storage::format) const FIELD_KEY_NODE_DOMAIN: &str = "node";
pub(in crate::storage::format) const FIELD_KEY_METADATA_PREFIX: &str = "fkb:";
pub(in crate::storage::format) const FIELD_KEY_WRAP_PREFIX: &str = "fkw:";
pub(in crate::storage::format) const FIELD_KEY_ID_MAX_BYTES: usize = 1024;
pub(crate) const FIELD_KEY_ROW_MAX_BYTES: u64 = 16 * 1024;
pub(in crate::storage::format) const FIELD_KEY_BUNDLE_MAX_BYTES: u64 = 4096;
pub(crate) const CHANGE_FEED_NAMESPACE: &str = "change_feed";
pub(crate) const CHANGE_FEED_LAST_SEQ_KEY: &str = "last_seq";
pub(in crate::storage::format) const CHANGE_FEED_EVENT_PREFIX: &str = "event:";
pub(in crate::storage::format) const CHANGE_FEED_EVENT_END: &str = "event;";
pub(crate) const CHANGE_FEED_APPEND_EVENTS: usize = 1_024;
pub(crate) const CHANGE_FEED_APPEND_BYTES: usize = 32 * 1024 * 1024;
/// Retained node-local notifications; readers report a gap beyond this window.
pub const CHANGE_FEED_MAX_EVENTS: u64 = 250_000;
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
    &[
        "rk",
        "ak",
        HISTORY_POLICY_FAMILY,
        DELETE_AUDIT_FAMILY,
        "layout",
    ],
    HISTORY_DATA_FAMILIES,
];

pub(crate) const RECORD_FILE_PREFIX: &str = "fb:";
pub(crate) const RETENTION_POLICY_KEY_PREFIX: &str = "\0retention_policy\0";
pub(crate) const NAME_CLAIM_KEY_PREFIX: &str = "\0name_claim\0";
pub(crate) const RETENTION_ANCHOR_KEY_PREFIX: &str = "\0retention_anchor\0";
pub(crate) const RETENTION_HASH_PARTITION_KEY_PREFIX: &str = "\0retention_hash_partition\0";
pub(crate) const RETENTION_AGE_KEY_PREFIX: &str = "\0retention_age\0";
pub(crate) const RETENTION_AGE_LATEST_PREFIX: &str = "\0retention_age_latest\0";

pub(crate) const CURRENT_KEY_FORMAT_KEY: &str = "layout:keys";
pub(crate) const DELETE_REPLAY_CUTOFF_KEY: &str = "layout:delete-replay-cutoff";
pub(crate) const FIELD_HISTORY_KEY: &str = "layout:field-history";
pub(crate) const FIELD_HISTORY_ON: &[u8] = b"on";
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

// Compose the canonical model marker; do not define another wire literal.
pub(in crate::storage) use lastdb_model::field_key_codec::KEY_FORMAT as CURRENT_KEY_FORMAT_VALUE;
