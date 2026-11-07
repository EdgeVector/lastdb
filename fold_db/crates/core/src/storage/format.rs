//! Product-owned definitions of the format 14 whole-record home.
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
pub(crate) mod metadata;
pub(crate) mod read;
pub(crate) mod records;
pub(crate) mod schemas;

/// The home format: one stored record per primary key and record-owned files.
pub const HOME_FORMAT: u32 = 14;
/// Exact clear node marker bytes. Preserve spelling and order on extraction.
pub const RECORD_NODE_BYTES: &[u8] = b"format=14\nhome=disk-run\ncas=disk-cas\n";
/// Backup profile of this home format.
pub(in crate::storage) const RECORD_PROFILE: u16 = 12;
pub(in crate::storage) const SHARED_RECORD_PROFILE: u16 = 13;
pub const SHARED_NODE_BYTES: &[u8] = b"format=14\nhome=disk-run\ncas=disk-cas\nscope=shared\n";
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
/// Marks a record file row whose bytes are already in the cloud: the node
/// downloaded it on request (design `design-lastdb-blob-files-on-demand-20261107`).
/// The key is this prefix and the same suffix as the row's `fb:` key, in
/// `cas_blobs`, written in the same batch as the row and removed with it. A row
/// without this marker was created locally by a record write. A personal
/// backup also writes it after the service accepts the job that uploaded
/// the version.
pub(crate) const RECORD_FILE_UPLOADED_PREFIX: &str = "fu:";
/// A record file version this device created and has not uploaded yet: `fp:`
/// plus the suffix of its `fb:` key. The next personal backup job uploads it.
pub(crate) const PENDING_UPLOAD_PREFIX: &str = "fp:";
/// A dropped record file version whose cloud object the next personal backup
/// job releases: `fr:` plus the suffix of its `fb:` key.
pub(crate) const RELEASED_FILE_PREFIX: &str = "fr:";
pub(crate) const RETENTION_POLICY_KEY_PREFIX: &str = "\0retention_policy\0";
pub(crate) const NAME_CLAIM_KEY_PREFIX: &str = "\0name_claim\0";
pub(crate) const RETENTION_ANCHOR_KEY_PREFIX: &str = "\0retention_anchor\0";
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

// Compose the canonical model marker; do not define another wire literal.
pub(in crate::storage) use lastdb_model::field_key_codec::KEY_FORMAT as CURRENT_KEY_FORMAT_VALUE;
