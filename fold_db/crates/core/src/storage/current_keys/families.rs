//! Key shapes of the record families that the typed stores own.
//!
//! The import key check of the storage layer needs these shapes. The stores
//! above it (blob counters, schema catalog) name the same constants here, so
//! one definition serves both.

/// Key prefix of one blob counter: `bref:v3:c:{sha256}\0c`.
pub(crate) const BLOB_COUNTER_PREFIX: &str = "bref:v3:c:";
/// Key family of the blob dwell queue entries.
pub(crate) const BLOB_QUEUE_ENTRIES: &str = "bref:v3:q";
/// Key family of the blob dwell queue bucket directory.
pub(crate) const BLOB_QUEUE_DIRECTORY: &str = "bref:v3:d";

/// Reserved internal key prefix for retention policies in `schema_states`.
///
/// Existing schema-state rows remain keyed by the bare schema name. The NUL
/// framed prefix keeps policy records disjoint from valid schema names while
/// retaining one node-local storage namespace.
pub(crate) const RETENTION_POLICY_KEY_PREFIX: &str = "\0retention_policy\0";
/// Reserved internal key prefix for retention anchors in `schema_states`.
///
/// One row names the schema that holds the retention state (policy, age index,
/// HashRange partition registry) for an app-qualified schema name. Same
/// framing and rationale as [`RETENTION_POLICY_KEY_PREFIX`].
pub(crate) const RETENTION_ANCHOR_KEY_PREFIX: &str = "\0retention_anchor\0";
/// Node-local HashRange partition registry for retained schemas.
///
/// A retention policy does not define the data partitions. Successful writes
/// add their hash to this registry, so a TTL sweep can query each observed
/// partition without a cross-partition product scan.
pub(crate) const RETENTION_HASH_PARTITION_KEY_PREFIX: &str = "\0retention_hash_partition\0";
pub(crate) const RETENTION_AGE_KEY_PREFIX: &str = "\0retention_age\0";
pub(crate) const RETENTION_AGE_LATEST_PREFIX: &str = "\0retention_age_latest\0";
pub(crate) const SCHEMA_DROP_RECEIPT_KEY_PREFIX: &str = "\0schema_drop_receipt\0";

/// A blob identity: the 64 hex digits of its SHA-256.
pub(crate) fn is_blob_id(id: &str) -> bool {
    id.len() == 64 && id.bytes().all(|byte| byte.is_ascii_hexdigit())
}

/// True for a counter key, a queue entry key or a queue directory key of the
/// current layout. The key check of every native write and every import calls
/// this for the `bref` family.
#[must_use]
pub(crate) fn is_blob_reference_key(key: &str) -> bool {
    match key.strip_prefix(BLOB_COUNTER_PREFIX) {
        Some(rest) => rest.strip_suffix("\0c").is_some_and(is_blob_id),
        None => is_blob_queue_key(key),
    }
}

/// True for the key of an entry or of a directory row of the blob queue.
fn is_blob_queue_key(key: &str) -> bool {
    if let Some(rest) = key.strip_prefix(&format!("{BLOB_QUEUE_DIRECTORY}\0")) {
        return digits(rest);
    }
    let Some(rest) = key.strip_prefix(&format!("{BLOB_QUEUE_ENTRIES}:")) else {
        return false;
    };
    let Some((bucket, tail)) = rest.split_once('\0') else {
        return false;
    };
    let Some((at, id)) = tail.split_once(':') else {
        return false;
    };
    digits(bucket) && digits(at) && is_blob_id(id)
}

fn digits(text: &str) -> bool {
    text.len() == 20 && text.bytes().all(|byte| byte.is_ascii_digit())
}

/// Only schema cache authority needs replayable bodies. Retention and
/// janitor receipts keep their existing physical capture policy.
pub(crate) fn is_replay_authority_key(namespace: &str, key: &[u8]) -> bool {
    match namespace {
        "schemas" | "schema_superseded_by" => true,
        "schema_states" => !other_schema_metadata_prefixes()
            .iter()
            .any(|prefix| key.starts_with(prefix.as_bytes())),
        _ => false,
    }
}

fn other_schema_metadata_prefixes() -> [&'static str; 6] {
    [
        RETENTION_POLICY_KEY_PREFIX,
        RETENTION_ANCHOR_KEY_PREFIX,
        RETENTION_HASH_PARTITION_KEY_PREFIX,
        RETENTION_AGE_KEY_PREFIX,
        RETENTION_AGE_LATEST_PREFIX,
        SCHEMA_DROP_RECEIPT_KEY_PREFIX,
    ]
}
