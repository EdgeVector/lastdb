//! Key shapes of the record families that the typed stores own.
//!
//! The import key check of the storage layer needs these shapes. The stores
//! above it (record files, schema catalog) name the same constants here, so
//! one definition serves both.

/// Key family of record files in `cas_blobs` (design
/// `design-lastdb-no-shared-file-blobs`).
pub(crate) const RECORD_FILE_PREFIX: &str = "fb:";

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

/// The file path of the record at one storage key, scope included:
/// `{scope:}fb:{S}:{sha256(record key)}`. One record has one path, so two
/// records never share a file.
pub(crate) fn record_file_path(record: &str) -> super::StorageResult<String> {
    use sha2::Digest as _;
    let scope = super::exact_scope(record)?;
    let bare = scope.map_or(Some(record), |scope| record.get(scope.len() + 1..));
    let space = bare
        .and_then(|bare| bare.strip_prefix(lastdb_model::record_space::RECORD_PREFIX))
        .and_then(|rest| rest.split(':').next())
        .filter(|space| !space.is_empty())
        .ok_or_else(|| super::unsupported("a record file path needs a record key"))?;
    let digest = lastdb_model::hex::hex_lower(sha2::Sha256::digest(record.as_bytes()));
    let base = format!("{RECORD_FILE_PREFIX}{space}:{digest}");
    Ok(match scope {
        Some(scope) => format!("{scope}:{base}"),
        None => base,
    })
}
