//! Key family validation, retired journal key prefixes, and record file CAS paths.

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
    use lastdb_model::{hex::hex_lower, portable_mutation::Position, record_space::RECORD_PREFIX};
    use sha2::{Digest, Sha256};

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
        let hash = hex_lower(Sha256::digest(record.as_bytes()));
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
