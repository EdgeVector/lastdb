//! Imported data must use the same exact keys as a fresh native home.
//!
//! The `main` namespace holds the two layout control keys and the key
//! families in [`CURRENT_FAMILIES`].
//! Every native read by key, every write and every restored row passes
//! [`validate`], which refuses every other family.

use super::error::{StorageError, StorageResult};
use super::format::{
    records::{applied_deletes, inline_files},
    CURRENT_FAMILIES, CURRENT_KEY_FORMAT_KEY, DELETE_REPLAY_CUTOFF_KEY, MAIN_NAMESPACE,
};

pub(crate) mod families;

/// The `main` key families of a home.
///
/// - `rk`: one record for each key. `ak`: the alternate key entries.
/// - `ad`: one applied-delete stamp at the erased record's address.
/// - `fi`: the inline file of the record at the same address.
/// - `dellog`: the hard-delete audit rows.
/// - `layout`: the key format marker and the delete replay floor.
pub(crate) fn validate(namespace: &str, key: &[u8]) -> StorageResult<()> {
    if namespace != MAIN_NAMESPACE {
        return Ok(());
    }
    let key = std::str::from_utf8(key).map_err(|_| unsupported("non-UTF-8 main key"))?;
    if matches!(key, CURRENT_KEY_FORMAT_KEY | DELETE_REPLAY_CUTOFF_KEY) {
        return Ok(());
    }
    let family = key.split([':', '\0']).next().unwrap_or_default();
    if !CURRENT_FAMILIES.contains(&family) {
        return Err(unsupported("unknown key family"));
    }
    if family == applied_deletes::DELETE_AGE_FAMILY {
        applied_deletes::validate_age_key(key)?;
    } else if let Some(record) =
        applied_deletes::record_key(key).or_else(|| inline_files::record_key(key))
    {
        let rest = record
            .strip_prefix(lastdb_model::record_space::RECORD_PREFIX)
            .ok_or_else(|| unsupported("invalid applied-delete or inline file record key"))?;
        validate_record(rest)?;
    } else if let Some(rest) = key
        .strip_prefix(lastdb_model::field_key_id::ALTERNATE_ENTRY_PREFIX)
        .or_else(|| key.strip_prefix(lastdb_model::record_space::RECORD_PREFIX))
    {
        validate_record(rest)?;
    }
    Ok(())
}

fn validate_record(rest: &str) -> StorageResult<()> {
    let (field_key, suffix) = rest
        .split_once(':')
        .ok_or_else(|| unsupported("invalid field key"))?;
    validate_field_key(field_key)?;
    validate_suffix(suffix)
}

fn validate_suffix(suffix: &str) -> StorageResult<()> {
    lastdb_model::field_key_codec::decode_hash_range_suffix(suffix)
        .ok_or_else(|| unsupported("invalid field key layout"))?;
    Ok(())
}

fn validate_field_key(field_key: &str) -> StorageResult<()> {
    if !lastdb_model::field_key_id::is_canonical_field_key_id(field_key) {
        return Err(unsupported("invalid field key identity"));
    }
    Ok(())
}

fn unsupported(reason: &str) -> StorageError {
    StorageError::InvalidOperation(format!(
        "unsupported key format: {reason}; start with a fresh home"
    ))
}
