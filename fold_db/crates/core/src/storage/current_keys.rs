//! Imported data must use the same exact keys as a fresh native home.
//!
//! The `main` namespace holds only the key families in [`CURRENT_FAMILIES`].
//! Every native read by key, every write and every restored row passes
//! [`validate`], which refuses every other family.

use super::error::{StorageError, StorageResult};
use super::format::{CURRENT_FAMILIES, MAIN_NAMESPACE};

pub(crate) mod families;

/// The `main` key families of a home.
///
/// - `rk`: one record for each key. `ak`: the alternate key entries.
/// - `rh`: field history copies of old record values. `ahp`, `ahk`, `ahq`,
///   `ahb`: the history policy of a field, the history count of a key, and the
///   expiry queue of the history entries and its bucket directory.
/// - `dellog`: the hard-delete audit rows.
/// - `layout`: the key format marker, the delete replay floor and the field
///   history setting.
pub(crate) fn validate(namespace: &str, key: &[u8]) -> StorageResult<()> {
    if namespace != MAIN_NAMESPACE {
        return Ok(());
    }
    let key = std::str::from_utf8(key).map_err(|_| unsupported("non-UTF-8 main key"))?;
    let family = key.split([':', '\0']).next().unwrap_or_default();
    if !CURRENT_FAMILIES.contains(&family) {
        return Err(unsupported("unknown key family"));
    }
    if let Some(rest) = key
        .strip_prefix(lastdb_model::field_key_id::ALTERNATE_ENTRY_PREFIX)
        .or_else(|| key.strip_prefix(lastdb_model::record_space::RECORD_PREFIX))
    {
        validate_record(rest)?;
    } else if let Some(rest) =
        key.strip_prefix(crate::records::field_history::RECORD_HISTORY_PREFIX)
    {
        validate_digest_key(rest, 3, "invalid record history key")?;
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

/// `rh:{S}:{sha(record key)}:{field identity}:{id}`: a field history entry.
/// `S` is the record space of the schema.
/// `{S}:{digest}...`: a record space, then `count` SHA-256 digests in hex.
fn validate_digest_key(rest: &str, count: usize, reason: &str) -> StorageResult<()> {
    let mut parts = rest.split(':');
    let space = parts.next().unwrap_or_default();
    validate_field_key(space)?;
    let digests: Vec<&str> = parts.collect();
    if digests.len() != count || !digests.iter().all(|part| hex_identity(part, 64)) {
        return Err(unsupported(reason));
    }
    Ok(())
}

fn validate_suffix(suffix: &str) -> StorageResult<()> {
    lastdb_model::field_key_codec::decode_hash_range_suffix(suffix)
        .ok_or_else(|| unsupported("invalid field key layout"))?;
    Ok(())
}

fn hex_identity(value: &str, width: usize) -> bool {
    value.len() == width && value.bytes().all(|byte| byte.is_ascii_hexdigit())
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
