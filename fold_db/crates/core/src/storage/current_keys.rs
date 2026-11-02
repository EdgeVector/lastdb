//! Imported data must use the same exact keys as a fresh native home.
//!
//! The `main` namespace holds only the key families in [`CURRENT_FAMILIES`].
//! Every native read by key, every write and every restored row passes
//! [`validate`], which refuses every other family.

use super::error::{StorageError, StorageResult};

pub(crate) mod families;

/// The `main` key families of a home.
///
/// - `rk`: one record for each key. `ak`: the alternate key entries.
/// - `rh`: field history copies of old record values. `ahp`, `ahk`, `ahq`,
///   `ahb`: the history policy of a field, the history count of a key, and the
///   expiry queue of the history entries and its bucket directory.
/// - `rcf`, `rcm`: the kept conflict rows of a record and its conflict marker.
///   `crq`, `crb`: the retention queue of those rows and its bucket directory.
/// - `bref`: the blob counters and the blob dwell queue (`bref:`).
///   `cas_blob`: the blob dwell deletes a `cas_blob:{blob}` row of the `main`
///   namespace with the counter.
/// - `dellog`: the hard-delete audit rows.
/// - `layout`: the key format marker, the delete replay floor and the field
///   history setting.
/// - `keep_small`: the marker of a process that booted past a failed
///   keep-small hydrate.
/// - `idem`: the idempotency keys. They live in the `idempotency`
///   namespace; the family stays so that a scoped key parses.
const CURRENT_FAMILIES: &[&str] = &[
    "rk",
    "ak",
    "rh",
    "ahp",
    "ahk",
    "ahq",
    "ahb",
    "rcf",
    "rcm",
    "crq",
    "crb",
    "bref",
    "cas_blob",
    "dellog",
    "layout",
    "keep_small",
    "idem",
];

/// True for a `main` key of the record lifecycle: a record, an alternate key
/// entry, a history or conflict row, and a blob counter or queue row. Raw
/// replay must never write one part of that lifecycle by itself, so restore
/// validation reads each one back from the logical store.
pub(crate) fn counter_owned(key: &[u8]) -> StorageResult<bool> {
    let key = std::str::from_utf8(key).map_err(|_| unsupported("non-UTF-8 main key"))?;
    let bare = source_key(key)?;
    let family = bare.split([':', '\0']).next().unwrap_or_default();
    Ok(matches!(
        family,
        "ak" | "rk"
            | "rh"
            | "rcf"
            | "rcm"
            | "ahk"
            | "ahp"
            | "ahq"
            | "ahb"
            | "bref"
            | "crq"
            | "crb"
            | "cas_blob"
    ))
}

pub(crate) fn validate(namespace: &str, key: &[u8]) -> StorageResult<()> {
    if namespace != "main" {
        return Ok(());
    }
    let key = std::str::from_utf8(key).map_err(|_| unsupported("non-UTF-8 main key"))?;
    let bare = source_key(key)?;
    let family = bare.split([':', '\0']).next().unwrap_or_default();
    if !CURRENT_FAMILIES.contains(&family) {
        return Err(unsupported("unknown key family"));
    }
    validate_references(bare, family)?;
    validate_retention(bare, family)?;
    if let Some(rest) = bare
        .strip_prefix("ak:")
        .or_else(|| bare.strip_prefix("rk:"))
        .or_else(|| bare.strip_prefix("rcm:"))
    {
        validate_record(rest)?;
    } else if let Some(rest) = bare.strip_prefix("rh:") {
        validate_digest_key(rest, 3, "invalid record history key")?;
    } else if let Some(rest) = bare.strip_prefix("rcf:") {
        // `rcf:{S}:{sha256 hex}`: the conflict row of a record.
        validate_digest_key(rest, 1, "invalid record conflict key")?;
    }
    Ok(())
}

/// The retention queue of kept conflict rows and its bucket directory.
fn validate_retention(key: &str, family: &str) -> StorageResult<()> {
    let current = match family {
        // `crq:{bucket:020}\0{expires:020}:{id}`
        "crq" => key
            .strip_prefix("crq:")
            .and_then(|rest| rest.split_once('\0'))
            .is_some_and(|(bucket, tail)| {
                digits(bucket, 20)
                    && tail
                        .split_once(':')
                        .is_some_and(|(at, id)| digits(at, 20) && !id.is_empty())
            }),
        // `crb\0{bucket:020}`
        // `crb\0params` records the window of the node
        "crb" => key
            .strip_prefix("crb\0")
            .is_some_and(|bucket| digits(bucket, 20) || bucket == "params"),
        _ => true,
    };
    current
        .then_some(())
        .ok_or_else(|| unsupported("invalid retention key format"))
}

fn digits(text: &str, width: usize) -> bool {
    text.len() == width && text.bytes().all(|byte| byte.is_ascii_digit())
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

fn validate_references(key: &str, family: &str) -> StorageResult<()> {
    if family == "bref" && !families::is_blob_reference_key(key) {
        return Err(unsupported("invalid reference key format"));
    }
    Ok(())
}

/// Scope components precede a structural family. The first family owns the
/// complete remainder, so a hash or record placement cannot become another scope.
pub(in crate::storage) fn source_key(key: &str) -> StorageResult<&str> {
    let mut bare = fixed_scope(key).unwrap_or(key);
    loop {
        let family_end = bare.find([':', '\0']).unwrap_or(bare.len());
        let family = &bare[..family_end];
        if main_family(family) {
            let scope_end = key.len() - bare.len();
            if scope_end != 0 {
                validate_scope(&key[..scope_end - 1])?;
            }
            return Ok(bare);
        }
        match bare.as_bytes().get(family_end) {
            Some(b':') => {
                if family.is_empty() {
                    return Err(unsupported("empty storage scope component"));
                }
                let next = &bare[family_end + 1..];
                bare = fixed_scope(next).unwrap_or(next);
            }
            Some(&0) => return Err(unsupported("ambiguous storage scope separator")),
            _ => return Ok(key),
        }
    }
}

/// The exact owner before the first structural family, including nested scopes.
pub(crate) fn exact_scope(key: &str) -> StorageResult<Option<&str>> {
    let bare = source_key(key)?;
    let scope_end = key.len() - bare.len();
    Ok((scope_end != 0).then(|| &key[..scope_end - 1]))
}

/// A scope must remain distinguishable from every native key family.
pub(crate) fn validate_scope(scope: &str) -> StorageResult<()> {
    if hex_identity(scope, 64) {
        return Ok(());
    }
    if let Some(identity) = scope.strip_prefix("cloud:") {
        return if hex_identity(identity, 64) {
            Ok(())
        } else {
            Err(unsupported("invalid cloud storage scope"))
        };
    }
    if let Some(rest) = scope.strip_prefix("from:") {
        let (sender, remainder) = rest.split_once(':').unwrap_or((rest, ""));
        let sender_valid = hex_identity(sender, 32) || hex_identity(sender, 64);
        let cloud_valid = remainder
            .strip_prefix("cloud:")
            .is_some_and(|identity| hex_identity(identity, 64));
        return if sender_valid && (remainder.is_empty() || cloud_valid) {
            Ok(())
        } else {
            Err(unsupported("invalid receive storage scope"))
        };
    }
    if scope.split(':').any(|component| {
        component.is_empty()
            || component.contains('\0')
            || main_family(component)
            || matches!(component, "from" | "cloud")
    }) {
        return Err(unsupported("ambiguous reserved storage scope"));
    }
    Ok(())
}

fn hex_identity(value: &str, width: usize) -> bool {
    value.len() == width && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn fixed_scope(key: &str) -> Option<&str> {
    if let Some((_, bare)) = lastdb_model::kind_partition::split_org_storage_prefix(key) {
        return Some(bare);
    }
    if let Some(rest) = key.strip_prefix("from:") {
        let (sender, bare) = rest.split_once(':')?;
        if hex_identity(sender, 32) || hex_identity(sender, 64) {
            return Some(cloud_scope(bare).unwrap_or(bare));
        }
    }
    cloud_scope(key)
}

fn cloud_scope(key: &str) -> Option<&str> {
    let (identity, bare) = key.strip_prefix("cloud:")?.split_once(':')?;
    hex_identity(identity, 64).then_some(bare)
}

/// A structural family of the `main` namespace. A key names its scope before
/// the first such family.
pub(in crate::storage) fn main_family(family: &str) -> bool {
    CURRENT_FAMILIES.contains(&family)
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
