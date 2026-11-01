//! Imported data must use the same exact keys as a fresh native home.
//!
//! Every home is format 7. The `main` namespace holds the key families in
//! [`CURRENT_FAMILIES`]. Every native read by key, every write and every
//! restored row passes [`validate`], which refuses a family of an older home
//! ([`RETIRED_FAMILIES`]).

use super::error::{StorageError, StorageResult};

pub(crate) mod families;

/// The `main` key families that a format 7 home writes or reads.
///
/// - `rk`: one record for each key. `ak`: the alternate key entries.
/// - `rh`: field history copies of old record values. `ahp`, `ahk`, `ahq`,
///   `ahb`: the history policy of a field, the history count of a key, and the
///   expiry queue of the history entries and its bucket directory.
/// - `rcf`, `rcm`: the kept conflict rows of a record and its conflict marker.
///   `crq`, `crb`: the retention queue of those rows and its bucket directory.
/// - `bref`: the blob counters and the blob dwell queue (`bref:v3:`).
///   `cas_blob`: the blob dwell deletes a `cas_blob:{blob}` row of the `main`
///   namespace with the counter. No format 7 writer puts one; the family
///   stays so that delete passes the key law.
/// - `dellog`: the hard-delete audit rows.
/// - `layout`: the key format marker and the delete replay floor.
/// - `idem`: the idempotency keys. They live in the `idempotency`
///   namespace; the family stays so that a scoped key parses.
const CURRENT_FAMILIES: &[&str] = &[
    "rk", "ak", "rh", "ahp", "ahk", "ahq", "ahb", "rcf", "rcm", "crq", "crb", "bref", "cas_blob",
    "dellog", "layout", "idem",
];

/// Key families of a format 6 or older home. [`source_key`] still names them,
/// so that a scope parse stops at them, and [`validate`] refuses them.
///
/// Format 6: atom bodies and counters (`atom`), field molecule tips (`mk`),
/// molecule headers (`mh`), generations (`mgp`, `mgr`, `mgd`), delete
/// barriers (`rdel`), binding edges and drain candidates (`mref`), atom file
/// dependencies (`fdep`), conflict caches (`mcc`, `hcu`), history and
/// conflict archive rows (`history`, `conflict`), the slot directory of kept
/// conflict records (`crs`), the field index of atom history (`ahf`) and the
/// schema markers (`schemaidx`). The rest retired before format 6.
const RETIRED_FAMILIES: &[&str] = &[
    "atom",
    "mk",
    "mh",
    "mgp",
    "mgr",
    "mgd",
    "rdel",
    "mref",
    "fdep",
    "mcc",
    "hcu",
    "history",
    "conflict",
    "crs",
    "ahf",
    "schemaidx",
    "tv",
    "tvr",
    "tvr-meta",
    "ref",
    "mord",
    "moc",
    "mhr",
    "mhi",
    "mhk",
    "amigr",
    "schema_atoms",
    "idx",
    "aloc",
    "aref",
    "gcatoms-meta",
    "gcatoms-probe-ref",
    "gcatoms-probe-tv-skip",
    "peer-apply",
];

/// True for a `main` key of the record lifecycle: a record, an alternate key
/// entry, a history or conflict row, a blob counter or queue row, and the
/// pending restore marker. Raw replay must never write one part of that
/// lifecycle by itself, so restore validation reads each one back from the
/// logical store.
pub(crate) fn counter_owned(key: &[u8]) -> StorageResult<bool> {
    let key = std::str::from_utf8(key).map_err(|_| unsupported("non-UTF-8 main key"))?;
    let bare = source_key(key)?;
    if bare == super::disk_pack::restore_safety::PENDING_KEY {
        return Ok(true);
    }
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
    if RETIRED_FAMILIES.contains(&family) {
        return Err(unsupported("retired key family"));
    }
    // `dellog\0…` is the only flat kind a format 7 home writes.
    if lastdb_model::kind_partition::KIND_PARTITION_KINDS.contains(&family)
        && bare.as_bytes().get(family.len()) == Some(&b':')
    {
        return Err(unsupported("retired flat key spelling"));
    }
    if bare.starts_with("bref:v1:") || bare.starts_with("bref:v2:") {
        return Err(unsupported("retired reference key format"));
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
        // `rcf:{S}:{sha256 hex}`: the conflict row of a format 7 record.
        validate_digest_key(rest, 1, "invalid record conflict key")?;
    }
    Ok(())
}

/// The retention queue of kept conflict rows and its bucket directory.
fn validate_retention(key: &str, family: &str) -> StorageResult<()> {
    let current = match family {
        // `crq:v1:{bucket:020}\0{expires:020}:{id}`
        "crq" => key
            .strip_prefix("crq:v1:")
            .and_then(|rest| rest.split_once('\0'))
            .is_some_and(|(bucket, tail)| {
                digits(bucket, 20)
                    && tail
                        .split_once(':')
                        .is_some_and(|(at, id)| digits(at, 20) && !id.is_empty())
            }),
        // `crb:v1\0{bucket:020}`
        // `crb:v1\0params` records the window of the node
        "crb" => key
            .strip_prefix("crb:v1\0")
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
    let (molecule, suffix) = rest
        .split_once(':')
        .ok_or_else(|| unsupported("invalid molecule key"))?;
    validate_molecule(molecule)?;
    validate_suffix(suffix)
}

/// `rh:{S}:{sha(record key)}:{field identity}:{id}`: a field history entry
/// of a format 7 home. `S` is the record space of the schema.
/// `{S}:{digest}...`: a record space, then `count` SHA-256 digests in hex.
fn validate_digest_key(rest: &str, count: usize, reason: &str) -> StorageResult<()> {
    let mut parts = rest.split(':');
    let space = parts.next().unwrap_or_default();
    validate_molecule(space)?;
    let digests: Vec<&str> = parts.collect();
    if digests.len() != count || !digests.iter().all(|part| hex_identity(part, 64)) {
        return Err(unsupported(reason));
    }
    Ok(())
}

fn validate_suffix(suffix: &str) -> StorageResult<()> {
    lastdb_model::molecule_key_codec::decode_hash_range_suffix(suffix)
        .ok_or_else(|| unsupported("retired molecule key layout"))?;
    Ok(())
}

fn validate_references(key: &str, family: &str) -> StorageResult<()> {
    if family == "bref" && !families::is_blob_reference_key(key) {
        return Err(unsupported("invalid reference key format"));
    }
    Ok(())
}

/// Scope components precede a structural family. The first family owns the
/// complete remainder, so a hash or atom placement cannot become another scope.
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

/// A structural family of the `main` namespace: a current one or a retired
/// one. A key names its scope before the first such family.
pub(in crate::storage) fn main_family(family: &str) -> bool {
    CURRENT_FAMILIES.contains(&family) || RETIRED_FAMILIES.contains(&family)
}

fn validate_molecule(molecule: &str) -> StorageResult<()> {
    if !lastdb_model::molecule_uuid::is_canonical_molecule_uuid(molecule) {
        return Err(unsupported("retired molecule identity spelling"));
    }
    Ok(())
}

fn unsupported(reason: &str) -> StorageError {
    StorageError::InvalidOperation(format!(
        "unsupported key format: {reason}; start with a fresh home"
    ))
}
