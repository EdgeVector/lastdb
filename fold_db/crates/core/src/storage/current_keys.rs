//! Imported data must use the same exact keys as a fresh native home.

use super::error::{StorageError, StorageResult};

/// Raw replay must never write one part of the counter lifecycle by itself.
pub(crate) fn counter_owned(namespace: &str, key: &[u8]) -> StorageResult<bool> {
    if namespace != "main" {
        return Ok(counter_namespace_alias(namespace)
            || namespace == "cas_blobs"
            || namespace == "db_catalog"
            || crate::db_operations::schema_store::SchemaStore::is_replay_authority_key(
                namespace, key,
            ));
    }
    let key = std::str::from_utf8(key).map_err(|_| unsupported("non-UTF-8 main key"))?;
    let bare = source_key(key)?;
    if bare == super::disk_pack::restore_safety::PENDING_KEY {
        return Ok(true);
    }
    let family = bare.split([':', '\0']).next().unwrap_or_default();
    Ok(matches!(
        family,
        "ak" | "mk"
            | "mh"
            | "ahf"
            | "ahk"
            | "ahp"
            | "ahq"
            | "ahb"
            | "atom"
            | "history"
            | "conflict"
            | "mcc"
            | "mref"
            | "bref"
            | "crq"
            | "crb"
            | "crs"
            | "rdel"
            | "mgp"
            | "mgr"
            | "mgd"
            | "fdep"
            | "cas_blob"
            | "protein"
            | "molprot"
    ))
}

/// Logical backups retain complete schema/catalog authority. Only physical
/// aliases of the main lifecycle records are unsupported snapshot namespaces.
pub(crate) fn counter_namespace_alias(namespace: &str) -> bool {
    matches!(
        namespace,
        "atoms" | "tips" | "field_tips" | "molecule_ref_edges" | "blob_ref_edges" | "proteins"
    )
}

pub(crate) fn validate(namespace: &str, key: &[u8]) -> StorageResult<()> {
    if namespace != "main" {
        return Ok(());
    }
    let key = std::str::from_utf8(key).map_err(|_| unsupported("non-UTF-8 main key"))?;
    let bare = source_key(key)?;
    let family = bare.split([':', '\0']).next().unwrap_or_default();
    if matches!(
        family,
        "tv" | "tvr"
            | "tvr-meta"
            | "ref"
            | "mord"
            | "moc"
            | "mhr"
            | "mhi"
            | "mhk"
            | "amigr"
            | "schema_atoms"
            | "idx"
            | "aloc"
            | "aref"
            | "gcatoms-meta"
            | "gcatoms-probe-ref"
            | "gcatoms-probe-tv-skip"
            | "peer-apply"
    ) {
        return Err(unsupported("retired key family"));
    }
    if crate::kind_partition::KIND_PARTITION_KINDS.contains(&family)
        && bare.as_bytes().get(family.len()) == Some(&b':')
    {
        let current_compound = match family {
            "atom" => crate::atom::atom_key_codec::uuid_of(bare).is_some(),
            _ => false,
        };
        if !current_compound {
            return Err(unsupported("retired flat key spelling"));
        }
    }
    if bare.starts_with("bref:v1:") || bare.starts_with("bref:v2:") || bare.starts_with("rdel:v1:")
    {
        return Err(unsupported("retired reference or delete key format"));
    }
    validate_references(bare, family)?;
    if family == "fdep" && !super::disk_pack::file_dependencies::is_directory_key(bare) {
        return Err(unsupported("invalid file dependency directory key"));
    }
    validate_retention(bare, family)?;
    if let Some(rest) = bare
        .strip_prefix("mk:")
        .or_else(|| bare.strip_prefix("ak:"))
    {
        validate_record(rest)?;
    } else if let Some(molecule) = bare
        .strip_prefix("mh:")
        .or_else(|| bare.strip_prefix("mcc:"))
        .or_else(|| bare.strip_prefix("molprot:"))
    {
        validate_molecule(molecule)?;
    } else if let Some(rest) = bare.strip_prefix("mgp:v1:") {
        validate_molecule(rest)?;
    } else if let Some(rest) = bare.strip_prefix("mgd:v1:") {
        validate_record(rest)?;
    } else if let Some(rest) = bare.strip_prefix("mgr:v1:") {
        let (molecule, suffix) = rest
            .split_once(':')
            .ok_or_else(|| unsupported("invalid generation key"))?;
        validate_molecule(molecule)?;
        let (generation, suffix) = suffix
            .split_once(':')
            .ok_or_else(|| unsupported("invalid generation key"))?;
        if generation.is_empty() {
            return Err(unsupported("invalid generation identity"));
        }
        validate_suffix(suffix)?;
    } else if matches!(family, "mgp" | "mgr" | "mgd") {
        return Err(unsupported("invalid generation key format"));
    }
    Ok(())
}

/// The retention queue, its bucket directory, and the slot directory of kept
/// conflict records.
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
        "crs" => key
            .strip_prefix("crs:v1:")
            .and_then(|rest| rest.split_once('\0'))
            .is_some_and(|(molecule, _)| validate_molecule(molecule).is_ok()),
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

fn validate_suffix(suffix: &str) -> StorageResult<()> {
    crate::atom::molecule_key_codec::decode_hash_range_suffix(suffix)
        .ok_or_else(|| unsupported("retired molecule key layout"))?;
    Ok(())
}

fn validate_references(key: &str, family: &str) -> StorageResult<()> {
    let current = match family {
        "bref" => crate::db_operations::atom_store::blob_counter::is_current_key(key),
        "mref" => {
            key.strip_prefix("mref:v1:e:").is_some_and(hex_pair)
                || key
                    .strip_prefix("mref:v1:n:")
                    .is_some_and(|target| hex_identity(target, 64))
                || key
                    .strip_prefix("mref:v1:d\0")
                    .is_some_and(crate::db_operations::atom_store::valid_drain_candidate_key)
        }
        _ => true,
    };
    current
        .then_some(())
        .ok_or_else(|| unsupported("invalid reference key format"))
}

fn hex_pair(rest: &str) -> bool {
    rest.split_once('\0')
        .is_some_and(|(target, owner)| hex_identity(target, 64) && hex_identity(owner, 64))
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
    if let Some((_, bare)) = crate::kind_partition::split_org_storage_prefix(key) {
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

pub(in crate::storage) fn main_family(family: &str) -> bool {
    matches!(
        family,
        "atom"
            | "ahf"
            | "ahk"
            | "ahp"
            | "ahq"
            | "ahb"
            | "fdep"
            | "aloc"
            | "aref"
            | "bref"
            | "mref"
            | "ak"
            | "mk"
            | "mh"
            | "mgp"
            | "mgr"
            | "mgd"
            | "rdel"
            | "hcu"
            | "mcc"
            | "tv"
            | "tvr"
            | "tvr-meta"
            | "history"
            | "crq"
            | "crb"
            | "crs"
            | "conflict"
            | "dellog"
            | "schemaidx"
            | "protein"
            | "molprot"
            | "fldprot"
            | "pfq"
            | "cas_blob"
            | "gcatoms-meta"
            | "gcatoms-probe-ref"
            | "gcatoms-probe-tv-skip"
            | "peer-apply"
            | "layout"
            | "idem"
            | "ref"
            | "mord"
            | "moc"
            | "mhr"
            | "mhi"
            | "mhk"
            | "amigr"
            | "schema_atoms"
            | "idx"
    )
}

fn validate_molecule(molecule: &str) -> StorageResult<()> {
    if !crate::atom::molecule_uuid::is_canonical_molecule_uuid(molecule) {
        return Err(unsupported("retired molecule identity spelling"));
    }
    Ok(())
}

fn unsupported(reason: &str) -> StorageError {
    StorageError::InvalidOperation(format!(
        "unsupported key format: {reason}; start with a fresh home"
    ))
}
