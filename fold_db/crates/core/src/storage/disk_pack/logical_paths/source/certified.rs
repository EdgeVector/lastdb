//! A decoded body enters memory only after its full certificate and scope match.

use super::*;

/// The source, full decoded content, and exact requested scope form one proof.
pub(super) fn certified_source(
    sources: &Sources,
    scope: Option<&str>,
    atom: &Atom,
) -> StorageResult<Option<(SourceKey, String)>> {
    let Some(certificate) = sources
        .scoped
        .get(&scoped(scope, atom.uuid()))
        .and_then(|key| sources.atoms.get(key))
    else {
        return Ok(None);
    };
    let raw = crate::storage::disk_pack::native_routing::decode(&certificate.source.key)?;
    let Ok(key) = String::from_utf8(raw) else {
        return Ok(None);
    };
    if certificate.source.collection
        != crate::storage::disk_pack::native_routing::collection_for_key(key.as_bytes())
        || !same_scope(&key, scope)
        || crate::atom::atom_key_codec::body_uuid_of(&key) != Some(atom.uuid())
        || certificate.fingerprint != proof::fingerprint(atom)?
    {
        return Ok(None);
    }
    Ok(Some((certificate.source.clone(), key)))
}

fn same_scope(key: &str, scope: Option<&str>) -> bool {
    let Some(suffix) = lastdb_model::kind_partition::rest_of(key, "atom") else {
        return false;
    };
    let prefix_len = key.len() - suffix.len() - "atom".len() - 1;
    let prefix = scope.map(|scope| format!("{scope}:")).unwrap_or_default();
    key[..prefix_len] == prefix
}
