//! A decoded body enters memory only after its full certificate and scope match.

use super::*;

pub(crate) struct CertifiedAtom {
    pub(crate) atom: Arc<Atom>,
    pub(crate) key: String,
}

/// Resolve one batch against this call's real decoded body receipts.
pub(crate) fn certified_atoms(
    runtime: Option<Arc<PackRuntime>>,
    scope: Option<&str>,
    expected: Vec<Atom>,
) -> StorageResult<Vec<Option<CertifiedAtom>>> {
    let Some(sources) = sources(runtime)? else {
        return Ok(expected.into_iter().map(|_| None).collect());
    };
    let sources = sources.lock().expect("native actual body certificates");
    expected
        .into_iter()
        .map(|atom| {
            let Some((_, key)) = certified_source(&sources, scope, &atom)? else {
                return Ok(None);
            };
            Ok(Some(CertifiedAtom {
                atom: Arc::new(atom),
                key,
            }))
        })
        .collect()
}

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
    let raw = crate::storage::laststore::native_routing::decode(&certificate.source.key)?;
    let Ok(key) = String::from_utf8(raw) else {
        return Ok(None);
    };
    if certificate.source.collection
        != crate::storage::laststore::native_routing::collection_for_key(key.as_bytes())
        || !same_scope(&key, scope)
        || crate::atom::atom_key_codec::body_uuid_of(&key) != Some(atom.uuid())
        || certificate.fingerprint != proof::fingerprint(atom)?
    {
        return Ok(None);
    }
    Ok(Some((certificate.source.clone(), key)))
}

fn same_scope(key: &str, scope: Option<&str>) -> bool {
    let Some(suffix) = crate::kind_partition::rest_of(key, "atom") else {
        return false;
    };
    let prefix_len = key.len() - suffix.len() - "atom".len() - 1;
    let prefix = scope.map(|scope| format!("{scope}:")).unwrap_or_default();
    key[..prefix_len] == prefix
}
