//! A fallback body keeps its actual copy and full metadata, within one scope.

use super::*;
use std::collections::HashMap;

pub(crate) struct CertifiedAtom {
    pub(crate) atom: Arc<Atom>,
    pub(crate) key: String,
}

/// Resolve one batch against this call's real decoded body receipts.
pub(crate) fn certified_atoms(
    runtime: Option<Arc<PackRuntime>>,
    scope: Option<&str>,
    expected: &[Atom],
) -> StorageResult<Vec<Option<CertifiedAtom>>> {
    let mut out: Vec<_> = expected.iter().map(|_| None).collect();
    let Some(sources) = sources(runtime)? else {
        return Ok(out);
    };
    let sources = sources.lock().expect("native actual body certificates");
    let mut positions: HashMap<_, Vec<_>> = HashMap::new();
    for (position, atom) in expected.iter().enumerate() {
        positions.entry(atom.uuid()).or_default().push(position);
    }
    for facts in sources.atoms.values() {
        let Some(positions) = positions.get(facts.atom.uuid()) else {
            continue;
        };
        let raw = crate::storage::laststore::native_routing::decode(&facts.source.key)?;
        let Ok(key) = String::from_utf8(raw) else {
            continue;
        };
        // A logical body source names its native write collection. A receipt
        // from a legacy collection cannot authorize a different physical copy.
        if facts.source.collection
            != crate::storage::laststore::native_routing::write_collection(key.as_bytes())
        {
            continue;
        }
        if !same_scope(&key, scope) {
            continue;
        }
        for &position in positions {
            if out[position].is_none() && facts.atom.as_ref() == &expected[position] {
                out[position] = Some(CertifiedAtom {
                    atom: Arc::clone(&facts.atom),
                    key: key.clone(),
                });
            }
        }
    }
    Ok(out)
}

fn same_scope(key: &str, scope: Option<&str>) -> bool {
    let Some(suffix) = crate::kind_partition::rest_of(key, "atom") else {
        return false;
    };
    let prefix_len = key.len() - suffix.len() - "atom".len() - 1;
    let prefix = scope.map(|scope| format!("{scope}:")).unwrap_or_default();
    key[..prefix_len] == prefix
}
