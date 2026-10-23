//! A logical slot depends on every readable tip form and its exact controls.

use crate::atom::{delete_barrier::delete_barrier_key, molecule_key_codec};
use crate::schema::types::field::build_storage_key;

pub(super) fn tip_sources(
    scope: Option<&str>,
    molecule: &str,
    candidates: &[String],
) -> Vec<(String, String)> {
    let mut logical = Vec::with_capacity(candidates.len() * 2 + 2);
    let mut molecules = std::collections::BTreeSet::from([molecule]);
    for candidate in candidates {
        if let Some(uuid) = molecule_key_codec::molecule_uuid_from_storage_key(candidate) {
            molecules.insert(uuid);
        }
        let key = build_storage_key(scope, candidate);
        logical.push(delete_barrier_key(key.as_bytes()));
        logical.push(key);
    }
    for molecule in molecules {
        let control = molecule_key_codec::molecule_generation_pointer_key(molecule);
        logical.push(build_storage_key(scope, &control));
    }
    let mut sources: Vec<_> = logical
        .into_iter()
        .map(|key| super::super::kv::coordinate("main", key.as_bytes()))
        .collect();
    sources.sort();
    sources.dedup();
    sources
}
