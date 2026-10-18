//! Invalidate exact slot dependencies before a changed view becomes visible.

use crate::atom::molecule_key_codec;
use crate::resident::CoreWarmSet;
use crate::schema::types::field::build_storage_key;
use crate::storage::laststore::native_routing;

/// Include the base tip affected by generation records and every slot affected
/// by a structural replacement. Dirty model coverage uses the same controls.
pub(in crate::storage::disk_pack) fn expanded_coordinates(
    changes: &[(String, String)],
) -> Vec<(String, String)> {
    let mut sources = changes.to_vec();
    for (collection, key) in changes {
        let Ok(raw) = native_routing::decode(key) else {
            continue;
        };
        if !native_routing::read_candidates(&raw).contains(&collection.as_str()) {
            continue;
        }
        let Ok(raw) = std::str::from_utf8(&raw) else {
            continue;
        };
        let Some((scope, bare, family)) = control_coordinate(raw) else {
            continue;
        };
        let molecule = bare[family.len()..]
            .split([':', '\0'])
            .next()
            .unwrap_or_default();
        if molecule.is_empty() || family == "mgp:v1:" {
            continue;
        }
        let affected = generation_tip(molecule, bare, family)
            .unwrap_or_else(|| molecule_key_codec::molecule_generation_pointer_key(molecule));
        let affected = build_storage_key(scope, &affected);
        sources.extend(super::super::kv::coordinates("main", affected.as_bytes()));
    }
    sources.sort();
    sources.dedup();
    sources
}

pub(in crate::storage::disk_pack) fn invalidate(
    core: &mut CoreWarmSet,
    changes: &[(String, String)],
) {
    core.invalidate_sources(changes);
    for (collection, key) in changes {
        let Ok(raw) = native_routing::decode(key) else {
            core.clear_authorizations();
            continue;
        };
        if !native_routing::read_candidates(&raw).contains(&collection.as_str()) {
            continue;
        }
        let Ok(raw) = std::str::from_utf8(&raw) else {
            core.clear_authorizations();
            continue;
        };
        let Some((scope, bare, family)) = control_coordinate(raw) else {
            continue;
        };
        let molecule = bare[family.len()..]
            .split([':', '\0'])
            .next()
            .unwrap_or_default();
        if molecule.is_empty() {
            core.clear_authorizations();
            continue;
        }
        if matches!(family, "mgp:v1:" | "ref:") {
            core.invalidate_molecule(scope, molecule);
        } else if let Some(base) = generation_tip(molecule, bare, family) {
            let key = build_storage_key(scope, &base);
            core.invalidate_sources(&super::super::kv::coordinates("main", key.as_bytes()));
        } else {
            core.invalidate_molecule(scope, molecule);
        }
    }
}

fn generation_tip(molecule: &str, bare: &str, family: &str) -> Option<String> {
    if family == "mgd:v1:" {
        return molecule_key_codec::molecule_record_key_from_generation_delete_key(molecule, bare);
    }
    let suffix = bare.strip_prefix(&format!("mgr:v1:{molecule}:"))?;
    let (generation, _) = suffix.split_once(':')?;
    molecule_key_codec::molecule_record_key_from_generation_key(molecule, generation, bare)
}

fn control_coordinate(raw: &str) -> Option<(Option<&str>, &str, &str)> {
    let (at, family) = [
        "mk:", "rdel:v2:", "atom:", "mh:", "mgp:v1:", "mgr:v1:", "mgd:v1:", "ref:",
    ]
    .into_iter()
    .filter_map(|family| {
        raw.match_indices(family)
            .find(|(at, _)| *at == 0 || raw.as_bytes()[at - 1] == b':')
            .map(|(at, _)| (at, family))
    })
    .min_by_key(|(at, _)| *at)?;
    if matches!(family, "mk:" | "rdel:v2:" | "atom:" | "mh:") {
        return None;
    }
    let scope = raw[..at]
        .strip_suffix(':')
        .filter(|scope| !scope.is_empty());
    Some((scope, &raw[at..], family))
}
