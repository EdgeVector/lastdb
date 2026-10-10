//! Prove no retained source enters the candidate version graph.

use super::super::keys::{classify, mol_key, Class};
use super::super::molset::SpellingMap;
use super::super::walk::walk_both;
use super::super::ReapError;
use super::{abort, SourceSummary, VersionNode};
use fold_db::atom::molecule_key_codec as codec;
use fold_db::db_operations::atom_store::TipVersionBackref;
use fold_db::kind_partition::split_org_storage_prefix;
use fold_db::storage::traits::KvStore;
use serde_json::Value;
use std::collections::BTreeMap;
use std::sync::Arc;

pub(super) fn nonempty_links(plain: &[u8]) -> Result<std::vec::IntoIter<String>, ReapError> {
    let value: Value = serde_json::from_slice(plain)
        .map_err(|error| abort(format!("unsupported source value: {error}")))?;
    let mut pending = vec![value];
    let mut links = Vec::new();
    while let Some(value) = pending.pop() {
        match value {
            Value::Object(mut object) => {
                if let Some(value) = object.remove("prev_tip_id") {
                    let link = value
                        .as_str()
                        .ok_or_else(|| abort("prev_tip_id is not text"))?;
                    if !link.is_empty() {
                        links.push(link.into());
                    }
                }
                pending.extend(object.into_values());
            }
            Value::Array(array) => pending.extend(array),
            _ => {}
        }
    }
    Ok(links.into_iter())
}

// Production order entries are KeyValue, counts are integers, conflict caches
// are identifier lists, generation pointers contain generation ids, and Delete
// barriers contain winner metadata. None of these types embeds AtomEntry.
pub(super) fn non_source_metadata(key: &str) -> bool {
    let bare = bare_key(key);
    matches!(
        classify(bare).class,
        Class::MordColon
            | Class::MordSparse
            | Class::MordAnchored
            | Class::MocColon
            | Class::MocAnchored
            | Class::Mgp
            | Class::Mcc
            | Class::RdelV1
            | Class::RdelV2
    )
}

fn bare_key(key: &str) -> &str {
    let bare = split_org_storage_prefix(key).map_or(key, |(_, rest)| rest);
    bare.strip_prefix("from:")
        .and_then(|rest| rest.split_once(':').map(|(_, rest)| rest))
        .unwrap_or(bare)
}

fn check_backref(
    key: &str,
    plain: &[u8],
    dead: &SpellingMap,
    nodes: &BTreeMap<String, VersionNode>,
    personal_tips: bool,
) -> Result<(), ReapError> {
    let bare = bare_key(key);
    if !bare.starts_with(codec::TIP_VERSION_BACKREF_PREFIX) {
        return Ok(());
    }
    let row: TipVersionBackref = serde_json::from_slice(plain)
        .map_err(|error| abort(format!("decode version owner {key:?}: {error}")))?;
    let expected = codec::tip_version_backref_key(&row.atom_uuid, &row.version_id);
    if bare != expected {
        return Err(abort(format!(
            "version owner key differs from value: {key:?}"
        )));
    }
    if let Some(node) = nodes.get(&row.version_id) {
        if !personal_tips
            || bare != key
            || !dead.contains_key(&mol_key(&row.molecule_uuid))
            || row.atom_uuid != node.atom_uuid
        {
            return Err(abort(format!("kept or inconsistent version owner {key:?}")));
        }
    }
    Ok(())
}

pub(super) async fn check(
    raw: Arc<dyn KvStore>,
    seam: Arc<dyn KvStore>,
    dead: &SpellingMap,
    nodes: &BTreeMap<String, VersionNode>,
    summary: &mut SourceSummary,
    personal_tips: bool,
) -> Result<(), ReapError> {
    if nodes.is_empty() {
        return Ok(());
    }
    walk_both(raw, seam, "VERSION_SOURCE_OWNERSHIP", |_, page| {
        for (key, plain) in &page.rows {
            summary.ownership_rows += 1;
            let key = std::str::from_utf8(key).map_err(|error| abort(error.to_string()))?;
            check_backref(key, plain, dead, nodes, personal_tips)?;
            if non_source_metadata(key) {
                continue;
            }
            let row = classify(key);
            let doomed = personal_tips
                && row
                    .token
                    .as_deref()
                    .is_some_and(|molecule| dead.contains_key(&mol_key(molecule)));
            let bare = bare_key(key);
            let version = bare
                .strip_prefix("tv\0")
                .or_else(|| bare.strip_prefix(codec::TIP_VERSION_PREFIX));
            let candidate = personal_tips
                && version.is_some_and(|version| {
                    nodes
                        .get(version)
                        .is_some_and(|node| node.keys.iter().any(|stored| stored == key))
                });
            if version.is_some_and(|version| nodes.contains_key(version)) && !candidate {
                return Err(abort(format!(
                    "kept authoritative copy of candidate version {key:?}"
                )));
            }
            // Legacy namespaces and scoped sources remain kept, including a
            // physically duplicated candidate version in another collection.
            for link in nonempty_links(plain)? {
                if nodes.contains_key(&link) && !candidate && !doomed {
                    return Err(abort(format!(
                        "kept source {key:?} references candidate version {link}"
                    )));
                }
                if nodes.contains_key(&link)
                    && doomed
                    && !matches!(row.class, Class::Mk | Class::Mgr | Class::Mgd)
                {
                    return Err(abort(format!(
                        "unsupported doomed source {key:?} references version {link}"
                    )));
                }
            }
        }
        Ok(())
    })
    .await
}
