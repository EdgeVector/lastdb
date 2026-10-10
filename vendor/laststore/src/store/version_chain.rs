//! Version-chain types and planning for the offline version retention pass.

use std::collections::{BTreeMap, HashMap, HashSet};

/// Nanoseconds kept for an older version of a live record. The window is 7 days.
pub const SUPERSEDED_VERSION_RETENTION_NANOS: u64 = 7 * 24 * 60 * 60 * 1_000_000_000;

/// Counts from one offline pass over live-record versions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionRetentionReport {
    /// `written_at` values older than this are expired.
    pub version_cutoff_nanos: u64,
    /// `tv` keys read.
    pub version_keys: u64,
    /// Live heads whose link points at a version.
    pub heads_with_chain: u64,
    /// Live heads whose expired versions were removed, or would be removed.
    pub heads_truncated: u64,
    /// Tombstoned heads skipped. Their versions stay.
    pub heads_skipped_tombstoned: u64,
    /// Sealed bodies the opener could not read. Those versions stay.
    pub bodies_unopened: u64,
    /// Version nodes left on truncated heads.
    pub versions_kept: u64,
    /// Version nodes removed, or the count that would be removed.
    pub versions_dropped: u64,
    /// Head or version values whose link was cleared.
    pub links_rewritten: u64,
    /// Derived `tvr:v2:` keys removed, or the count that would be removed.
    pub backrefs_dropped: u64,
    /// Tip groups rewritten.
    pub groups_rewritten: u64,
    /// Tip segment bytes before those rewrites.
    pub tips_bytes_before: u64,
    /// Tip segment bytes after those rewrites.
    pub tips_bytes_after: u64,
}

pub(super) struct ParsedTip {
    pub(super) atom_uuid: String,
    pub(super) written_at: u64,
    pub(super) prev_tip_id: String,
    pub(super) tombstoned: bool,
}

pub(super) struct VersionLoc {
    pub(super) key: String,
    pub(super) shard: u16,
    pub(super) group: Option<u32>,
}

pub(super) struct VersionNode {
    pub(super) locs: Vec<VersionLoc>,
    pub(super) prev_tip_id: String,
    pub(super) written_at: u64,
    pub(super) atom_uuid: String,
    pub(super) filled: bool,
    pub(super) readable: bool,
}

pub(super) struct LiveHead {
    pub(super) key: String,
    pub(super) prev_tip_id: String,
    pub(super) shard: u16,
    pub(super) group: Option<u32>,
}

pub(super) struct Walked {
    pub(super) id: String,
    pub(super) written_at: u64,
}

#[derive(Default)]
pub(super) struct GroupEdit {
    pub(super) drop_versions: Vec<String>,
    pub(super) drop_backrefs: Vec<String>,
    pub(super) rewrites: Vec<String>,
}

pub(super) type TipGroupKey = (u16, Option<u32>);
pub(super) type VersionEditPlan = (BTreeMap<TipGroupKey, GroupEdit>, Vec<String>);

pub(super) fn version_id_of(key: &str) -> Option<&str> {
    let rest = key
        .strip_prefix("tv\0")
        .or_else(|| key.strip_prefix("tv:"))?;
    if rest.is_empty() {
        None
    } else {
        Some(rest)
    }
}

pub(super) fn is_mk_key(key: &str) -> bool {
    key.strip_prefix("mk:")
        .or_else(|| key.strip_prefix("mk\0"))
        .is_some_and(|rest| !rest.is_empty())
}

pub(super) fn contains_slice(hay: &[u8], needle: &[u8]) -> bool {
    hay.windows(needle.len()).any(|window| window == needle)
}

pub(super) fn may_have_version_link(body: &[u8]) -> bool {
    contains_slice(body, b"prev_tip_id") || contains_slice(body, b"prev_atom_uuid")
}

pub(super) fn at_rest_envelope(body: &[u8]) -> bool {
    body.starts_with(b"ENC:") || body.starts_with(b"ENZ:") || body.starts_with(b"ENB:")
}

pub(super) fn opened_body(body: &[u8], open: &dyn Fn(&[u8]) -> Option<Vec<u8>>) -> Option<Vec<u8>> {
    if at_rest_envelope(body) {
        open(body)
    } else {
        Some(body.to_vec())
    }
}

pub(super) fn sealed_body(
    original: &[u8],
    plain: &[u8],
    seal: &dyn Fn(&[u8]) -> Option<Vec<u8>>,
) -> Option<Vec<u8>> {
    if at_rest_envelope(original) {
        seal(plain)
    } else {
        Some(plain.to_vec())
    }
}

pub(super) fn json_string(obj: &serde_json::Map<String, serde_json::Value>, key: &str) -> String {
    obj.get(key)
        .and_then(serde_json::Value::as_str)
        .unwrap_or("")
        .to_string()
}

pub(super) fn parse_tip(body: &[u8]) -> Option<ParsedTip> {
    let value: serde_json::Value = serde_json::from_slice(body).ok()?;
    let obj = value.as_object()?;
    let entry = obj.get("entry").unwrap_or(&value);
    let entry = entry.as_object()?;
    let prev_tip_id = {
        let current = json_string(entry, "prev_tip_id");
        if current.is_empty() {
            json_string(entry, "prev_atom_uuid")
        } else {
            current
        }
    };
    let written_at = entry
        .get("written_at")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    let tombstoned = obj
        .get("meta")
        .and_then(|meta| meta.get("tombstoned"))
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    Some(ParsedTip {
        atom_uuid: json_string(entry, "atom_uuid"),
        written_at,
        prev_tip_id,
        tombstoned,
    })
}

pub(super) fn clear_prev_link(body: &[u8]) -> Option<Vec<u8>> {
    let mut value: serde_json::Value = serde_json::from_slice(body).ok()?;
    let mut changed = false;
    let strip = |value: &mut serde_json::Value| -> bool {
        let Some(obj) = value.as_object_mut() else {
            return false;
        };
        let removed_tip = obj.remove("prev_tip_id").is_some();
        let removed_alias = obj.remove("prev_atom_uuid").is_some();
        removed_tip || removed_alias
    };
    changed |= strip(&mut value);
    if let Some(entry) = value.get_mut("entry") {
        changed |= strip(entry);
    }
    if !changed {
        return Some(body.to_vec());
    }
    serde_json::to_vec(&value).ok()
}

pub(super) fn backref_key(atom_uuid: &str, version_id: &str) -> String {
    format!("tvr:v2:{atom_uuid}\0{version_id}")
}

pub(super) fn push_unique(keys: &mut Vec<String>, key: String) {
    if !keys.iter().any(|existing| existing == &key) {
        keys.push(key);
    }
}

pub(super) fn first_expired_index(nodes: &[Walked], cutoff_nanos: u64) -> Option<usize> {
    nodes
        .iter()
        .position(|node| node.written_at > 0 && node.written_at < cutoff_nanos)
}

pub(super) fn plan_version_edits(
    versions: &HashMap<String, VersionNode>,
    heads: &[LiveHead],
    cutoff_nanos: u64,
    report: &mut VersionRetentionReport,
) -> VersionEditPlan {
    let mut plans = Vec::new();
    for head in heads {
        report.heads_with_chain = report.heads_with_chain.saturating_add(1);
        let walked = walk_version_chain(versions, &head.prev_tip_id);
        let Some(expired_at) = first_expired_index(&walked, cutoff_nanos) else {
            continue;
        };
        plans.push((head, walked, expired_at));
    }
    let mut kept = HashSet::new();
    for (_, walked, expired_at) in &plans {
        for node in walked.iter().take(*expired_at) {
            kept.insert(node.id.clone());
        }
    }
    let mut edits: BTreeMap<TipGroupKey, GroupEdit> = BTreeMap::new();
    let mut backrefs = Vec::new();
    for (head, walked, expired_at) in plans {
        let expired: Vec<&Walked> = walked
            .iter()
            .skip(expired_at)
            .filter(|node| !kept.contains(&node.id))
            .collect();
        if expired.is_empty() {
            continue;
        }
        report.heads_truncated = report.heads_truncated.saturating_add(1);
        report.versions_kept = report.versions_kept.saturating_add(expired_at as u64);
        report.versions_dropped = report.versions_dropped.saturating_add(expired.len() as u64);
        let immediate_id = &walked[expired_at].id;
        let immediate_dropped = expired.iter().any(|node| node.id == *immediate_id);
        if immediate_dropped {
            if expired_at == 0 {
                let edit = edits.entry((head.shard, head.group)).or_default();
                push_unique(&mut edit.rewrites, head.key.clone());
            } else if let Some(node) = versions.get(&walked[expired_at - 1].id) {
                for loc in &node.locs {
                    let edit = edits.entry((loc.shard, loc.group)).or_default();
                    push_unique(&mut edit.rewrites, loc.key.clone());
                }
            }
        }
        for node in expired {
            let Some(stored) = versions.get(&node.id) else {
                continue;
            };
            for loc in &stored.locs {
                let edit = edits.entry((loc.shard, loc.group)).or_default();
                push_unique(&mut edit.drop_versions, loc.key.clone());
            }
            if !stored.atom_uuid.is_empty() {
                push_unique(&mut backrefs, backref_key(&stored.atom_uuid, &node.id));
            }
        }
    }
    (edits, backrefs)
}

pub(super) fn walk_version_chain(
    versions: &HashMap<String, VersionNode>,
    first: &str,
) -> Vec<Walked> {
    let mut walked = Vec::new();
    let mut seen = HashSet::new();
    let mut vid = first.to_string();
    while !vid.is_empty() && walked.len() < 1_000_000 {
        if !seen.insert(vid.clone()) {
            break;
        }
        let Some(node) = versions.get(&vid) else {
            break;
        };
        if !node.readable || !node.filled {
            break;
        }
        let next = node.prev_tip_id.clone();
        walked.push(Walked {
            id: vid,
            written_at: node.written_at,
        });
        vid = next;
    }
    walked
}
