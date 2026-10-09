//! Offline rewrite for one store process and no warm set.
//!
//! The caller opens the store while the daemon is stopped. Each group is
//! loaded, filtered, rewritten, and dropped before the next group loads.

use super::LastStore;
use crate::keysidecar;
use crate::options::PackagingMode;
use crate::{Error, Result};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::{self, File};
use std::path::Path;

/// Collections whose live records stay and whose dead copies are rewritten.
///
/// Atoms and `cas_blobs` are absent on purpose. Atom rewrite needs a
/// retirement receipt. Blob identity is a backup record.
const HELPER_COLLECTIONS: &[&str] = &[
    "molecule_ref_edges",
    "keep_small",
    "schema_index",
    "atom_locators",
    "atom_ref_edges_v2",
];

/// Counts from one maintenance pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaintenanceReport {
    /// Molecules that still have an `mk:` key.
    pub live_molecules: u64,
    /// `mk:` keys read in the first pass.
    pub mk_keys: u64,
    /// Live tip keys visited in the second pass.
    pub keys_seen: u64,
    /// Order-log keys removed, or the count that would be removed.
    pub keys_dropped: u64,
    /// Tip groups rewritten.
    pub groups_rewritten: u64,
    /// Tip segment bytes before those rewrites.
    pub tips_bytes_before: u64,
    /// Tip segment bytes after those rewrites.
    pub tips_bytes_after: u64,
    /// Helper segment bytes before rewrite.
    pub helper_bytes_before: u64,
    /// Helper segment bytes after rewrite.
    pub helper_bytes_after: u64,
}

enum OrderKey<'a> {
    Mk(&'a str),
    MordSparse { id: &'a str, nanos: u64 },
    MordDense { id: &'a str },
    Moc(&'a str),
}

fn molecule_token(rest: &str) -> Option<&str> {
    let end = rest.find([':', '\0']).unwrap_or(rest.len());
    let id = &rest[..end];
    if id.is_empty() {
        None
    } else {
        Some(id)
    }
}

fn classify(key: &str) -> Option<OrderKey<'_>> {
    if let Some(rest) = key.strip_prefix("mk:") {
        return molecule_token(rest).map(OrderKey::Mk);
    }
    if let Some(rest) = key.strip_prefix("mord:") {
        return classify_mord_rest(rest);
    }
    if let Some(rest) = key.strip_prefix("mord\u{0}") {
        return molecule_token(rest).map(|id| OrderKey::MordDense { id });
    }
    if let Some(rest) = key.strip_prefix("moc:") {
        return molecule_token(rest).map(OrderKey::Moc);
    }
    if let Some(rest) = key.strip_prefix("moc\u{0}") {
        return molecule_token(rest).map(OrderKey::Moc);
    }
    None
}

fn classify_mord_rest(rest: &str) -> Option<OrderKey<'_>> {
    let id_end = rest.find([':', '\0'])?;
    let id = &rest[..id_end];
    if id.is_empty() {
        return None;
    }
    if rest.as_bytes().get(id_end) == Some(&0) {
        let stamped = rest.get(id_end + 1..)?;
        let nanos = stamped.get(..20)?;
        if nanos.len() == 20 && nanos.bytes().all(|byte| byte.is_ascii_digit()) {
            return nanos
                .parse::<u64>()
                .ok()
                .map(|nanos| OrderKey::MordSparse { id, nanos });
        }
        return None;
    }
    Some(OrderKey::MordDense { id })
}

fn should_drop(key: &str, live: &HashSet<String>, cutoff_nanos: u64) -> bool {
    match classify(key) {
        Some(OrderKey::MordSparse { id, nanos }) => nanos < cutoff_nanos || !live.contains(id),
        Some(OrderKey::MordDense { id } | OrderKey::Moc(id)) => !live.contains(id),
        Some(OrderKey::Mk(_)) | None => false,
    }
}

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

struct ParsedTip {
    atom_uuid: String,
    written_at: u64,
    prev_tip_id: String,
    tombstoned: bool,
}

struct VersionLoc {
    key: String,
    shard: u16,
    group: Option<u32>,
}

struct VersionNode {
    locs: Vec<VersionLoc>,
    prev_tip_id: String,
    written_at: u64,
    atom_uuid: String,
    filled: bool,
    readable: bool,
}

struct LiveHead {
    key: String,
    prev_tip_id: String,
    shard: u16,
    group: Option<u32>,
}

struct Walked {
    id: String,
    written_at: u64,
}

#[derive(Default)]
struct GroupEdit {
    drop_versions: Vec<String>,
    drop_backrefs: Vec<String>,
    rewrites: Vec<String>,
}

type TipGroupKey = (u16, Option<u32>);
type VersionEditPlan = (BTreeMap<TipGroupKey, GroupEdit>, Vec<String>);

fn version_id_of(key: &str) -> Option<&str> {
    let rest = key
        .strip_prefix("tv\0")
        .or_else(|| key.strip_prefix("tv:"))?;
    if rest.is_empty() {
        None
    } else {
        Some(rest)
    }
}

fn is_mk_key(key: &str) -> bool {
    key.strip_prefix("mk:")
        .or_else(|| key.strip_prefix("mk\0"))
        .is_some_and(|rest| !rest.is_empty())
}

fn contains_slice(hay: &[u8], needle: &[u8]) -> bool {
    hay.windows(needle.len()).any(|window| window == needle)
}

fn may_have_version_link(body: &[u8]) -> bool {
    contains_slice(body, b"prev_tip_id") || contains_slice(body, b"prev_atom_uuid")
}

fn at_rest_envelope(body: &[u8]) -> bool {
    body.starts_with(b"ENC:") || body.starts_with(b"ENZ:") || body.starts_with(b"ENB:")
}

fn opened_body(body: &[u8], open: &dyn Fn(&[u8]) -> Option<Vec<u8>>) -> Option<Vec<u8>> {
    if at_rest_envelope(body) {
        open(body)
    } else {
        Some(body.to_vec())
    }
}

fn sealed_body(
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

fn json_string(obj: &serde_json::Map<String, serde_json::Value>, key: &str) -> String {
    obj.get(key)
        .and_then(serde_json::Value::as_str)
        .unwrap_or("")
        .to_string()
}

fn parse_tip(body: &[u8]) -> Option<ParsedTip> {
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

fn clear_prev_link(body: &[u8]) -> Option<Vec<u8>> {
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

fn backref_key(atom_uuid: &str, version_id: &str) -> String {
    format!("tvr:v2:{atom_uuid}\0{version_id}")
}

fn push_unique(keys: &mut Vec<String>, key: String) {
    if !keys.iter().any(|existing| existing == &key) {
        keys.push(key);
    }
}

fn first_expired_index(nodes: &[Walked], cutoff_nanos: u64) -> Option<usize> {
    nodes
        .iter()
        .position(|node| node.written_at > 0 && node.written_at < cutoff_nanos)
}

fn segment_bytes(dir: &Path) -> u64 {
    let Ok(entries) = fs::read_dir(dir) else {
        return 0;
    };
    entries
        .filter_map(std::result::Result::ok)
        .filter_map(|entry| {
            let path = entry.path();
            if path.extension().and_then(|ext| ext.to_str()) == Some("seg") {
                entry.metadata().ok().map(|meta| meta.len())
            } else {
                None
            }
        })
        .fold(0u64, u64::saturating_add)
}

fn list_segments(dir: &Path) -> Result<Vec<(u64, std::path::PathBuf)>> {
    let mut out = Vec::new();
    if !dir.exists() {
        return Ok(out);
    }
    for entry in fs::read_dir(dir)? {
        let path = entry?.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("seg") {
            continue;
        }
        let Some(seq) = path
            .file_stem()
            .and_then(|stem| stem.to_str())
            .and_then(|stem| stem.parse::<u64>().ok())
        else {
            continue;
        };
        out.push((seq, path));
    }
    Ok(out)
}

/// After a legacy rewrite, only the segment named by the in-memory group
/// may remain. An older file still holds keys the rewrite removed.
fn finish_legacy_rewrite(sh: &super::Shard) -> Result<()> {
    let on_disk = list_segments(&sh.dir)?;
    if sh.segments.is_empty() {
        for (_, path) in on_disk {
            fs::remove_file(path)?;
        }
    } else if sh.segments.len() == 1 {
        let keep = sh.segments[0];
        for (seq, path) in on_disk {
            if seq != keep {
                fs::remove_file(path)?;
            }
        }
    } else {
        return Err(Error::Corrupt(
            "maintenance rewrite left more than one live segment".into(),
        ));
    }
    match fs::remove_file(keysidecar::sidecar_path(&sh.dir)) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    File::open(&sh.dir)?.sync_all()?;
    Ok(())
}

impl LastStore {
    /// Drop expired order-log keys and rewrite the groups that held them.
    ///
    /// `cutoff_nanos` is the sparse `mord` timestamp in nanoseconds. A sparse
    /// key older than this cutoff is dropped. A `mord` or `moc` key whose
    /// molecule has no `mk:` key is dropped. Every other key stays.
    ///
    /// `execute` false counts the drops and writes nothing. The caller must
    /// be the only process with this store open. One group is loaded at a
    /// time. The group is not published into the warm set.
    pub fn maintenance_shrink(
        &self,
        cutoff_nanos: u64,
        execute: bool,
    ) -> Result<MaintenanceReport> {
        if self.opts.packaging != PackagingMode::Plain || self.opts.data_key.is_some() {
            return Err(Error::Config(
                "maintenance rewrite supports plain packaging only".into(),
            ));
        }
        let (live, mk_keys, mord_keys) = self.collect_live_molecules()?;
        if execute && live.is_empty() && mord_keys > 0 {
            return Err(Error::Config(
                "refusing to drop order-log keys when no mk: key was read".into(),
            ));
        }
        let mut report = MaintenanceReport {
            live_molecules: live.len() as u64,
            mk_keys,
            keys_seen: 0,
            keys_dropped: 0,
            groups_rewritten: 0,
            tips_bytes_before: 0,
            tips_bytes_after: 0,
            helper_bytes_before: 0,
            helper_bytes_after: 0,
        };
        self.rewrite_tips(&live, cutoff_nanos, execute, &mut report)?;
        if execute {
            self.rewrite_helpers(&mut report)?;
        }
        Ok(report)
    }

    fn collect_live_molecules(&self) -> Result<(HashSet<String>, u64, u64)> {
        let mut live = HashSet::new();
        let mut mk_keys = 0u64;
        let mut mord_keys = 0u64;
        for (shard, group) in self.handles_on_disk("tips")? {
            let key = ("tips".to_string(), shard, group);
            let handle = self.open_group_unpublished(key)?;
            let sh = handle.lock().expect("poison");
            if sh.uses_sorted_index() {
                return Err(Error::Config(
                    "maintenance rewrite supports legacy segments only".into(),
                ));
            }
            sh.visit_keys("", None, |id, _| {
                match classify(id) {
                    Some(OrderKey::Mk(molecule)) => {
                        live.insert(molecule.to_string());
                        mk_keys = mk_keys.saturating_add(1);
                    }
                    Some(
                        OrderKey::MordSparse { .. } | OrderKey::MordDense { .. } | OrderKey::Moc(_),
                    ) => {
                        mord_keys = mord_keys.saturating_add(1);
                    }
                    None => {}
                }
                true
            })?;
            drop(sh);
            drop(handle);
        }
        Ok((live, mk_keys, mord_keys))
    }

    fn rewrite_tips(
        &self,
        live: &HashSet<String>,
        cutoff_nanos: u64,
        execute: bool,
        report: &mut MaintenanceReport,
    ) -> Result<()> {
        let groups = self.handles_on_disk("tips")?;
        let total = groups.len();
        for (index, (shard, group)) in groups.into_iter().enumerate() {
            let key = ("tips".to_string(), shard, group);
            let handle = self.open_group_unpublished(key)?;
            let mut sh = handle.lock().expect("poison");
            if sh.uses_sorted_index() {
                return Err(Error::Config(
                    "maintenance rewrite supports legacy segments only".into(),
                ));
            }
            let mut drop_ids = Vec::new();
            let seen = sh.visit_keys("", None, |id, _| {
                if should_drop(id, live, cutoff_nanos) {
                    drop_ids.push(id.to_string());
                }
                true
            })?;
            report.keys_seen = report.keys_seen.saturating_add(seen);
            report.keys_dropped = report.keys_dropped.saturating_add(drop_ids.len() as u64);
            if execute && !drop_ids.is_empty() {
                let before = segment_bytes(&sh.dir);
                for id in &drop_ids {
                    sh.remove_index(id)?;
                }
                LastStore::compact_shard(&mut sh)?;
                finish_legacy_rewrite(&sh)?;
                let after = segment_bytes(&sh.dir);
                report.groups_rewritten = report.groups_rewritten.saturating_add(1);
                report.tips_bytes_before = report.tips_bytes_before.saturating_add(before);
                report.tips_bytes_after = report.tips_bytes_after.saturating_add(after);
                eprintln!(
                    "maintenance tips shard={shard} group={} drop={} before={before} after={after}",
                    group
                        .map(|value| format!("{value:03x}"))
                        .unwrap_or_else(|| "-".to_string()),
                    drop_ids.len()
                );
            }
            drop(sh);
            drop(handle);
            if index % 64 == 0 {
                eprintln!(
                    "maintenance tips progress {}/{total} dropped={}",
                    index + 1,
                    report.keys_dropped
                );
            }
        }
        Ok(())
    }

    fn rewrite_helpers(&self, report: &mut MaintenanceReport) -> Result<()> {
        for collection in HELPER_COLLECTIONS {
            let groups = self.handles_on_disk(collection)?;
            for (shard, group) in groups {
                let key = ((*collection).to_string(), shard, group);
                let handle = self.open_group_unpublished(key)?;
                let mut sh = handle.lock().expect("poison");
                if sh.uses_sorted_index() || sh.segments.is_empty() {
                    drop(sh);
                    drop(handle);
                    continue;
                }
                let before = segment_bytes(&sh.dir);
                LastStore::compact_shard(&mut sh)?;
                finish_legacy_rewrite(&sh)?;
                let after = segment_bytes(&sh.dir);
                report.helper_bytes_before = report.helper_bytes_before.saturating_add(before);
                report.helper_bytes_after = report.helper_bytes_after.saturating_add(after);
                drop(sh);
                drop(handle);
            }
            eprintln!("maintenance helper {collection} done");
        }
        Ok(())
    }

    /// Drop expired versions of a live record and clear the link that pointed
    /// at the first removed version.
    ///
    /// `cutoff_nanos` is the `written_at` threshold. A version with
    /// `written_at == 0` stays. The first dated version older than the cutoff,
    /// and every version after it, is removed. A tombstoned head is skipped.
    /// A version with no live head is left in place. `execute` false counts
    /// the removals and writes nothing. One group is loaded at a time. The
    /// group is not published into the warm set.
    pub fn maintenance_drop_versions(
        &self,
        cutoff_nanos: u64,
        execute: bool,
    ) -> Result<VersionRetentionReport> {
        self.maintenance_drop_versions_with(cutoff_nanos, execute, &|_| None, &|plain| {
            Some(plain.to_vec())
        })
    }

    /// Same rule as [`Self::maintenance_drop_versions`]. `open` reads a sealed
    /// body. `seal` writes a changed body back. A plain body does not use
    /// `open`. A sealed body that `open` cannot read stays in place.
    pub fn maintenance_drop_versions_with(
        &self,
        cutoff_nanos: u64,
        execute: bool,
        open: &dyn Fn(&[u8]) -> Option<Vec<u8>>,
        seal: &dyn Fn(&[u8]) -> Option<Vec<u8>>,
    ) -> Result<VersionRetentionReport> {
        if self.opts.packaging != PackagingMode::Plain || self.opts.data_key.is_some() {
            return Err(Error::Config(
                "maintenance rewrite supports plain packaging only".into(),
            ));
        }
        let mut report = VersionRetentionReport {
            version_cutoff_nanos: cutoff_nanos,
            version_keys: 0,
            heads_with_chain: 0,
            heads_truncated: 0,
            heads_skipped_tombstoned: 0,
            bodies_unopened: 0,
            versions_kept: 0,
            versions_dropped: 0,
            links_rewritten: 0,
            backrefs_dropped: 0,
            groups_rewritten: 0,
            tips_bytes_before: 0,
            tips_bytes_after: 0,
        };
        let (versions, version_keys) = self.collect_version_nodes(open, &mut report)?;
        report.version_keys = version_keys;
        if versions.is_empty() {
            eprintln!("maintenance versions none");
            return Ok(report);
        }
        let heads = self.collect_version_heads(open, &mut report)?;
        let (mut edits, backrefs) =
            plan_version_edits(&versions, &heads, cutoff_nanos, &mut report);
        for key in backrefs {
            let (_, shard, group) = self.point_key("tips", &key);
            let edit = edits.entry((shard, group)).or_default();
            push_unique(&mut edit.drop_backrefs, key);
        }
        self.apply_version_edits(&edits, execute, open, seal, &mut report)?;
        Ok(report)
    }

    fn collect_version_nodes(
        &self,
        open: &dyn Fn(&[u8]) -> Option<Vec<u8>>,
        report: &mut VersionRetentionReport,
    ) -> Result<(HashMap<String, VersionNode>, u64)> {
        let mut versions = HashMap::new();
        let mut version_keys = 0u64;
        let groups = self.handles_on_disk("tips")?;
        let total = groups.len();
        for (index, (shard, group)) in groups.into_iter().enumerate() {
            let handle = self.open_group_unpublished(("tips".to_string(), shard, group))?;
            let mut sh = handle.lock().expect("poison");
            if sh.uses_sorted_index() {
                return Err(Error::Config(
                    "maintenance rewrite supports legacy segments only".into(),
                ));
            }
            let mut ids = Vec::new();
            sh.visit_keys("", None, |id, _| {
                if version_id_of(id).is_some() {
                    ids.push(id.to_string());
                }
                true
            })?;
            for id in ids {
                let Some(vid) = version_id_of(&id).map(str::to_string) else {
                    continue;
                };
                version_keys = version_keys.saturating_add(1);
                let body = LastStore::current_body_locked(&mut sh, &id)?;
                if sh.cache_legacy_bodies() {
                    sh.values.remove(&id);
                }
                let plain = body.as_deref().and_then(|bytes| opened_body(bytes, open));
                if body.as_deref().is_some_and(at_rest_envelope) && plain.is_none() {
                    report.bodies_unopened = report.bodies_unopened.saturating_add(1);
                }
                let parsed = plain.as_deref().and_then(parse_tip);
                let node = versions.entry(vid).or_insert_with(|| VersionNode {
                    locs: Vec::new(),
                    prev_tip_id: String::new(),
                    written_at: 0,
                    atom_uuid: String::new(),
                    filled: false,
                    readable: true,
                });
                node.locs.push(VersionLoc {
                    key: id,
                    shard,
                    group,
                });
                match parsed {
                    Some(tip) if !node.filled => {
                        node.filled = true;
                        node.prev_tip_id = tip.prev_tip_id;
                        node.written_at = tip.written_at;
                        node.atom_uuid = tip.atom_uuid;
                    }
                    Some(_) => {}
                    None => node.readable = false,
                }
            }
            drop(sh);
            drop(handle);
            if index % 64 == 0 {
                eprintln!(
                    "maintenance versions scan {}/{total} version_keys={version_keys}",
                    index + 1
                );
            }
        }
        Ok((versions, version_keys))
    }

    fn collect_version_heads(
        &self,
        open: &dyn Fn(&[u8]) -> Option<Vec<u8>>,
        report: &mut VersionRetentionReport,
    ) -> Result<Vec<LiveHead>> {
        let mut heads = Vec::new();
        let groups = self.handles_on_disk("tips")?;
        let total = groups.len();
        for (index, (shard, group)) in groups.into_iter().enumerate() {
            let handle = self.open_group_unpublished(("tips".to_string(), shard, group))?;
            let mut sh = handle.lock().expect("poison");
            if sh.uses_sorted_index() {
                return Err(Error::Config(
                    "maintenance rewrite supports legacy segments only".into(),
                ));
            }
            let mut ids = Vec::new();
            sh.visit_keys("", None, |id, _| {
                if is_mk_key(id) {
                    ids.push(id.to_string());
                }
                true
            })?;
            for id in ids {
                let body = match LastStore::current_body_locked(&mut sh, &id)? {
                    Some(body) => body,
                    None => continue,
                };
                if sh.cache_legacy_bodies() {
                    sh.values.remove(&id);
                }
                let Some(plain) = opened_body(&body, open) else {
                    if at_rest_envelope(&body) {
                        report.bodies_unopened = report.bodies_unopened.saturating_add(1);
                    }
                    continue;
                };
                if !may_have_version_link(&plain) {
                    continue;
                }
                let Some(tip) = parse_tip(&plain) else {
                    continue;
                };
                if tip.tombstoned {
                    report.heads_skipped_tombstoned =
                        report.heads_skipped_tombstoned.saturating_add(1);
                    continue;
                }
                if tip.prev_tip_id.is_empty() {
                    continue;
                }
                heads.push(LiveHead {
                    key: id,
                    prev_tip_id: tip.prev_tip_id,
                    shard,
                    group,
                });
            }
            drop(sh);
            drop(handle);
            if index % 64 == 0 {
                eprintln!(
                    "maintenance versions heads {}/{total} chained={}",
                    index + 1,
                    heads.len()
                );
            }
        }
        Ok(heads)
    }

    fn apply_version_edits(
        &self,
        edits: &BTreeMap<(u16, Option<u32>), GroupEdit>,
        execute: bool,
        open: &dyn Fn(&[u8]) -> Option<Vec<u8>>,
        seal: &dyn Fn(&[u8]) -> Option<Vec<u8>>,
        report: &mut VersionRetentionReport,
    ) -> Result<()> {
        for ((shard, group), edit) in edits {
            let handle = self.open_group_unpublished(("tips".to_string(), *shard, *group))?;
            let mut sh = handle.lock().expect("poison");
            let mut backrefs = Vec::new();
            for id in &edit.drop_backrefs {
                if sh.lookup(id)?.is_some() {
                    backrefs.push(id.clone());
                }
            }
            report.backrefs_dropped = report
                .backrefs_dropped
                .saturating_add(backrefs.len() as u64);
            let mut version_drops = Vec::new();
            for id in &edit.drop_versions {
                if sh.lookup(id)?.is_some() {
                    version_drops.push(id.clone());
                }
            }
            let mut rewrites = Vec::new();
            for id in &edit.rewrites {
                let Some(body) = LastStore::current_body_locked(&mut sh, id)? else {
                    return Err(Error::Corrupt(format!("version link key is missing: {id}")));
                };
                if sh.cache_legacy_bodies() {
                    sh.values.remove(id);
                }
                let Some(plain) = opened_body(&body, open) else {
                    return Err(Error::Corrupt(format!(
                        "version link value cannot be opened: {id}"
                    )));
                };
                let Some(cleared) = clear_prev_link(&plain) else {
                    return Err(Error::Corrupt(format!(
                        "version link value is not valid: {id}"
                    )));
                };
                if cleared == plain {
                    continue;
                }
                let Some(new_body) = sealed_body(&body, &cleared, seal) else {
                    return Err(Error::Corrupt(format!(
                        "version link value cannot be sealed: {id}"
                    )));
                };
                rewrites.push((id.clone(), new_body));
            }
            report.links_rewritten = report.links_rewritten.saturating_add(rewrites.len() as u64);
            if !execute || (version_drops.is_empty() && backrefs.is_empty() && rewrites.is_empty())
            {
                drop(sh);
                drop(handle);
                continue;
            }
            let before = segment_bytes(&sh.dir);
            for id in version_drops.iter().chain(backrefs.iter()) {
                sh.remove_index(id)?;
            }
            for (id, body) in &rewrites {
                let line = crate::segfmt::encode_put(id, body)?;
                let previous_len = sh.lookup(id)?.map(|location| location.record_len());
                let loc = self.append(&mut sh, &line, id, super::CaptureOp::Put, previous_len)?;
                sh.insert_index_known(id.clone(), loc, previous_len);
            }
            LastStore::compact_shard(&mut sh)?;
            finish_legacy_rewrite(&sh)?;
            let after = segment_bytes(&sh.dir);
            report.groups_rewritten = report.groups_rewritten.saturating_add(1);
            report.tips_bytes_before = report.tips_bytes_before.saturating_add(before);
            report.tips_bytes_after = report.tips_bytes_after.saturating_add(after);
            eprintln!(
                "maintenance versions shard={shard} group={} drop={} rewrite={} before={before} after={after}",
                group
                    .map(|value| format!("{value:03x}"))
                    .unwrap_or_else(|| "-".to_string()),
                version_drops.len() + backrefs.len(),
                rewrites.len()
            );
            drop(sh);
            drop(handle);
        }
        Ok(())
    }
}

fn plan_version_edits(
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

fn walk_version_chain(versions: &HashMap<String, VersionNode>, first: &str) -> Vec<Walked> {
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
