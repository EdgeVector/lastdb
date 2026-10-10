//! Offline drop of expired record versions.

use super::maintenance::{finish_legacy_rewrite, segment_bytes};
use super::version_chain::*;
use super::LastStore;
use crate::options::PackagingMode;
use crate::{Error, Result};
use std::collections::{BTreeMap, HashMap};

impl LastStore {
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
