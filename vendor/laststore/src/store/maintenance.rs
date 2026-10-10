//! Offline rewrite for one store process and no warm set.
//!
//! The caller opens the store while the daemon is stopped. Each group is
//! loaded, filtered, rewritten, and dropped before the next group loads.

use super::LastStore;
use crate::keysidecar;
use crate::options::PackagingMode;
use crate::{Error, Result};
use std::collections::HashSet;
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

pub(super) fn segment_bytes(dir: &Path) -> u64 {
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
pub(super) fn finish_legacy_rewrite(sh: &super::Shard) -> Result<()> {
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
}
