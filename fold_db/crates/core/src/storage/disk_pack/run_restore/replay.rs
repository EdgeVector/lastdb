//! Restore-only native publication. No daemon or logical maintenance worker starts.
use super::*;
use crate::storage::disk_pack::{
    map,
    mutation_log::{Frontiers, LogAnchor, LogEntry, LogPosition},
    run_node, NODE_MARKER,
};
use laststore::disk_run::home::RunHome;
use std::fs::File;

pub struct MutationReplay {
    main: RunHome,
    cas: RunHome,
    _node: File,
}
impl MutationReplay {
    pub fn open(data: &Path, key: &[u8; 32], anchor: &LogAnchor) -> StorageResult<Self> {
        files::directory(data)?;
        files::require_root_members(data)?;
        files::node_marker(data)?;
        let node = files::regular(&data.join(NODE_MARKER), true)?;
        node.try_lock()
            .map_err(|_| invalid("mutation replay node is owned"))?;
        let main =
            RunHome::open(&data.join(run_node::RUN_DIR), &run_node::home_key(key)).map_err(map)?;
        let cas =
            RunHome::open(&data.join(run_node::CAS_DIR), &run_node::cas_key(key)).map_err(map)?;
        if main.identity().to_string() != anchor.main_home.as_str()
            || cas.identity().to_string() != anchor.cas_home.as_str()
        {
            return Err(invalid("mutation replay native home identity"));
        }
        Ok(Self {
            main,
            cas,
            _node: node,
        })
    }
    pub fn frontiers(&self) -> Frontiers {
        Frontiers {
            main: self.main.current_root().frontier(),
            cas: self.cas.current_root().frontier(),
        }
    }
    /// A durable caller cursor precedes the next part. An exact repeated part is allowed.
    pub fn part(&self, entry: &LogEntry, part_index: usize, source: &Path) -> StorageResult<()> {
        let part = entry
            .parts
            .get(part_index)
            .ok_or_else(|| invalid("mutation replay part number"))?;
        let mut before = entry.before.clone();
        for old in &entry.parts[..part_index] {
            if old.role.is_cas() {
                before.cas = old.frontier;
            } else {
                before.main = old.frontier;
            }
        }
        let mut after = before.clone();
        let (home, from) = if part.role.is_cas() {
            after.cas = part.frontier;
            (&self.cas, before.cas)
        } else {
            after.main = part.frontier;
            (&self.main, before.main)
        };
        let current = self.frontiers();
        if current != before && current != after {
            return Err(invalid("mutation replay exact partial frontier"));
        }
        let applied = home
            .replay_commit_file(source, from, part.frontier)
            .map_err(map)?;
        if applied.run_id.to_string() != part.run_id.as_str()
            || applied.run_bytes != part.bytes
            || self.frontiers() != after
        {
            return Err(invalid("mutation replay original run identity or length"));
        }
        Ok(())
    }
}

pub fn verify_replayed(
    data: &Path,
    anchor: &LogAnchor,
    through: &LogPosition,
    key: &[u8; 32],
    scratch: &Path,
) -> StorageResult<RunRestoreGuard> {
    if through.epoch != anchor.epoch {
        return Err(invalid("mutation replay epoch"));
    }
    let native = RunNodeGuard::open(data, key)?;
    if native.profile.main.home_id != anchor.main_home
        || native.profile.cas.home_id != anchor.cas_home
        || native.profile.main.frontier.get() != through.frontiers.main
        || native.profile.cas.frontier.get() != through.frontiers.cas
    {
        return Err(invalid("mutation replay exact final frontier"));
    }
    let atoms = verify_readers_for_profile(
        &native.main,
        &native.cas,
        key,
        scratch,
        native.profile.version,
    )?;
    let guard = RunRestoreGuard { native, atoms };
    guard.check_binding()?;
    Ok(guard)
}
