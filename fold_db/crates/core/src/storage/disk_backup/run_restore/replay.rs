//! Restore-only native publication. No daemon or logical maintenance worker starts.
use super::*;
use crate::storage::disk_backup::mutation_log::{Frontiers, LogAnchor, LogEntry, LogPosition};
use crate::storage::disk_pack::{run_node, NODE_MARKER};
use laststore::disk_run::home::RunHome;
use std::fs::File;

/// Replays main-home log parts. The CAS home is not logged and stays empty.
pub struct MutationReplay {
    main: RunHome,
    _node: File,
}
impl MutationReplay {
    pub fn open(data: &Path, key: &[u8; 32], anchor: &LogAnchor) -> StorageResult<Self> {
        files::directory(data)?;
        files::require_root_members(data, false)?;
        files::node_marker(data)?;
        let node = files::regular(&data.join(NODE_MARKER), true)?;
        node.try_lock()
            .map_err(|_| invalid("mutation replay node is owned"))?;
        let main =
            RunHome::open(&data.join(run_node::RUN_DIR), &run_node::home_key(key)).map_err(map)?;
        if main.identity().to_string() != anchor.main_home.as_str() {
            return Err(invalid("mutation replay native home identity"));
        }
        Ok(Self { main, _node: node })
    }
    pub fn frontiers(&self) -> Frontiers {
        Frontiers {
            main: self.main.current_root().frontier(),
        }
    }
    /// Only after the caller durably advances beyond every part in a checkpoint.
    /// Physical native compaction bounds replay files without changing frontiers.
    pub fn settle(&self) -> StorageResult<()> {
        let before = self.frontiers();
        while self.main.merge_step().map_err(map)?.is_some() {}
        if self.frontiers() != before {
            return Err(invalid("mutation replay compaction changed frontier"));
        }
        Ok(())
    }
    /// A durable caller cursor precedes the next part. An exact repeated part is allowed.
    pub fn part(&self, entry: &LogEntry, part_index: usize, source: &Path) -> StorageResult<()> {
        let part = entry
            .parts
            .get(part_index)
            .ok_or_else(|| invalid("mutation replay part number"))?;
        let mut before = entry.before;
        for old in &entry.parts[..part_index] {
            before.main = old.frontier;
        }
        let mut after = before;
        after.main = part.frontier;
        let (home, from) = (&self.main, before.main);
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
) -> StorageResult<RunRestoreGuard> {
    if through.epoch != anchor.epoch {
        return Err(invalid("mutation replay epoch"));
    }
    let native = phase(Stage::PhysicalRoot, RunNodeGuard::open(data, key))?;
    if native.profile.main.home_id != anchor.main_home
        || native.profile.main.frontier.get() != through.frontiers.main
        || native.profile.cas.is_some()
    {
        return Err(invalid("mutation replay exact final frontier"));
    }
    native.require_empty_local_cas()?;
    let records = verify_readers_for_profile(
        &native.main,
        native.backed_up_cas(),
        key,
        native.profile.version,
    )?;
    let guard = RunRestoreGuard { native, records };
    phase(Stage::FinalBinding, guard.check_binding())?;
    Ok(guard)
}
