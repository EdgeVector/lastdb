//! Verify an inactive run home without a writer, cleanup, or conversion.
//!
//! A personal backup carries the main home only. Restore installs an empty
//! CAS home ([`install_empty_cas_home`]); record blobs are fetched from their
//! cloud paths on request. A shared database backup still carries its CAS home.

mod database;
mod files;
mod node;
mod record;

use super::byte_backup::RunBackupProfile;
use crate::storage::disk_backup::mutation_log::{Frontiers, LogAnchor, LogEntry, LogPosition};
use crate::storage::disk_pack::{run_node, NODE_MARKER, RUN_DIR};
use crate::storage::error::{
    NativeProofCause, NativeProofStage as Stage, StorageError, StorageResult,
};
use cloud_backup_protocol::NativeRoot;
use laststore::disk_run::home::{ReadBudget, RunHome, RunRead, RUNS};
use std::fs::{DirBuilder, File, OpenOptions};
use std::io::{self, Read};
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::path::Path;

pub use database::{SnapshotRecordPage, SnapshotSchema};
pub use node::{validate_replayed_shared_node, validate_restored_node, RunNodeGuard};
pub use record::{RunSnapshotReader, SnapshotAlternatePage, StagedRecord, StagedRecordQuery};

pub struct RunRestoreFile {
    pub path: String,
    pub bytes: u64,
}

/// Complete semantic proof retains the physical account guard until publication.
pub struct RunRestoreGuard {
    native: RunNodeGuard,
    records: u64,
}

impl RunRestoreGuard {
    pub fn native_root(&self) -> StorageResult<NativeRoot> {
        self.native.profile.native_root()
    }

    /// The number of record files that the proof opened and checked against
    /// their markers.
    pub fn verified_records(&self) -> u64 {
        self.records
    }

    pub fn files(&self) -> impl Iterator<Item = &RunRestoreFile> {
        self.native.files.iter()
    }

    pub fn totals(&self) -> StorageResult<(u64, u64)> {
        let count =
            u64::try_from(self.native.files.len()).map_err(|_| invalid("run file count"))?;
        let bytes = self.native.files.iter().try_fold(0u64, |sum, file| {
            sum.checked_add(file.bytes)
                .ok_or_else(|| invalid("run file byte sum"))
        })?;
        Ok((count, bytes))
    }

    pub fn check_binding(&self) -> StorageResult<()> {
        self.native.check_binding()
    }

    pub fn validate_context(&self, data_root: &Path, root: &NativeRoot) -> StorageResult<()> {
        if self.native.root != data_root {
            return Err(invalid("run restore publication root differs"));
        }
        self.native.profile.validate_root(root)?;
        self.check_binding()
    }
}

/// Complete physical closure precedes account and record validation.
pub fn verify(
    data_root: &Path,
    profile: &RunBackupProfile,
    native_root: &NativeRoot,
    account_key: &[u8; 32],
) -> StorageResult<RunRestoreGuard> {
    phase(Stage::Profile, profile.validate_root(native_root))?;
    let native = phase(
        Stage::PhysicalRoot,
        RunNodeGuard::open(data_root, account_key),
    )?;
    if native.profile != *profile {
        return Err(invalid("run restore captured profile differs"));
    }
    native.require_empty_local_cas()?;
    let records = verify_readers_for_profile(
        &native.main,
        native.backed_up_cas(),
        account_key,
        native.profile.version,
    )?;
    let guard = RunRestoreGuard { native, records };
    phase(Stage::FinalBinding, guard.check_binding())?;
    Ok(guard)
}

pub(crate) fn verify_readers_for_profile(
    main: &RunRead,
    _cas: Option<&RunRead>,
    account_key: &[u8; 32],
    version: u16,
) -> StorageResult<u64> {
    phase(
        Stage::Account,
        node::verify_account(main, account_key, version),
    )?;
    Ok(0)
}

/// Create the empty local CAS home of a restored personal node, after its
/// main home files are in place and before the proof opens the node. An
/// existing CAS home is kept; the proof requires it to be empty.
pub fn install_empty_cas_home(data_root: &Path, account_key: &[u8; 32]) -> StorageResult<()> {
    let path = data_root.join(run_node::CAS_DIR);
    if path.try_exists()? {
        return Ok(());
    }
    let home = RunHome::create(&path, &run_node::cas_key(account_key)).map_err(map)?;
    drop(home);
    File::open(data_root)?.sync_all()?;
    Ok(())
}

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
        while self.main.merge_step().map_err(map)? {}
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
        let current = self.frontiers();
        if current != before && current != after {
            return Err(invalid("mutation replay exact partial frontier"));
        }
        let applied = self
            .main
            .replay_commit_file(source, before.main, part.frontier)
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

impl RunSnapshotReader {
    /// Seed a fresh stage from the exact verified predecessor. Runtime locks
    /// and the empty local CAS home are created separately by the caller.
    pub fn seed_replay(
        &self,
        destination: &Path,
        anchor: &LogAnchor,
        position: &LogPosition,
    ) -> StorageResult<()> {
        let native = &self.0;
        if native.profile.main.home_id != anchor.main_home
            || native.profile.main.frontier.get() != position.frontiers.main
            || position.epoch != anchor.epoch
            || native.profile.cas.is_some()
        {
            return Err(invalid("cached mutation replay predecessor"));
        }
        native.check_binding()?;
        files::directory(destination)?;
        let bytes = native.files.iter().try_fold(0u64, |sum, file| {
            sum.checked_add(file.bytes)
                .ok_or_else(|| invalid("cached mutation replay bytes"))
        })?;
        let _space = laststore::space::reserve_space(
            destination,
            &laststore::space::SpacePolicy::default(),
            bytes,
        )
        .map_err(map)?;
        let main = destination.join(RUN_DIR);
        let runs = main.join(RUNS);
        for directory in [&main, &runs] {
            DirBuilder::new().mode(0o700).create(directory)?;
        }
        for file in &native.files {
            copy_replay_file(
                &native.root.join(&file.path),
                &destination.join(&file.path),
                file.bytes,
            )?;
        }
        for directory in [runs.as_path(), main.as_path(), destination] {
            File::open(directory)?.sync_all()?;
        }
        native.check_binding()
    }
}

fn copy_replay_file(source: &Path, destination: &Path, bytes: u64) -> StorageResult<()> {
    let mut input = files::regular(source, false)?;
    let mut output = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(destination)?;
    let before = input.metadata()?;
    let copied = io::copy(&mut (&mut input).take(bytes.saturating_add(1)), &mut output)?;
    files::same_entry(&input, source)?;
    let after = input.metadata()?;
    if copied != bytes || before.len() != bytes || after.len() != bytes {
        return Err(invalid("cached mutation replay file length"));
    }
    output.sync_all()?;
    Ok(())
}

pub(in crate::storage::disk_backup::run_restore) fn invalid(message: &'static str) -> StorageError {
    StorageError::NativeProof {
        stage: Stage::Validation,
        cause: NativeProofCause::Static(message),
    }
}

pub(in crate::storage::disk_backup::run_restore) fn phase<T>(
    stage: Stage,
    result: StorageResult<T>,
) -> StorageResult<T> {
    result.map_err(|error| error.at_native_proof(stage))
}

/// Classify native reader types before the ordinary adapter flattens their text.
pub(in crate::storage::disk_backup::run_restore) fn map(error: laststore::Error) -> StorageError {
    let cause = match error {
        laststore::Error::Corrupt(_) => NativeProofCause::NativeFormat,
        laststore::Error::AeadAuthFail => NativeProofCause::Authentication,
        laststore::Error::Config(message)
            if message == "disk run snapshot: another owner holds the home" =>
        {
            NativeProofCause::Static("run restore owner lock busy")
        }
        laststore::Error::Config(_) => NativeProofCause::Configuration,
        other => return crate::storage::disk_pack::map(other),
    };
    StorageError::NativeProof {
        stage: Stage::NativeReader,
        cause,
    }
}

/// Open the main run first. The cas run then uses the remaining memory budget.
pub(in crate::storage::disk_backup::run_restore) fn open_paired_runs(
    root: &Path,
    key: &[u8; 32],
    shared: bool,
) -> StorageResult<(RunRead, RunRead)> {
    let open = if shared {
        RunRead::open_existing_shared_bounded
    } else {
        RunRead::open_existing_bounded
    };
    let maximum = lastdb_model::storage_policy::BACKUP_RESOURCES.working_memory_bytes;
    let main = open(
        &root.join(run_node::RUN_DIR),
        &run_node::home_key(key),
        read_budget(),
        maximum,
    )
    .map_err(map)?;
    let remaining = maximum
        .checked_sub(main.retained_bytes())
        .ok_or_else(|| invalid("paired run metadata exceeds memory admission"))?;
    let cas = open(
        &root.join(run_node::CAS_DIR),
        &run_node::cas_key(key),
        read_budget(),
        remaining,
    )
    .map_err(map)?;
    Ok((main, cas))
}

/// Charge both readers with one shared byte count. The main reader is first.
pub(in crate::storage::disk_backup::run_restore) fn reserve_paired_metadata(
    main: &mut RunRead,
    cas: &mut RunRead,
    shared: u64,
) -> StorageResult<()> {
    main.reserve_metadata(shared).map_err(map)?;
    cas.reserve_metadata(shared).map_err(map)
}

pub(in crate::storage::disk_backup::run_restore) fn read_budget() -> ReadBudget {
    ReadBudget {
        pages: lastdb_model::storage_policy::backup_decode::RECORD_INVENTORY_PAGES,
        blocks: lastdb_model::storage_policy::backup_decode::NATIVE_STREAM_BLOCKS,
        bytes: lastdb_model::storage_policy::backup_decode::RECORD_INVENTORY_READ_BYTES,
        keys: 10000,
    }
}
