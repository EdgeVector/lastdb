//! Verify an inactive run home without a writer, cleanup, or conversion.

mod database;
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
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::path::Path;

pub use database::{SnapshotRecordPage, SnapshotSchema};
pub use node::{validate_restored_node, RunNodeGuard};
pub use record::{RunSnapshotReader, SnapshotAlternatePage, StagedRecord, StagedRecordQuery};

pub struct RunRestoreFile {
    pub path: String,
    pub bytes: u64,
}

pub struct RunRestoreGuard {
    native: RunNodeGuard,
}

fn sum_file_bytes(files: &[RunRestoreFile], msg: &'static str) -> StorageResult<u64> {
    files
        .iter()
        .try_fold(0u64, |sum, f| sum.checked_add(f.bytes))
        .ok_or_else(|| invalid(msg))
}

impl RunRestoreGuard {
    pub fn native_root(&self) -> StorageResult<NativeRoot> {
        self.native.profile.native_root()
    }
    pub fn files(&self) -> impl Iterator<Item = &RunRestoreFile> {
        self.native.files.iter()
    }
    pub fn totals(&self) -> StorageResult<(u64, u64)> {
        let count =
            u64::try_from(self.native.files.len()).map_err(|_| invalid("run file count"))?;
        sum_file_bytes(&self.native.files, "run file byte sum").map(|b| (count, b))
    }
    pub fn check_binding(&self) -> StorageResult<()> {
        self.native.check_binding()
    }
    pub fn validate_context(&self, data_root: &Path, root: &NativeRoot) -> StorageResult<()> {
        if self.native.root != data_root {
            return Err(invalid("run restore publication root differs"));
        }
        self.native
            .profile
            .validate_root(root)
            .and_then(|()| self.check_binding())
    }
}

pub fn verify(
    data_root: &Path,
    profile: &RunBackupProfile,
    native_root: &NativeRoot,
    account_key: &[u8; 32],
) -> StorageResult<RunRestoreGuard> {
    phase(Stage::Profile, profile.validate_root(native_root))?;
    let native = phase(
        Stage::PhysicalRoot,
        RunNodeGuard::open(data_root, account_key, false),
    )?;
    (native.profile == *profile)
        .then_some(native)
        .ok_or_else(|| invalid("run restore captured profile differs"))
        .and_then(finish_restore_guard)
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
    let native = phase(Stage::PhysicalRoot, RunNodeGuard::open(data, key, false))?;
    if native.profile.main.home_id != anchor.main_home
        || native.profile.main.frontier.get() != through.frontiers.main
    {
        return Err(invalid("mutation replay exact final frontier"));
    }
    finish_restore_guard(native)
}

fn finish_restore_guard(native: RunNodeGuard) -> StorageResult<RunRestoreGuard> {
    native.require_empty_local_cas()?;
    let guard = RunRestoreGuard { native };
    phase(Stage::FinalBinding, guard.check_binding())?;
    Ok(guard)
}

pub(in crate::storage::disk_backup) fn verify_captured_account(
    main: &RunRead,
    account_key: &[u8; 32],
    version: u16,
) -> StorageResult<()> {
    phase(
        Stage::Account,
        node::verify_account(main, account_key, version),
    )
}

pub fn install_empty_cas_home(data_root: &Path, account_key: &[u8; 32]) -> StorageResult<()> {
    let path = data_root.join(run_node::CAS_DIR);
    if !path.try_exists()? {
        drop(RunHome::create(&path, &run_node::cas_key(account_key)).map_err(map)?);
        File::open(data_root)?.sync_all()?;
    }
    Ok(())
}

pub struct MutationReplay {
    main: RunHome,
    _node: File,
}

impl MutationReplay {
    pub fn open(data: &Path, key: &[u8; 32], anchor: &LogAnchor) -> StorageResult<Self> {
        node::directory(data)?;
        node::require_root_members(data)?;
        node::profile_version(data)?;
        let marker = node::regular(&data.join(NODE_MARKER), true)?;
        marker
            .try_lock()
            .map_err(|_| invalid("mutation replay node is owned"))?;
        let main =
            RunHome::open(&data.join(run_node::RUN_DIR), &run_node::home_key(key)).map_err(map)?;
        (main.identity().to_string() == anchor.main_home.as_str())
            .then_some(Self {
                main,
                _node: marker,
            })
            .ok_or_else(|| invalid("mutation replay native home identity"))
    }

    pub fn frontiers(&self) -> Frontiers {
        Frontiers {
            main: self.main.current_root().frontier(),
        }
    }

    pub fn settle(&self) -> StorageResult<()> {
        let before = self.frontiers();
        while self.main.merge_step().map_err(map)? {}
        (self.frontiers() == before)
            .then_some(())
            .ok_or_else(|| invalid("mutation replay compaction changed frontier"))
    }

    pub fn part(&self, entry: &LogEntry, part_index: usize, source: &Path) -> StorageResult<()> {
        let part = entry
            .parts
            .get(part_index)
            .ok_or_else(|| invalid("mutation replay part number"))?;
        let before_main = part_index
            .checked_sub(1)
            .and_then(|i| entry.parts.get(i))
            .map_or(entry.before.main, |p| p.frontier);
        let (before, after) = (
            Frontiers { main: before_main },
            Frontiers {
                main: part.frontier,
            },
        );
        let current = self.frontiers();
        if current != before && current != after {
            return Err(invalid("mutation replay exact partial frontier"));
        }
        let applied = self
            .main
            .replay_commit_file(source, before.main, part.frontier)
            .map_err(map)?;
        (applied.run_id.to_string() == part.run_id.as_str()
            && applied.run_bytes == part.bytes
            && self.frontiers() == after)
            .then_some(())
            .ok_or_else(|| invalid("mutation replay original run identity or length"))
    }
}

impl RunSnapshotReader {
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
        {
            return Err(invalid("cached mutation replay predecessor"));
        }
        native.check_binding()?;
        node::directory(destination)?;
        let bytes = sum_file_bytes(&native.files, "cached mutation replay bytes")?;
        let _space = laststore::space::reserve_space(
            destination,
            &laststore::space::SpacePolicy::default(),
            bytes,
        )
        .map_err(map)?;
        let main = destination.join(RUN_DIR);
        let runs = main.join(RUNS);
        for dir in [&main, &runs] {
            DirBuilder::new().mode(0o700).create(dir)?;
        }
        for file in &native.files {
            copy_replay_file(
                &native.root.join(&file.path),
                &destination.join(&file.path),
                file.bytes,
            )?;
        }
        for dir in [runs.as_path(), main.as_path(), destination] {
            File::open(dir)?.sync_all()?;
        }
        native.check_binding()
    }
}

fn copy_replay_file(source: &Path, destination: &Path, bytes: u64) -> StorageResult<()> {
    let mut output = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(destination)?;
    if node::stream_regular(source, &mut output, "cached mutation replay file length")? != bytes {
        return Err(invalid("cached mutation replay file length"));
    }
    Ok(output.sync_all()?)
}

fn invalid(message: &'static str) -> StorageError {
    StorageError::NativeProof {
        stage: Stage::Validation,
        cause: NativeProofCause::Static(message),
    }
}

fn phase<T>(stage: Stage, result: StorageResult<T>) -> StorageResult<T> {
    result.map_err(|error| error.at_native_proof(stage))
}

fn map(error: laststore::Error) -> StorageError {
    let cause = match error {
        laststore::Error::Corrupt(_) => NativeProofCause::NativeFormat,
        laststore::Error::AeadAuthFail => NativeProofCause::Authentication,
        laststore::Error::Config(msg)
            if msg == "disk run snapshot: another owner holds the home" =>
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

fn fork_bounded(read: &RunRead, bytes: u64, keys: u64) -> StorageResult<RunRead> {
    read.fork_bounded(ReadBudget {
        bytes,
        keys,
        ..read_budget()
    })
    .map_err(map)
}

fn read_budget() -> ReadBudget {
    use lastdb_model::storage_policy::backup_decode as b;
    ReadBudget {
        pages: b::RECORD_INVENTORY_PAGES,
        blocks: b::NATIVE_STREAM_BLOCKS,
        bytes: b::RECORD_INVENTORY_READ_BYTES,
        keys: 10000,
    }
}
