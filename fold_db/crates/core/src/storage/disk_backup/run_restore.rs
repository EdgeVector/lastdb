//! Verify an inactive run home without a writer, cleanup, or conversion.

mod database;
mod node;
mod record;

use super::byte_backup::RunBackupProfile;
use crate::storage::disk_backup::mutation_log::{Frontiers, LogAnchor, LogEntry, LogPosition};
use crate::storage::disk_pack::{run_node, RUN_DIR};
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

pub type RunRestoreGuard = RunNodeGuard;

pub(super) fn sum_file_bytes(files: &[(String, u64)], msg: &'static str) -> StorageResult<u64> {
    files
        .iter()
        .try_fold(0u64, |s, (_, b)| s.checked_add(*b))
        .ok_or_else(|| invalid(msg))
}

pub fn verify(
    data_root: &Path,
    profile: &RunBackupProfile,
    native_root: &NativeRoot,
    account_key: &[u8; 32],
) -> StorageResult<RunRestoreGuard> {
    phase(Stage::Profile, profile.validate_root(native_root))?;
    finish_restore_guard(
        data_root,
        account_key,
        |n| &n.profile == profile,
        "run restore captured profile differs",
    )
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
    finish_restore_guard(
        data,
        key,
        |n| {
            n.profile.main.home_id == anchor.main_home
                && n.profile.main.frontier.get() == through.frontiers.main
        },
        "mutation replay exact final frontier",
    )
}

fn finish_restore_guard(
    data_root: &Path,
    key: &[u8; 32],
    check: impl FnOnce(&RunNodeGuard) -> bool,
    msg: &'static str,
) -> StorageResult<RunRestoreGuard> {
    let native = phase(
        Stage::PhysicalRoot,
        RunNodeGuard::open(data_root, key, false),
    )?;
    if !check(&native) {
        return Err(invalid(msg));
    }
    native.require_empty_local_cas()?;
    phase(Stage::FinalBinding, native.check_binding()).map(|()| native)
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
        let (_, _node, _) = node::lock_node(data, false, "mutation replay node is owned")?;
        let main =
            RunHome::open(&data.join(run_node::RUN_DIR), &run_node::home_key(key)).map_err(map)?;
        (main.identity().to_string() == anchor.main_home.as_str())
            .then_some(Self { main, _node })
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
        let prev = part_index
            .checked_sub(1)
            .and_then(|i| entry.parts.get(i))
            .map_or(entry.before.main, |p| p.frontier);
        let cur = self.frontiers().main;
        if cur != prev && cur != part.frontier {
            return Err(invalid("mutation replay exact partial frontier"));
        }
        let applied = self
            .main
            .replay_commit_file(source, prev, part.frontier)
            .map_err(map)?;
        (applied.run_id.to_string() == part.run_id.as_str()
            && applied.run_bytes == part.bytes
            && self.frontiers().main == part.frontier)
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
        let pol = laststore::space::SpacePolicy::default();
        let _space = laststore::space::reserve_space(destination, &pol, bytes).map_err(map)?;
        let (main, runs) = (
            destination.join(RUN_DIR),
            destination.join(RUN_DIR).join(RUNS),
        );
        for dir in [&main, &runs] {
            DirBuilder::new().mode(0o700).create(dir)?;
        }
        for (rel, expected_bytes) in &native.files {
            let mut out = OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .custom_flags(libc::O_NOFOLLOW)
                .open(destination.join(rel))?;
            let n = node::stream_regular(
                &native.root.join(rel),
                &mut out,
                "cached mutation replay file length",
            )?;
            if n != *expected_bytes {
                return Err(invalid("cached mutation replay file length"));
            }
            out.sync_all()?;
        }
        for dir in [runs.as_path(), main.as_path(), destination] {
            File::open(dir)?.sync_all()?;
        }
        native.check_binding()
    }
}

pub(super) fn invalid(message: &'static str) -> StorageError {
    StorageError::NativeProof {
        stage: Stage::Validation,
        cause: NativeProofCause::Static(message),
    }
}

fn phase<T>(stage: Stage, result: StorageResult<T>) -> StorageResult<T> {
    result.map_err(|error| error.at_native_proof(stage))
}

pub(super) fn map(error: laststore::Error) -> StorageError {
    let cause = match error {
        laststore::Error::Corrupt(_) => NativeProofCause::NativeFormat,
        laststore::Error::AeadAuthFail => NativeProofCause::Authentication,
        laststore::Error::Config(m) if m == "disk run snapshot: another owner holds the home" => {
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

pub(super) fn fork_bounded(read: &RunRead, bytes: u64, keys: u64) -> StorageResult<RunRead> {
    read.fork_bounded(ReadBudget {
        bytes,
        keys,
        ..read_budget()
    })
    .map_err(map)
}

pub(super) fn read_budget() -> ReadBudget {
    use lastdb_model::storage_policy::backup_decode as b;
    ReadBudget {
        pages: b::RECORD_INVENTORY_PAGES,
        blocks: b::NATIVE_STREAM_BLOCKS,
        bytes: b::RECORD_INVENTORY_READ_BYTES,
        keys: 10000,
    }
}
