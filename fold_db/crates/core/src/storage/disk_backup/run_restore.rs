//! Verify an inactive paired run home without a writer, cleanup, or conversion.
mod files;
mod node;
mod proof;
mod replay;
mod snapshot_identity;

use super::byte_backup::RunBackupProfile;
use crate::storage::disk_pack::run_node;
use crate::storage::error::{NativeProofCause, NativeProofStage as Stage};
use crate::storage::error::{StorageError, StorageResult};
use cloud_backup_protocol::NativeRoot;
use laststore::disk_run::home::{ReadBudget, RunRead};
use std::path::Path;

pub use node::{validate_restored_node, RunNodeGuard};
pub use replay::{verify_replayed, MutationReplay};
pub use snapshot_identity::{inspect_snapshot_identity, prepare_snapshot_identity};

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
    pub fn verified_rows(&self) -> u64 {
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

    /// Match the authenticated client claim to the actual restored native cut.
    /// This never loads a private signer or enrolls the recorded devices.
    pub fn verify_logical_inclusion(
        &mut self,
        inclusion: &crate::storage::disk_pack::logical_capture::SnapshotInclusion,
    ) -> StorageResult<()> {
        self.check_binding()?;
        crate::storage::disk_pack::logical_capture::verify_snapshot_inclusion(
            &mut self.native.main,
            &self.native.profile,
            inclusion,
        )?;
        self.check_binding()
    }

    pub fn validate_context(&self, data_root: &Path, root: &NativeRoot) -> StorageResult<()> {
        if self.native.root != data_root {
            return Err(invalid("run restore publication root differs"));
        }
        self.native.profile.validate_root(root)?;
        self.check_binding()
    }
}

/// Complete physical closure precedes account and logical record validation.
pub fn verify(
    data_root: &Path,
    profile: &RunBackupProfile,
    native_root: &NativeRoot,
    account_key: &[u8; 32],
    scratch_root: &Path,
) -> StorageResult<RunRestoreGuard> {
    phase(Stage::Profile, profile.validate_root(native_root))?;
    phase(Stage::Scratch, files::directory(scratch_root))?;
    let native = phase(
        Stage::PhysicalRoot,
        RunNodeGuard::open(data_root, account_key),
    )?;
    if native.profile != *profile {
        return Err(invalid("run restore captured profile differs"));
    }
    let records = verify_readers_for_profile(
        &native.main,
        &native.cas,
        account_key,
        scratch_root,
        native.profile.version,
    )?;
    let guard = RunRestoreGuard { native, records };
    phase(Stage::FinalBinding, guard.check_binding())?;
    Ok(guard)
}

pub(crate) fn verify_readers_for_profile(
    main: &RunRead,
    cas: &RunRead,
    account_key: &[u8; 32],
    scratch_root: &Path,
    version: u16,
) -> StorageResult<u64> {
    phase(Stage::Scratch, files::directory(scratch_root))?;
    proof::verify(main, cas, account_key, scratch_root, version)
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

/// Classify native reader types before the ordinary adapter flattens their text.
fn map(error: laststore::Error) -> StorageError {
    let cause = match error {
        laststore::Error::Corrupt(_) => NativeProofCause::NativeFormat,
        laststore::Error::AeadAuthFail => NativeProofCause::Authentication,
        laststore::Error::Config(_) => NativeProofCause::Configuration,
        laststore::Error::NativeProof(code) => NativeProofCause::Static(code),
        other => return crate::storage::disk_pack::map(other),
    };
    StorageError::NativeProof {
        stage: Stage::NativeReader,
        cause,
    }
}

/// Open the main run first. The cas run then uses the remaining memory budget.
fn open_paired_runs(
    root: &Path,
    key: &[u8; 32],
    admission_detail: &'static str,
) -> StorageResult<(RunRead, RunRead)> {
    let maximum = lastdb_storage_policy::BACKUP_RESOURCES.working_memory_bytes;
    let main = RunRead::open_existing_bounded(
        &root.join(run_node::RUN_DIR),
        &run_node::home_key(key),
        read_budget(),
        maximum,
    )
    .map_err(map)?;
    let remaining = maximum
        .checked_sub(main.retained_bytes())
        .ok_or_else(|| invalid(admission_detail))?;
    let cas = RunRead::open_existing_bounded(
        &root.join(run_node::CAS_DIR),
        &run_node::cas_key(key),
        read_budget(),
        remaining,
    )
    .map_err(map)?;
    Ok((main, cas))
}

/// Charge both readers with one shared byte count. The main reader is first.
fn reserve_paired_metadata(
    main: &mut RunRead,
    cas: &mut RunRead,
    shared: u64,
) -> StorageResult<()> {
    main.reserve_metadata(shared).map_err(map)?;
    cas.reserve_metadata(shared).map_err(map)
}

fn read_budget() -> ReadBudget {
    ReadBudget {
        pages: lastdb_storage_policy::backup_decode::RECORD_INVENTORY_PAGES,
        blocks: lastdb_storage_policy::backup_decode::NATIVE_STREAM_BLOCKS,
        bytes: lastdb_storage_policy::backup_decode::RECORD_INVENTORY_READ_BYTES,
        keys: 10000,
    }
}
