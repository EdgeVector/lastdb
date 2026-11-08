//! Verify an inactive run home without a writer, cleanup, or conversion.
//!
//! A personal backup carries the main home only. Restore installs an empty
//! CAS home ([`install_empty_cas_home`]); record blobs are fetched from their
//! cloud paths on request. A shared database backup still carries its CAS home.
mod files;
mod node;
mod proof;
mod record;
mod replay;

use super::byte_backup::RunBackupProfile;
use crate::storage::disk_pack::run_node;
use crate::storage::error::{NativeProofCause, NativeProofStage as Stage};
use crate::storage::error::{StorageError, StorageResult};
use cloud_backup_protocol::NativeRoot;
use laststore::disk_run::home::{ReadBudget, RunRead};
use std::path::Path;

pub use node::{validate_replayed_shared_node, validate_restored_node, RunNodeGuard};
pub use record::{StagedRecord, StagedRecordQuery};
pub use replay::{verify_replayed, MutationReplay};

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
    cas: Option<&RunRead>,
    account_key: &[u8; 32],
    version: u16,
) -> StorageResult<u64> {
    proof::verify(main, cas, account_key, version)
}

/// Create the empty local CAS home of a restored personal node, after its
/// main home files are in place and before the proof opens the node. An
/// existing CAS home is kept; the proof requires it to be empty.
pub fn install_empty_cas_home(data_root: &Path, account_key: &[u8; 32]) -> StorageResult<()> {
    let path = data_root.join(run_node::CAS_DIR);
    if path.try_exists()? {
        return Ok(());
    }
    let home = laststore::disk_run::home::RunHome::create(&path, &run_node::cas_key(account_key))
        .map_err(map)?;
    drop(home);
    std::fs::File::open(data_root)?.sync_all()?;
    Ok(())
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
        laststore::Error::Config(message)
            if message == "disk run snapshot: another owner holds the home" =>
        {
            NativeProofCause::Static("run restore owner lock busy")
        }
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
