//! Semantic proof against two fixed, authenticated disk V2 roots.
mod atoms;
mod keys;
mod metadata;
mod native;
mod ranges;
mod rows;

use crate::storage::disk_pack::{file_dependencies as files, map};
use crate::storage::error::StorageResult;
use crate::storage::StorageError;
use lastdb_storage_policy::backup_decode as budget;
use laststore::disk_run::home::ReadBudget;
use laststore::disk_run::home::RunRead;
use std::path::Path;

/// The caller authenticates both native inventories and keeps their readers
/// held throughout this proof. No live home or source path is opened here.
pub(super) fn verify(
    main: &RunRead,
    cas: &RunRead,
    account_key: &[u8; 32],
    scratch: &Path,
    version: u16,
) -> StorageResult<u64> {
    let proof = Proof {
        main,
        cas,
        key: account_key,
        scratch,
    };
    native::prepare(scratch)?;
    verify_account(main, account_key, version)?;
    ranges::verify(main, account_key, version)?;
    let atoms = atoms::verify(&proof)?;
    metadata::verify(&proof)?;
    Ok(atoms)
}

/// Account admission is separate from the complete atom and file proof.
pub(super) fn verify_account(main: &RunRead, key: &[u8; 32], version: u16) -> StorageResult<()> {
    keys::verify(main, key, version)
}

struct Proof<'a> {
    main: &'a RunRead,
    cas: &'a RunRead,
    key: &'a [u8; 32],
    scratch: &'a Path,
}

fn read_budget(bytes: u64, keys: u64) -> ReadBudget {
    ReadBudget {
        pages: budget::ATOM_INVENTORY_PAGES,
        blocks: budget::NATIVE_STREAM_BLOCKS,
        bytes,
        keys,
    }
}

fn invalid(detail: &str) -> StorageError {
    StorageError::InvalidOperation(detail.into())
}
