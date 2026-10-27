//! Semantic proof against two fixed, authenticated disk V2 roots.
mod atoms;
mod keys;
mod metadata;
mod native;
mod rows;

use crate::storage::disk_pack::{file_dependencies as files, map};
use crate::storage::error::StorageResult;
use crate::storage::StorageError;
use lastdb_storage_policy::backup_decode as budget;
use laststore::disk_pack::ReadBudget;
use laststore::disk_run::home::RunRead;
use std::path::Path;

/// The caller authenticates both native inventories and keeps their readers
/// held throughout this proof. No live home or source path is opened here.
pub(super) fn verify(
    main: &RunRead,
    cas: &RunRead,
    account_key: &[u8; 32],
    scratch: &Path,
) -> StorageResult<u64> {
    let reserved = reserve_cursor(main, cas)?;
    let key_heads = reserved.is_some();
    let (main, cas) = reserved
        .as_ref()
        .map_or((main, cas), |(main, cas)| (main, cas));
    let proof = Proof {
        main,
        cas,
        key: account_key,
        scratch,
        key_heads,
    };
    native::prepare(scratch)?;
    verify_account(main, account_key)?;
    let atoms = atoms::verify(&proof)?;
    metadata::verify(&proof)?;
    Ok(atoms)
}

fn reserve_cursor(main: &RunRead, cas: &RunRead) -> StorageResult<Option<(RunRead, RunRead)>> {
    // Both proof forks and the single long-lived inventory fork coexist with
    // nested directory and CAS reads. The manifest charge is conservative:
    // those forks share its Arc, but every reader has its own run-slot vector.
    let shared = main
        .reserved_metadata_bytes()
        .max(cas.reserved_metadata_bytes())
        .checked_add(main.retained_bytes())
        .and_then(|n| n.checked_add(main.retained_bytes()))
        .and_then(|n| n.checked_add(cas.retained_bytes()))
        .and_then(|n| n.checked_add(budget::PROOF_KEY_HEAD_BYTES));
    let Some(shared) = shared else {
        return Ok(None);
    };
    // Optional acceleration must not tighten the old proof's admission. Leave
    // its complete peak allowance available, or use the original stateless read.
    if shared
        .checked_add(lastdb_storage_policy::backup_memory::PROOF_PEAK_BYTES)
        .is_none_or(|n| n > lastdb_storage_policy::BACKUP_RESOURCES.working_memory_bytes)
    {
        return Ok(None);
    }
    let mut main = match main.fork_bounded(main.budget()) {
        Ok(read) => read,
        Err(laststore::Error::ReadBudgetExceeded) => return Ok(None),
        Err(error) => return Err(map(error)),
    };
    let mut cas = match cas.fork_bounded(cas.budget()) {
        Ok(read) => read,
        Err(laststore::Error::ReadBudgetExceeded) => return Ok(None),
        Err(error) => return Err(map(error)),
    };
    main.reserve_metadata(shared).map_err(map)?;
    cas.reserve_metadata(shared).map_err(map)?;
    Ok(Some((main, cas)))
}

/// Account admission is separate from the complete atom and file proof.
pub(super) fn verify_account(main: &RunRead, key: &[u8; 32]) -> StorageResult<()> {
    keys::verify(main, key)
}

struct Proof<'a> {
    main: &'a RunRead,
    cas: &'a RunRead,
    key: &'a [u8; 32],
    scratch: &'a Path,
    key_heads: bool,
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
