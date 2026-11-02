//! Semantic proof against two fixed, authenticated disk V2 roots.
mod blobs;
mod keys;
mod native;
mod ranges;
mod records;
mod rows;

use super::{map, phase, Stage};
use crate::storage::disk_backup::file_dependencies as files;
use crate::storage::error::NativeProofCause;
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
    let reserved = match phase(Stage::Admission, reserve_cursor(main, cas)) {
        Ok(reserved) => reserved,
        Err(error) if descriptor_pressure(&error) => {
            drop(error);
            return verify_once(main, cas, account_key, scratch, version, false);
        }
        Err(error) => return Err(error),
    };
    let key_heads = reserved.is_some();
    let result = {
        let (read_main, read_cas) = reserved
            .as_ref()
            .map_or((main, cas), |(main, cas)| (main, cas));
        verify_once(
            read_main,
            read_cas,
            account_key,
            scratch,
            version,
            key_heads,
        )
    };
    // Every optional inventory cursor and fork must close before nested reads
    // retry. The next attempt repeats exact scratch cleanup and its sync barrier
    // before any account, schema, record, directory or CAS validation.
    drop(reserved);
    if key_heads && result.as_ref().err().is_some_and(descriptor_pressure) {
        drop(result);
        return verify_once(main, cas, account_key, scratch, version, false);
    }
    result
}

fn verify_once(
    main: &RunRead,
    cas: &RunRead,
    account_key: &[u8; 32],
    scratch: &Path,
    version: u16,
    key_heads: bool,
) -> StorageResult<u64> {
    let proof = Proof {
        main,
        cas,
        key: account_key,
        scratch,
        key_heads,
    };
    phase(Stage::Scratch, native::prepare(scratch))?;
    phase(Stage::Account, verify_account(main, account_key, version))?;
    phase(
        Stage::Schemas,
        ranges::verify(main, account_key, version, key_heads),
    )?;
    let proved = phase(Stage::Blobs, blobs::verify(&proof))?;
    phase(Stage::Records, records::verify_records(&proof))?;
    Ok(proved)
}

fn descriptor_pressure(error: &StorageError) -> bool {
    let code = match error {
        StorageError::IoError(io) => io.raw_os_error(),
        StorageError::NativeProof {
            cause: NativeProofCause::Io { os_code, .. },
            ..
        } => *os_code,
        _ => None,
    };
    matches!(code, Some(code) if code == libc::EMFILE || code == libc::ENFILE)
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
    let Some(mut main) = fork_or_skip(main)? else {
        return Ok(None);
    };
    let Some(mut cas) = fork_or_skip(cas)? else {
        return Ok(None);
    };
    super::reserve_paired_metadata(&mut main, &mut cas, shared)?;
    Ok(Some((main, cas)))
}

/// A budget miss skips the optional cursor. Any other fork error stays mapped.
fn fork_or_skip(read: &RunRead) -> StorageResult<Option<RunRead>> {
    match read.fork_bounded(read.budget()) {
        Ok(read) => Ok(Some(read)),
        Err(laststore::Error::ReadBudgetExceeded) => Ok(None),
        Err(error) => Err(map(error)),
    }
}

/// Fork one reader with the caller's budget. The error mapping stays here.
fn fork_mapped(read: &RunRead, budget: ReadBudget) -> StorageResult<RunRead> {
    read.fork_bounded(budget).map_err(map)
}

/// Account admission is separate from the complete record and file proof.
pub(super) fn verify_account(main: &RunRead, key: &[u8; 32], version: u16) -> StorageResult<()> {
    keys::verify(main, key, version)
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
        pages: budget::RECORD_INVENTORY_PAGES,
        blocks: budget::NATIVE_STREAM_BLOCKS,
        bytes,
        keys,
    }
}

fn invalid(detail: &'static str) -> StorageError {
    StorageError::NativeProof {
        stage: Stage::Validation,
        cause: NativeProofCause::Static(detail),
    }
}
