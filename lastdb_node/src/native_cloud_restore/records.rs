//! Offline native inventory proof. No user query or native record rewrite occurs.
use super::{dependencies, RestoreError, RestoreSession, Result};
use cloud_backup_protocol::Digest;
use fold_db::storage::disk_pack::{file_dependencies as directory, file_recovery};
use lastdb_storage_policy::backup_decode::{
    ATOM_BODY_MAX_BYTES, ATOM_INVENTORY_PAGES, DIRECTORY_READ_BYTES, FILE_COVERAGE_IDENTITY_BYTES,
    FILE_COVERAGE_MEMORY_BYTES, NATIVE_STREAM_BLOCKS,
};
use laststore::disk_pack::home::DiskHome;
use laststore::disk_pack::ReadBudget;
use std::collections::BTreeSet;
mod native_file;

// The two charged atom JSON trees use 64 MiB; release them before any CAS decode.
// This separate 8 MiB identity cap leaves the complete active proof below 96 MiB.
const COVERAGE_MAX: usize = FILE_COVERAGE_MEMORY_BYTES / FILE_COVERAGE_IDENTITY_BYTES;

pub(super) fn admit_count(external_count: u64) -> Result<()> {
    if external_count > COVERAGE_MAX as u64 {
        return Err(RestoreError::Limit("file coverage identity memory"));
    }
    Ok(())
}

pub(super) fn verify(session: &RestoreSession, key: &[u8; 32], external_count: u64) -> Result<u64> {
    admit_count(external_count)?;
    let home =
        DiskHome::open(&session.physical()).map_err(|_| RestoreError::Invalid("native home"))?;
    let mut atoms = 0u64;
    let mut covered = BTreeSet::new();
    for group in 0..1024u16 {
        let mut start = Vec::new();
        loop {
            let page = {
                let mut read = home
                    .read(budget(directory::ATOM_INVENTORY_READ_BYTES, 10000))
                    .map_err(|_| RestoreError::Limit("native atom read"))?;
                let mut cursor = directory::atom_inventory(&mut read, group, &start)
                    .map_err(|_| RestoreError::Limit("native atom inventory"))?;
                cursor
                    .next_page_yielding_bounded(1, 0, 0, ATOM_BODY_MAX_BYTES)
                    .map_err(|_| RestoreError::Limit("native atom page"))?
            }; // No native reader/block cache survives the atom decode.
            let next = page.resume.clone().or_else(|| {
                page.records
                    .last()
                    .map(|record| record.key.as_bytes().to_vec())
            });
            for record in page.records {
                fold_db::storage::disk_pack::validate_restored_atom_key(&record.key)
                    .map_err(|_| RestoreError::Invalid("native atom key format"))?;
                let Some((scope, id)) = directory::atom_coordinate(&record.key)
                    .map_err(|_| RestoreError::Invalid("native atom coordinate"))?
                else {
                    continue;
                };
                let atom = directory::open_atom(key, &record.body)
                    .map_err(|_| RestoreError::Invalid("native atom envelope"))?;
                if atom.uuid() != id {
                    return Err(RestoreError::Invalid("atom identity"));
                }
                let expected = directory::expected(&atom)
                    .map_err(|_| RestoreError::Unsupported("uncertified native file reference"))?;
                drop(atom);
                drop(record);
                let mut read = home
                    .read(budget(DIRECTORY_READ_BYTES, 3))
                    .map_err(|_| RestoreError::Limit("native file directory"))?;
                directory::read_directories(&mut read, scope.as_deref(), &id, &expected, key)
                    .map_err(|_| RestoreError::Invalid("native file directory coverage"))?;
                drop(read);
                for dependency in expected {
                    verify_dependency(session, &home, key, &dependency, &mut covered)?;
                }
                atoms = atoms
                    .checked_add(1)
                    .ok_or(RestoreError::Limit("atom inventory"))?;
            }
            if page.exhausted {
                break;
            }
            let mut next = next.ok_or(RestoreError::Invalid("atom continuation"))?;
            if next.len() >= 65535 || (!start.is_empty() && next < start) {
                return Err(RestoreError::Invalid("atom continuation"));
            }
            next.push(0);
            start = next;
        }
    }
    if covered.len() as u64 != external_count {
        return Err(RestoreError::Invalid("exact external file coverage"));
    }
    Ok(atoms)
}

pub(super) fn verify_metadata(session: &RestoreSession, key: &[u8; 32]) -> Result<()> {
    // The coverage set from verify() is gone before this separate bounded pass.
    let physical = session.physical();
    let bytes = super::download::bytes(&physical.join("CURRENT"), 4096)?;
    if cloud_backup_protocol::bytes_sha256(&bytes) != session.accepted().native_root.current_sha256
    {
        return Err(RestoreError::Invalid("metadata accepted native root"));
    }
    let fixed: [u8; 4096] = bytes
        .try_into()
        .map_err(|_| RestoreError::Invalid("metadata native root size"))?;
    let current = laststore::disk_pack::Current::decode(&fixed)
        .map_err(|_| RestoreError::Invalid("metadata native root"))?;
    let home = DiskHome::open(&physical).map_err(|_| RestoreError::Invalid("native home"))?;
    directory::verify_metadata_coverage(&current, key, |budget| {
        home.read_committed(current.clone(), budget).map_err(|_| {
            fold_db::storage::StorageError::InvalidOperation("metadata native read".into())
        })
    })
    .map_err(|_| RestoreError::Unsupported("uncertified live file metadata"))?;
    Ok(())
}

fn verify_dependency(
    session: &RestoreSession,
    home: &DiskHome,
    key: &[u8; 32],
    dependency: &directory::FileDependency,
    covered: &mut BTreeSet<[u8; 32]>,
) -> Result<()> {
    if native_file::verify(session, home, key, dependency)? {
        return Ok(());
    }
    let file = file_recovery::open_verified_ciphertext(
        &dependencies::data_root(session),
        &session.accepted().native_root.home_id,
        dependency.logical_pointer_sha256(),
        dependency.kind(),
        dependency.access(),
    )
    .map_err(|_| RestoreError::Invalid("backup-owned file proof"))?
    .ok_or(RestoreError::Invalid("missing backup-owned file source"))?;
    drop(file);
    let identity = file_recovery::entry_key(dependency.logical_pointer_sha256(), dependency.kind());
    let hash = cloud_backup_protocol::bytes_sha256(identity.as_bytes());
    let raw = raw_digest(&hash)?;
    if !covered.contains(&raw) && covered.len() == COVERAGE_MAX {
        return Err(RestoreError::Limit("file coverage identity memory"));
    }
    covered.insert(raw);
    Ok(())
}

fn budget(bytes: u64, keys: u64) -> ReadBudget {
    ReadBudget {
        pages: ATOM_INVENTORY_PAGES,
        blocks: NATIVE_STREAM_BLOCKS,
        bytes,
        keys,
    }
}
fn raw_digest(digest: &Digest) -> Result<[u8; 32]> {
    hex::decode(digest.as_str())
        .map_err(|_| RestoreError::Invalid("digest"))?
        .try_into()
        .map_err(|_| RestoreError::Invalid("digest"))
}
