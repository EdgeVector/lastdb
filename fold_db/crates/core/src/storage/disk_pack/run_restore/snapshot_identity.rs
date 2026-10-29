//! Prepare only an inactive receiver identity. No publication or source write occurs.

use super::{files, invalid, map, proof, RunBackupProfile, RunNodeGuard, StorageResult};
use crate::storage::disk_pack::{
    logical_capture::{
        snapshot_identity as logical, SnapshotAdoptionCandidate, SnapshotIdentityReport,
    },
    physical, run_node, transaction, NativeCloudWorkGuard, NODE_MARKER,
};
use laststore::disk_run::home::{ReadBudget, RunRead, RunReadOwner};
use std::fs::File;
use std::path::{Path, PathBuf};

const WORK_BYTES: u64 = 1024 * 1024;

struct SourceLocks {
    root: PathBuf,
    directory: File,
    marker: File,
    control: File,
    bindings: Vec<(PathBuf, File)>,
    _homes: [RunReadOwner; 2],
}

impl SourceLocks {
    fn check(&self) -> StorageResult<()> {
        files::same_entry(&self.directory, &self.root)?;
        files::same_entry(&self.marker, &self.root.join(NODE_MARKER))?;
        files::same_entry(&self.control, &self.root.join(logical::CONTROL_FILE))?;
        for (path, file) in &self.bindings {
            files::same_entry(file, path)?;
        }
        Ok(())
    }
}

/// Copy the receiver's signer only into an inactive, fully verified destination.
/// The destination's distinct prepared state never permits normal startup.
pub fn prepare_snapshot_identity(
    source: &Path,
    target: &Path,
    key: &[u8; 32],
    candidate: &SnapshotAdoptionCandidate,
    scratch: &Path,
) -> StorageResult<SnapshotIdentityReport> {
    let _admission = NativeCloudWorkGuard::acquire()?;
    separate_paths(source, target, scratch)?;
    let (source, source_locks) = read_source(source, key)?;
    if logical::has_prepared_control(target)? {
        let result = inspect_target(&source, target, key, candidate, scratch);
        source_locks.check()?;
        return result;
    }
    let mut verified = super::verify(
        target,
        &candidate.run_profile,
        &candidate.native_root,
        key,
        scratch,
    )?;
    let inclusion = candidate
        .logical_inclusion
        .as_ref()
        .ok_or_else(|| invalid("identity preparation requires certified candidate coverage"))?;
    verified.verify_logical_inclusion(inclusion)?;
    let operations = logical::prepare_mutations(
        &source,
        candidate,
        &mut verified.native.main,
        &verified.native.profile,
    )?;
    verified.check_binding()?;
    source_locks.check()?;
    let (v2, directory, marker) = verified.native.into_identity_writer()?;
    let logical_bytes = transaction::batch_bytes(&operations, physical::V2_BATCH_OPERATIONS)?;
    if logical_bytes > WORK_BYTES {
        return Err(invalid("snapshot identity control batch exceeds one MiB"));
    }
    let policy = super::super::space::policy_from_env()?;
    let reserve = v2
        .home
        .read(read_budget())
        .retained_bytes()
        .checked_add(logical_bytes)
        .and_then(|bytes| bytes.checked_add(2 * WORK_BYTES))
        .ok_or_else(|| invalid("snapshot identity space admission overflow"))?;
    let _reservation = laststore::space::reserve_space(target, &policy, reserve).map_err(map)?;
    // CURRENT commits every public control record together. Before private
    // publication there is no signer; afterward its prepared state refuses boot.
    v2.home.commit(operations).map_err(map)?;
    let profile = writer_profile(target, &v2)?;
    files::same_entry(&directory, target)?;
    files::same_entry(&marker, &target.join(NODE_MARKER))?;
    logical::save_prepared(&source, target, &profile)?;
    drop(v2);
    drop(marker);
    drop(directory);
    let result = inspect_target(&source, target, key, candidate, scratch);
    source_locks.check()?;
    result
}

/// Reopen the completed inactive preparation without changing either home.
pub fn inspect_snapshot_identity(
    source: &Path,
    target: &Path,
    key: &[u8; 32],
    candidate: &SnapshotAdoptionCandidate,
    scratch: &Path,
) -> StorageResult<SnapshotIdentityReport> {
    let _admission = NativeCloudWorkGuard::acquire()?;
    separate_paths(source, target, scratch)?;
    let (source, source_locks) = read_source(source, key)?;
    let result = inspect_target(&source, target, key, candidate, scratch);
    source_locks.check()?;
    result
}

fn inspect_target(
    source: &logical::SourceIdentity,
    target: &Path,
    key: &[u8; 32],
    candidate: &SnapshotAdoptionCandidate,
    scratch: &Path,
) -> StorageResult<SnapshotIdentityReport> {
    let mut guard = RunNodeGuard::open_identity(target, key)?;
    super::verify_readers_for_profile(
        &guard.main,
        &guard.cas,
        key,
        scratch,
        guard.profile.version,
    )?;
    let result =
        logical::inspect_prepared(source, candidate, target, &mut guard.main, &guard.profile)?;
    guard.check_binding()?;
    Ok(result)
}

fn read_source(
    root: &Path,
    key: &[u8; 32],
) -> StorageResult<(logical::SourceIdentity, SourceLocks)> {
    let directory = files::directory(root)?;
    let marker = files::regular(&root.join(NODE_MARKER), true)?;
    marker
        .try_lock()
        .map_err(|_| invalid("snapshot identity source is owned"))?;
    if files::profile_version(root)? != Some(2) {
        return Err(invalid("snapshot identity source format differs"));
    }
    let bindings = source_bindings(root)?;
    let control = files::regular(&root.join(logical::CONTROL_FILE), false)?;
    let maximum = lastdb_storage_policy::BACKUP_RESOURCES.working_memory_bytes;
    let mut main = RunRead::open_existing_bounded(
        &root.join(run_node::RUN_DIR),
        &run_node::home_key(key),
        read_budget(),
        maximum,
    )
    .map_err(map)?;
    let remaining = maximum
        .checked_sub(main.retained_bytes())
        .ok_or_else(|| invalid("snapshot identity source exceeds memory admission"))?;
    let mut cas = RunRead::open_existing_bounded(
        &root.join(run_node::CAS_DIR),
        &run_node::cas_key(key),
        read_budget(),
        remaining,
    )
    .map_err(map)?;
    let shared = main
        .retained_bytes()
        .checked_add(cas.retained_bytes())
        .and_then(|bytes| bytes.checked_add(WORK_BYTES))
        .ok_or_else(|| invalid("snapshot identity source memory overflow"))?;
    main.reserve_metadata(shared).map_err(map)?;
    cas.reserve_metadata(shared).map_err(map)?;
    proof::verify_account(&main, key, 2)?;
    let profile = reader_profile(root, &main, &cas)?;
    let source = logical::read_source(root, &mut main, &profile)?;
    files::same_entry(&directory, root)?;
    files::same_entry(&marker, &root.join(NODE_MARKER))?;
    Ok((
        source,
        SourceLocks {
            root: root.to_path_buf(),
            directory,
            marker,
            control,
            bindings,
            _homes: [main.into_owner_guard(), cas.into_owner_guard()],
        },
    ))
}

fn source_bindings(root: &Path) -> StorageResult<Vec<(PathBuf, File)>> {
    let mut bindings = Vec::with_capacity(10);
    for name in [run_node::RUN_DIR, run_node::CAS_DIR] {
        for child in ["", "runs"] {
            let path = root.join(name).join(child);
            let file = files::directory(&path)?;
            bindings.push((path, file));
        }
        for child in [
            "LOCK",
            "CURRENT",
            laststore::disk_run::home::RUN_HOME_MARKER,
        ] {
            let path = root.join(name).join(child);
            let file = files::regular(&path, false)?;
            bindings.push((path, file));
        }
    }
    Ok(bindings)
}

fn reader_profile(root: &Path, main: &RunRead, cas: &RunRead) -> StorageResult<RunBackupProfile> {
    let profile = RunBackupProfile {
        version: 2,
        main: files::profile_home(root, run_node::RUN_DIR, main)?,
        cas: files::profile_home(root, run_node::CAS_DIR, cas)?,
    };
    profile.validate()?;
    Ok(profile)
}

fn writer_profile(root: &Path, v2: &physical::V2Home) -> StorageResult<RunBackupProfile> {
    reader_profile(
        root,
        &v2.home.read(read_budget()),
        &v2.cas.read(read_budget()),
    )
}

fn separate_paths(source: &Path, target: &Path, scratch: &Path) -> StorageResult<()> {
    if source.starts_with(target)
        || target.starts_with(source)
        || [source, target]
            .iter()
            .any(|root| scratch.starts_with(root) || root.starts_with(scratch))
    {
        return Err(invalid(
            "snapshot identity homes and scratch must be separate",
        ));
    }
    files::directory(source)?;
    files::directory(target)?;
    files::directory(scratch)?;
    Ok(())
}

fn read_budget() -> ReadBudget {
    ReadBudget {
        pages: lastdb_storage_policy::backup_decode::ATOM_INVENTORY_PAGES,
        blocks: lastdb_storage_policy::backup_decode::NATIVE_STREAM_BLOCKS,
        bytes: lastdb_storage_policy::backup_decode::ATOM_INVENTORY_READ_BYTES,
        keys: 10000,
    }
}
