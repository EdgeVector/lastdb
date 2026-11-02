//! Read-only physical and account proof for an inactive disk V2 node.
use super::{files, invalid, proof, RunBackupProfile, RunRestoreFile, StorageResult};
use crate::storage::disk_backup::byte_backup::RunBackupHome;
use crate::storage::disk_pack::{run_node, NODE_MARKER};
use laststore::disk_run::home::RunRead;
use std::fs::File;
use std::path::{Path, PathBuf};

/// Retains the node owner and both run-home owner locks without any writer.
pub struct RunNodeGuard {
    pub(super) root: PathBuf,
    directory: File,
    marker: File,
    homes: [files::HomeBinding; 2],
    pub(super) main: RunRead,
    pub(super) cas: RunRead,
    pub(super) profile: RunBackupProfile,
    pub(super) files: Vec<RunRestoreFile>,
}

impl RunNodeGuard {
    pub fn check_binding(&self) -> StorageResult<()> {
        files::same_entry(&self.directory, &self.root)?;
        files::same_entry(&self.marker, &self.root.join(NODE_MARKER))?;
        files::node_marker(&self.root)?;
        if files::profile_version(&self.root)? != Some(self.profile.version) {
            return Err(invalid("run restore logical profile changed"));
        }
        for (binding, name) in self
            .homes
            .iter()
            .zip([run_node::RUN_DIR, run_node::CAS_DIR])
        {
            binding.check(&self.root, name)?;
        }
        check_home(
            &self.root,
            run_node::RUN_DIR,
            &self.main,
            &self.profile.main,
        )?;
        check_home(&self.root, run_node::CAS_DIR, &self.cas, &self.profile.cas)?;
        files::require_root_members(&self.root)
    }

    pub(super) fn open(root: &Path, key: &[u8; 32]) -> StorageResult<Self> {
        let directory = files::directory(root)?;
        files::require_root_members(root)?;
        let marker = files::regular(&root.join(NODE_MARKER), true)?;
        marker
            .try_lock()
            .map_err(|_| invalid("run restore node is owned"))?;
        files::node_marker(root)?;
        let version = files::profile_version(root)?
            .ok_or_else(|| invalid("run restore logical profile is absent"))?;
        let homes = [
            files::prepare_home(root, run_node::RUN_DIR)?,
            files::prepare_home(root, run_node::CAS_DIR)?,
        ];
        let (mut main, mut cas) =
            super::open_paired_runs(root, key, "paired run metadata exceeds memory admission")?;
        reserve_pair(&mut main, &mut cas)?;
        let profile = files::backup_profile(root, version, &main, &cas)?;
        let mut entries = vec![RunRestoreFile {
            path: NODE_MARKER.into(),
            bytes: profile.node_marker()?.len() as u64,
        }];
        entries.extend(check_home(root, run_node::RUN_DIR, &main, &profile.main)?);
        entries.extend(check_home(root, run_node::CAS_DIR, &cas, &profile.cas)?);
        super::phase(
            super::Stage::Account,
            proof::verify_account(&main, key, version),
        )?;
        let guard = Self {
            root: root.to_path_buf(),
            directory,
            marker,
            homes,
            main,
            cas,
            profile,
            files: entries,
        };
        guard.check_binding()?;
        Ok(guard)
    }
}

/// Validate a restored node. This path performs no scratch, record,
/// directory, or source-file writes.
pub fn validate_restored_node(root: &Path, key: &[u8; 32]) -> StorageResult<RunNodeGuard> {
    RunNodeGuard::open(root, key)
}

fn check_home(
    root: &Path,
    name: &str,
    reader: &RunRead,
    expected: &RunBackupHome,
) -> StorageResult<Vec<RunRestoreFile>> {
    if reader.home_id().to_string() != expected.home_id.as_str()
        || reader.generation() != expected.generation.get()
        || reader.frontier() != expected.frontier.get()
    {
        return Err(invalid("run restore captured home differs"));
    }
    files::home(root, name, reader, expected)
}

fn reserve_pair(main: &mut RunRead, cas: &mut RunRead) -> StorageResult<()> {
    let files = main
        .referenced_runs()
        .count()
        .checked_add(cas.referenced_runs().count())
        .and_then(|count| count.checked_add(5))
        .ok_or_else(|| invalid("paired run file count overflow"))?;
    // The fixed-length paths, returned inventory, and temporary closure maps
    // use four existing identity charges per file before any map allocation.
    let inventories = (files as u64)
        .checked_mul(4 * lastdb_storage_policy::backup_decode::IDENTITY_MEMORY_BYTES)
        .ok_or_else(|| invalid("paired run inventory memory overflow"))?;
    let shared = main
        .retained_bytes()
        .checked_add(cas.retained_bytes())
        .and_then(|bytes| bytes.checked_add(inventories))
        .and_then(|bytes| {
            bytes.checked_add(lastdb_storage_policy::BACKUP_RESOURCES.stream_buffer_bytes)
        })
        .ok_or_else(|| invalid("paired run memory overflow"))?;
    super::reserve_paired_metadata(main, cas, shared)
}
