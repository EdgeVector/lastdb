//! Read-only physical and account proof for an inactive native node.
use super::{files, invalid, proof, RunBackupProfile, RunRestoreFile, StorageResult};
use crate::storage::disk_backup::byte_backup::RunBackupHome;
use crate::storage::disk_pack::{run_node, NODE_MARKER};
use laststore::disk_run::home::RunRead;
use std::fs::File;
use std::path::{Path, PathBuf};

/// Retains the node and all checked run-home owner locks without a writer.
pub struct RunNodeGuard {
    pub(super) root: PathBuf,
    directory: File,
    marker: File,
    homes: [files::HomeBinding; 2],
    private: Option<LocalRoot>,
    pub(super) main: RunRead,
    pub(super) cas: RunRead,
    pub(super) profile: RunBackupProfile,
    pub(super) files: Vec<RunRestoreFile>,
}

struct LocalRoot {
    binding: files::HomeBinding,
    read: RunRead,
    profile: RunBackupHome,
}

impl LocalRoot {
    fn open(root: &Path, key: &[u8; 32], budget: u64) -> StorageResult<Self> {
        let name = run_node::PRIVATE_DIR;
        let binding = files::prepare_home(root, name)?;
        let mut read = RunRead::open_existing_bounded(
            &root.join(name),
            &run_node::private_key(key),
            super::read_budget(),
            budget,
        )
        .map_err(super::map)?;
        run_node::verify_private_author(&mut read)?;
        let profile = files::profile(root, name, &read)?;
        Ok(Self {
            binding,
            read,
            profile,
        })
    }

    fn check(&self, root: &Path) -> StorageResult<()> {
        let name = run_node::PRIVATE_DIR;
        self.binding.check(root, name)?;
        check_home(root, name, &self.read, &self.profile)?;
        Ok(())
    }
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
        if let Some(private) = &self.private {
            private.check(&self.root)?;
        }
        files::require_root_members(&self.root, self.private.is_some())
    }

    pub(super) fn open(root: &Path, key: &[u8; 32]) -> StorageResult<Self> {
        Self::open_with_local(root, key, false)
    }

    fn open_with_local(root: &Path, key: &[u8; 32], replayed: bool) -> StorageResult<Self> {
        let directory = files::directory(root)?;
        let marker = files::regular(&root.join(NODE_MARKER), true)?;
        marker
            .try_lock()
            .map_err(|_| invalid("run restore node is owned"))?;
        files::node_marker(root)?;
        let version = files::profile_version(root)?
            .ok_or_else(|| invalid("run restore logical profile is absent"))?;
        let with_private = replayed && version == crate::storage::format::SHARED_RECORD_PROFILE;
        files::require_root_members(root, with_private)?;
        let homes = [
            files::prepare_home(root, run_node::RUN_DIR)?,
            files::prepare_home(root, run_node::CAS_DIR)?,
        ];
        let (mut main, mut cas) =
            super::open_paired_runs(root, key, "paired run metadata exceeds memory admission")?;
        let private = if with_private {
            let budget = lastdb_storage_policy::BACKUP_RESOURCES
                .working_memory_bytes
                .checked_sub(main.retained_bytes())
                .and_then(|bytes| bytes.checked_sub(cas.retained_bytes()))
                .ok_or_else(|| invalid("private run metadata exceeds memory admission"))?;
            Some(LocalRoot::open(root, key, budget)?)
        } else {
            None
        };
        reserve_roots(&mut main, &mut cas, private.as_ref())?;
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
            private,
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

/// Verify the rebuilt device-private state as well as the original shared roots.
pub fn validate_replayed_shared_node(root: &Path, key: &[u8; 32]) -> StorageResult<()> {
    RunNodeGuard::open_with_local(root, key, true).map(|_| ())
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

fn reserve_roots(
    main: &mut RunRead,
    cas: &mut RunRead,
    private: Option<&LocalRoot>,
) -> StorageResult<()> {
    let files = main
        .referenced_runs()
        .count()
        .checked_add(cas.referenced_runs().count())
        .and_then(|count| count.checked_add(5))
        .and_then(|count| {
            count.checked_add(private.map_or(0, |root| root.read.referenced_runs().count() + 2))
        })
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
        .and_then(|bytes| bytes.checked_add(private.map_or(0, |root| root.read.retained_bytes())))
        .and_then(|bytes| {
            bytes.checked_add(lastdb_storage_policy::BACKUP_RESOURCES.stream_buffer_bytes)
        })
        .ok_or_else(|| invalid("paired run memory overflow"))?;
    super::reserve_paired_metadata(main, cas, shared)
}
