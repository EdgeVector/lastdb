//! Read-only physical and account proof for an inactive native node.

use super::{
    files, invalid, map, open_paired_runs, phase, read_budget, reserve_paired_metadata,
    RunBackupProfile, RunRestoreFile, Stage, StorageResult,
};
use crate::storage::content_key_proof;
use crate::storage::disk_backup::byte_backup::RunBackupHome;
use crate::storage::disk_pack::{native_routing, run_node, NODE_MARKER};
use crate::storage::format::control::{
    validate_frozen_replay_floor, CURRENT_KEY_FORMAT_KEY, DELETE_REPLAY_CUTOFF_KEY,
};
use crate::storage::format::metadata::{validate_local_author, LOCAL_AUTHOR_KEY};
use crate::storage::format::{
    METADATA_NAMESPACE, RECORDS_COLLECTION, RECORD_PROFILE, SHARED_RECORD_PROFILE,
};
use lastdb_model::storage_policy::backup_decode as budget;
use laststore::disk_run::home::{ReadBudget, RunRead};
use std::fs::File;
use std::path::{Path, PathBuf};

/// Retains the node and all checked run-home owner locks without a writer.
pub struct RunNodeGuard {
    pub(in crate::storage::disk_backup::run_restore) root: PathBuf,
    directory: File,
    marker: File,
    homes: [files::HomeBinding; 2],
    private: Option<LocalRoot>,
    pub(in crate::storage::disk_backup::run_restore) main: RunRead,
    pub(in crate::storage::disk_backup::run_restore) cas: RunRead,
    pub(in crate::storage::disk_backup::run_restore) profile: RunBackupProfile,
    pub(in crate::storage::disk_backup::run_restore) files: Vec<RunRestoreFile>,
}

struct LocalRoot {
    binding: files::HomeBinding,
    read: RunRead,
    profile: RunBackupHome,
}

#[derive(Clone, Copy)]
enum NodeOpen {
    Exclusive,
    Replayed,
    Snapshot,
}

impl LocalRoot {
    fn open(root: &Path, key: &[u8; 32], budget: u64) -> StorageResult<Self> {
        let name = run_node::PRIVATE_DIR;
        let binding = files::prepare_home(root, name)?;
        let mut read = RunRead::open_existing_bounded(
            &root.join(name),
            &run_node::private_key(key),
            read_budget(),
            budget,
        )
        .map_err(map)?;
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
    /// The CAS reader the proof checks file rows in: only a shared database
    /// backs up its CAS home.
    pub(in crate::storage::disk_backup::run_restore) fn backed_up_cas(&self) -> Option<&RunRead> {
        self.profile.cas.as_ref().map(|_| &self.cas)
    }

    /// A restored personal home holds no record blob rows: each blob is
    /// fetched from its cloud path on its first request.
    pub(in crate::storage::disk_backup::run_restore) fn require_empty_local_cas(
        &self,
    ) -> StorageResult<()> {
        if self.profile.cas.is_none() && self.cas.referenced_runs().next().is_some() {
            return Err(invalid("restored personal CAS home is not empty"));
        }
        Ok(())
    }

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
        if let Some(cas) = &self.profile.cas {
            check_home(&self.root, run_node::CAS_DIR, &self.cas, cas)?;
        }
        if let Some(private) = &self.private {
            private.check(&self.root)?;
        }
        files::require_root_members(&self.root, self.private.is_some())
    }

    pub(in crate::storage::disk_backup::run_restore) fn open(
        root: &Path,
        key: &[u8; 32],
    ) -> StorageResult<Self> {
        Self::open_mode(root, key, NodeOpen::Exclusive)
    }

    pub(in crate::storage::disk_backup::run_restore) fn open_snapshot(
        root: &Path,
        key: &[u8; 32],
    ) -> StorageResult<Self> {
        Self::open_mode(root, key, NodeOpen::Snapshot)
    }

    fn open_mode(root: &Path, key: &[u8; 32], mode: NodeOpen) -> StorageResult<Self> {
        let shared = matches!(mode, NodeOpen::Snapshot);
        let directory = files::directory(root)?;
        let marker = files::regular(&root.join(NODE_MARKER), !shared)?;
        if shared {
            marker.try_lock_shared()
        } else {
            marker.try_lock()
        }
        .map_err(|_| invalid("run restore node is owned"))?;
        files::node_marker(root)?;
        let version = files::profile_version(root)?
            .ok_or_else(|| invalid("run restore logical profile is absent"))?;
        let with_private = matches!(mode, NodeOpen::Replayed) && version == SHARED_RECORD_PROFILE;
        files::require_root_members(root, with_private)?;
        let homes = [
            files::prepare_home(root, run_node::RUN_DIR)?,
            files::prepare_home(root, run_node::CAS_DIR)?,
        ];
        let (mut main, mut cas) = open_paired_runs(root, key, shared)?;
        let private = if with_private {
            let max = lastdb_model::storage_policy::BACKUP_RESOURCES
                .working_memory_bytes
                .checked_sub(main.retained_bytes())
                .and_then(|bytes| bytes.checked_sub(cas.retained_bytes()))
                .ok_or_else(|| invalid("private run metadata exceeds memory admission"))?;
            Some(LocalRoot::open(root, key, max)?)
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
        if let Some(expected) = &profile.cas {
            entries.extend(check_home(root, run_node::CAS_DIR, &cas, expected)?);
        }
        phase(Stage::Account, verify_account(&main, key, version))?;
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
    RunNodeGuard::open_mode(root, key, NodeOpen::Replayed).map(|_| ())
}

/// Account admission check against the inactive root's format markers and content-key proof.
pub(in crate::storage::disk_backup::run_restore) fn verify_account(
    main: &RunRead,
    account_key: &[u8; 32],
    version: u16,
) -> StorageResult<()> {
    let coordinates = [
        (
            RECORDS_COLLECTION.to_owned(),
            native_routing::encode(CURRENT_KEY_FORMAT_KEY.as_bytes()),
        ),
        (
            RECORDS_COLLECTION.to_owned(),
            native_routing::encode(DELETE_REPLAY_CUTOFF_KEY.as_bytes()),
        ),
        (
            METADATA_NAMESPACE.into(),
            native_routing::encode(LOCAL_AUTHOR_KEY.as_bytes()),
        ),
    ];
    let values = {
        let mut read = main
            .fork_bounded(account_read_budget(
                budget::DIRECTORY_READ_BYTES,
                coordinates.len() as u64,
            ))
            .map_err(map)?;
        read.get_many_bounded(&coordinates, 8192).map_err(map)?
    };
    let values = values
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .map_err(map)?;
    if values.len() != coordinates.len() {
        return Err(invalid("run backup logical marker read is incomplete"));
    }
    validate_frozen_replay_floor(values[0].as_deref(), values[1].as_deref())?;
    match version {
        SHARED_RECORD_PROFILE if values[2].is_none() => {}
        SHARED_RECORD_PROFILE => return Err(invalid("shared root contains a private author")),
        _ => validate_local_author(values[2].as_deref())?,
    }
    let marker = values[0]
        .as_deref()
        .ok_or_else(|| invalid("run backup logical marker is absent"))?;
    if !matches!(version, RECORD_PROFILE | SHARED_RECORD_PROFILE)
        || !lastdb_model::field_key_codec::current_key_format(marker)
    {
        return Err(invalid(
            "run backup logical marker differs from its node profile",
        ));
    }
    let stored = {
        let mut read = main
            .fork_bounded(account_read_budget(budget::DIRECTORY_READ_BYTES, 1))
            .map_err(map)?;
        read.get_bounded(
            content_key_proof::NAMESPACE,
            &native_routing::encode(content_key_proof::KEY),
            8192,
        )
        .map_err(map)?
        .ok_or_else(|| invalid("run backup account content-key proof is missing"))?
    };
    content_key_proof::verify(account_key, &stored)
        .map_err(|_| invalid("run backup account content-key proof differs"))
}

fn account_read_budget(bytes: u64, keys: u64) -> ReadBudget {
    ReadBudget {
        pages: budget::RECORD_INVENTORY_PAGES,
        blocks: budget::NATIVE_STREAM_BLOCKS,
        bytes,
        keys,
    }
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
    let inventories = (files as u64)
        .checked_mul(4 * budget::IDENTITY_MEMORY_BYTES)
        .ok_or_else(|| invalid("paired run inventory memory overflow"))?;
    let shared = main
        .retained_bytes()
        .checked_add(cas.retained_bytes())
        .and_then(|bytes| bytes.checked_add(inventories))
        .and_then(|bytes| bytes.checked_add(private.map_or(0, |root| root.read.retained_bytes())))
        .and_then(|bytes| {
            bytes.checked_add(lastdb_model::storage_policy::BACKUP_RESOURCES.stream_buffer_bytes)
        })
        .ok_or_else(|| invalid("paired run memory overflow"))?;
    reserve_paired_metadata(main, cas, shared)
}
