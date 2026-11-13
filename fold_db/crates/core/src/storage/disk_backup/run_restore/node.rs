//! Read-only physical and account proof for an inactive native node.

use super::{
    fork_bounded, invalid, map, phase, read_budget, sum_file_bytes, RunBackupProfile, Stage,
    StorageError, StorageResult,
};
use crate::storage::content_key_proof::{self, KEY as CKP_KEY, NAMESPACE as CKP_NS};
use crate::storage::disk_backup::byte_backup::RunBackupHome;
use crate::storage::disk_pack::{native_routing::encode as enc, run_node, NODE_MARKER};
use crate::storage::format::control::{
    validate_frozen_replay_floor, CURRENT_KEY_FORMAT_KEY, DELETE_REPLAY_CUTOFF_KEY,
};
use crate::storage::format::metadata::{validate_local_author, LOCAL_AUTHOR_KEY};
use crate::storage::format::{METADATA_NAMESPACE, RECORDS_COLLECTION, RECORD_PROFILE};
use cloud_backup_protocol::{DecimalU64, Digest, Id, NativeRoot};
use lastdb_model::storage_policy::{backup_decode as budget, BACKUP_RESOURCES};
use laststore::disk_run::home::{RunRead, CURRENT, LOCK, RUNS, RUN_HOME_MARKER};
use sha2::{Digest as _, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::Read;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};

type HomeBinding = [File; 3];

pub struct RunNodeGuard {
    pub(super) root: PathBuf,
    directory: File,
    marker: File,
    homes: [HomeBinding; 2],
    pub(super) main: RunRead,
    cas: RunRead,
    pub(super) profile: RunBackupProfile,
    pub(super) files: Vec<(String, u64)>,
}

impl RunNodeGuard {
    pub fn native_root(&self) -> StorageResult<NativeRoot> {
        self.profile.native_root()
    }
    pub fn files(&self) -> impl Iterator<Item = &(String, u64)> {
        self.files.iter()
    }
    pub fn totals(&self) -> StorageResult<(u64, u64)> {
        let count = u64::try_from(self.files.len()).map_err(|_| invalid("run file count"))?;
        sum_file_bytes(&self.files, "run file byte sum").map(|b| (count, b))
    }
    pub fn validate_context(&self, data_root: &Path, root: &NativeRoot) -> StorageResult<()> {
        (self.root == data_root)
            .then_some(())
            .ok_or_else(|| invalid("run restore publication root differs"))
            .and_then(|()| self.profile.validate_root(root))
            .and_then(|()| self.check_binding())
    }
    pub(super) fn require_empty_local_cas(&self) -> StorageResult<()> {
        self.cas
            .referenced_runs()
            .next()
            .is_none()
            .then_some(())
            .ok_or_else(|| invalid("restored personal CAS home is not empty"))
    }
    pub fn check_binding(&self) -> StorageResult<()> {
        same_entry(&self.directory, &self.root)?;
        same_entry(&self.marker, &self.root.join(NODE_MARKER))?;
        if profile_version(&self.root)? != self.profile.version {
            return Err(invalid("run restore logical profile changed"));
        }
        for ([home, runs, lock], name) in self
            .homes
            .iter()
            .zip([run_node::RUN_DIR, run_node::CAS_DIR])
        {
            let p = self.root.join(name);
            same_entry(home, &p)?;
            same_entry(runs, &p.join(RUNS))?;
            same_entry(lock, &p.join(LOCK))?;
        }
        (scan_main_home(&self.root, self.profile.version, &self.main)?.0 == self.profile)
            .then_some(())
            .ok_or_else(|| invalid("run restore captured home differs"))
            .and_then(|()| require_root_members(&self.root))
    }

    pub(super) fn open(root: &Path, key: &[u8; 32], shared: bool) -> StorageResult<Self> {
        let (directory, marker, version) = lock_node(root, shared, "run restore node is owned")?;
        let homes = [
            prepare_home(root, run_node::RUN_DIR)?,
            prepare_home(root, run_node::CAS_DIR)?,
        ];
        let open_fn = if shared {
            RunRead::open_existing_shared_bounded
        } else {
            RunRead::open_existing_bounded
        };
        let max_mem = BACKUP_RESOURCES.working_memory_bytes;
        let mut main = open_fn(
            &root.join(run_node::RUN_DIR),
            &run_node::home_key(key),
            read_budget(),
            max_mem,
        )
        .map_err(map)?;
        let rem = max_mem
            .checked_sub(main.retained_bytes())
            .ok_or_else(|| invalid("paired run metadata exceeds memory admission"))?;
        let mut cas = open_fn(
            &root.join(run_node::CAS_DIR),
            &run_node::cas_key(key),
            read_budget(),
            rem,
        )
        .map_err(map)?;
        let runs = (main.referenced_runs().count() + cas.referenced_runs().count() + 5) as u64;
        let retained = runs
            .checked_mul(4 * budget::IDENTITY_MEMORY_BYTES)
            .and_then(|b| b.checked_add(main.retained_bytes() + cas.retained_bytes()))
            .and_then(|b| b.checked_add(BACKUP_RESOURCES.stream_buffer_bytes))
            .ok_or_else(|| invalid("paired run memory overflow"))?;
        main.reserve_metadata(retained).map_err(map)?;
        cas.reserve_metadata(retained).map_err(map)?;
        let (profile, mut files) = scan_main_home(root, version, &main)?;
        files.insert(0, (NODE_MARKER.into(), profile.node_marker()?.len() as u64));
        phase(Stage::Account, verify_account(&main, key, version))?;
        let guard = Self {
            root: root.to_path_buf(),
            directory,
            marker,
            homes,
            main,
            cas,
            profile,
            files,
        };
        guard.check_binding().map(|()| guard)
    }
}

pub(super) fn lock_node(
    root: &Path,
    shared: bool,
    lock_err: &'static str,
) -> StorageResult<(File, File, u16)> {
    let (dir, marker) = (directory(root)?, regular(&root.join(NODE_MARKER), !shared)?);
    if shared {
        marker.try_lock_shared()
    } else {
        marker.try_lock()
    }
    .map_err(|_| invalid(lock_err))?;
    let version = profile_version(root)?;
    require_root_members(root).map(|()| (dir, marker, version))
}

pub(super) fn verify_account(main: &RunRead, key: &[u8; 32], version: u16) -> StorageResult<()> {
    let rec = RECORDS_COLLECTION;
    let coords = [
        (rec.into(), enc(CURRENT_KEY_FORMAT_KEY.as_bytes())),
        (rec.into(), enc(DELETE_REPLAY_CUTOFF_KEY.as_bytes())),
        (METADATA_NAMESPACE.into(), enc(LOCAL_AUTHOR_KEY.as_bytes())),
        (CKP_NS.into(), enc(CKP_KEY)),
    ];
    let values = fork_bounded(main, budget::DIRECTORY_READ_BYTES, coords.len() as u64)?
        .get_many_bounded(&coords, 8192)
        .map_err(map)?
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .map_err(map)?;
    if values.len() != coords.len() {
        return Err(invalid("run backup logical marker read is incomplete"));
    }
    validate_frozen_replay_floor(values[0].as_deref(), values[1].as_deref())?;
    validate_local_author(values[2].as_deref())?;
    let marker = values[0]
        .as_deref()
        .ok_or_else(|| invalid("run backup logical marker is absent"))?;
    if version != RECORD_PROFILE || !lastdb_model::field_key_codec::current_key_format(marker) {
        return Err(invalid(
            "run backup logical marker differs from its node profile",
        ));
    }
    let stored = values[3]
        .as_deref()
        .ok_or_else(|| invalid("run backup account content-key proof is missing"))?;
    content_key_proof::verify(key, stored)
        .map_err(|_| invalid("run backup account content-key proof differs"))
}

fn is_owned_private(meta: &fs::Metadata) -> bool {
    meta.uid() == unsafe { libc::geteuid() } && meta.mode() & 0o077 == 0
}

pub(super) fn directory(path: &Path) -> StorageResult<File> {
    if !path.is_absolute() || path.canonicalize()? != path {
        return Err(invalid("run restore directory alias"));
    }
    let f = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_DIRECTORY)
        .open(path)?;
    let m = f.metadata()?;
    (m.is_dir() && is_owned_private(&m))
        .then_some(())
        .ok_or_else(|| invalid("run restore private directory"))
        .and_then(|()| same_entry(&f, path))
        .map(|()| f)
}

pub(super) fn regular(path: &Path, write: bool) -> StorageResult<File> {
    let f = OpenOptions::new()
        .read(true)
        .write(write)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)?;
    let m = f.metadata()?;
    (m.is_file() && is_owned_private(&m) && m.nlink() == 1)
        .then_some(())
        .ok_or_else(|| invalid("run restore private file"))
        .and_then(|()| same_entry(&f, path))
        .map(|()| f)
}

fn same_entry(file: &File, path: &Path) -> StorageResult<()> {
    let (h, c) = (file.metadata()?, fs::symlink_metadata(path)?);
    ((h.dev(), h.ino(), h.uid(), h.mode()) == (c.dev(), c.ino(), c.uid(), c.mode()))
        .then_some(())
        .ok_or_else(|| invalid("run restore entry changed"))
}

fn profile_version(root: &Path) -> StorageResult<u16> {
    let f = regular(&root.join(NODE_MARKER), false).or_else(|_| {
        OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(root.join(NODE_MARKER))
            .map_err(StorageError::from)
    })?;
    if !f.metadata()?.is_file() {
        return Err(invalid("restored node marker type"));
    }
    let mut bytes = Vec::new();
    (&f).take(129).read_to_end(&mut bytes)?;
    crate::storage::retired::refuse_shared_home(root, &bytes)?;
    run_node::profile(&bytes).ok_or_else(|| invalid("run restore node marker differs"))
}

fn require_root_members(root: &Path) -> StorageResult<()> {
    exact_members(
        root,
        &[NODE_MARKER, run_node::RUN_DIR, run_node::CAS_DIR],
        "run restore wrapper closure",
    )
}

fn exact_members(path: &Path, allowed: &[&str], err_msg: &'static str) -> StorageResult<()> {
    let mut names = BTreeSet::new();
    for entry in fs::read_dir(path)? {
        let name = entry?
            .file_name()
            .into_string()
            .map_err(|_| invalid(err_msg))?;
        if !allowed.contains(&name.as_str()) || !names.insert(name) {
            return Err(invalid(err_msg));
        }
    }
    (names.len() == allowed.len())
        .then_some(())
        .ok_or_else(|| invalid(err_msg))
}

fn prepare_home(root: &Path, name: &str) -> StorageResult<HomeBinding> {
    let p = root.join(name);
    let binding = [
        directory(&p)?,
        directory(&p.join(RUNS))?,
        regular(&p.join(LOCK), false)?,
    ];
    if regular(&p.join(RUN_HOME_MARKER), false)?.metadata()?.len() != 64
        || binding[2].metadata()?.len() != 0
    {
        return Err(invalid("run restore descriptor or ephemeral lock length"));
    }
    regular(&p.join(CURRENT), false)?;
    exact_members(
        &p,
        &[RUN_HOME_MARKER, CURRENT, LOCK, RUNS],
        "run restore home closure",
    )
    .map(|()| binding)
}

fn scan_main_home(
    root: &Path,
    version: u16,
    reader: &RunRead,
) -> StorageResult<(RunBackupProfile, Vec<(String, u64)>)> {
    let name = run_node::RUN_DIR;
    let path = root.join(name);
    prepare_home(root, name)?;
    let (descriptor_sha256, d_bytes) = inspect(&path.join(RUN_HOME_MARKER))?;
    if d_bytes != 64 {
        return Err(invalid("run restore file digest differs"));
    }
    let (current_sha256, c_bytes) = inspect(&path.join(CURRENT))?;
    let mut entries = vec![
        (format!("{name}/{RUN_HOME_MARKER}"), d_bytes),
        (format!("{name}/{CURRENT}"), c_bytes),
    ];
    let expected: BTreeMap<_, _> = reader.referenced_runs().collect();
    let mut seen = BTreeSet::new();
    for entry in fs::read_dir(path.join(RUNS))? {
        let entry = entry?;
        let fname = entry
            .file_name()
            .into_string()
            .map_err(|_| invalid("run restore file name"))?;
        let id = fname
            .strip_suffix(".ldrun")
            .and_then(|s| uuid::Uuid::parse_str(s).ok())
            .ok_or_else(|| invalid("run restore file identity"))?;
        let bytes = *expected
            .get(&id)
            .ok_or_else(|| invalid("run restore unreferenced file"))?;
        if fname != format!("{id}.ldrun")
            || !seen.insert(id)
            || regular(&entry.path(), false)?.metadata()?.len() != bytes
        {
            return Err(invalid("run restore file closure"));
        }
        entries.push((format!("{name}/{RUNS}/{fname}"), bytes));
    }
    if seen.len() != expected.len() {
        return Err(invalid("run restore missing file"));
    }
    let bytes = sum_file_bytes(&entries, "run restore byte sum")?;
    let home_id = Id::try_from(reader.home_id().to_string())
        .map_err(|_| invalid("run restore home identity"))?;
    let profile = RunBackupProfile {
        version,
        main: RunBackupHome {
            home_id,
            generation: DecimalU64::new(reader.generation()),
            frontier: DecimalU64::new(reader.frontier()),
            descriptor_sha256,
            current_sha256,
            files: entries.len() as u64,
            bytes,
        },
    };
    profile.validate().map(|()| (profile, entries))
}

pub(super) fn stream_regular(
    path: &Path,
    sink: &mut impl std::io::Write,
    err: &'static str,
) -> StorageResult<u64> {
    let file = regular(path, false)?;
    let b = file.metadata()?;
    let count = std::io::copy(&mut (&file).take(b.len().saturating_add(1)), sink)?;
    let a = file.metadata()?;
    same_entry(&file, path)?;
    ((count, b.len(), b.mtime(), b.mtime_nsec()) == (b.len(), a.len(), a.mtime(), a.mtime_nsec()))
        .then_some(count)
        .ok_or_else(|| invalid(err))
}

fn inspect(path: &Path) -> StorageResult<(Digest, u64)> {
    let mut hasher = Sha256::new();
    let count = stream_regular(path, &mut hasher, "run restore file changed")?;
    let digest = Digest::try_from(lastdb_model::hex::hex_lower(hasher.finalize()))
        .map_err(|_| invalid("run restore file digest"))?;
    Ok((digest, count))
}
