use super::super::byte_backup::RunBackupProfile;
use super::super::run_node;
use super::{invalid, RunRestoreFile, StorageResult};
use crate::storage::disk_pack::{byte_backup::RunBackupHome, NODE_MARKER};
use cloud_backup_protocol::{DecimalU64, Digest, Id};
use laststore::disk_run::home::{RunRead, RUN_HOME_MARKER};
use sha2::{Digest as _, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::Read;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::Path;

pub(super) struct HomeBinding {
    home: File,
    runs: File,
    lock: File,
}

impl HomeBinding {
    pub(super) fn check(&self, root: &Path, name: &str) -> StorageResult<()> {
        let path = root.join(name);
        same_entry(&self.home, &path)?;
        same_entry(&self.runs, &path.join("runs"))?;
        same_entry(&self.lock, &path.join("LOCK"))
    }
}

pub(super) fn directory(path: &Path) -> StorageResult<File> {
    if !path.is_absolute() || path.canonicalize()? != path {
        return Err(invalid("run restore directory alias"));
    }
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_DIRECTORY)
        .open(path)?;
    let meta = file.metadata()?;
    if !meta.is_dir() || meta.uid() != unsafe { libc::geteuid() } || meta.mode() & 0o077 != 0 {
        return Err(invalid("run restore private directory"));
    }
    same_entry(&file, path)?;
    Ok(file)
}

pub(super) fn regular(path: &Path, write: bool) -> StorageResult<File> {
    private_file(path, write, false)
}

/// Source snapshots hardlink these immutable selectors. Targets remain unlinked.
pub(super) fn captured_selector(path: &Path) -> StorageResult<File> {
    if !matches!(
        path.file_name().and_then(|name| name.to_str()),
        Some("CURRENT" | RUN_HOME_MARKER)
    ) {
        return Err(invalid("captured source selector name differs"));
    }
    private_file(path, false, true)
}

fn private_file(path: &Path, write: bool, captured: bool) -> StorageResult<File> {
    let file = OpenOptions::new()
        .read(true)
        .write(write)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)?;
    let meta = file.metadata()?;
    if !meta.is_file()
        || meta.uid() != unsafe { libc::geteuid() }
        || meta.mode() & 0o077 != 0
        || (!captured && meta.nlink() != 1)
    {
        return Err(invalid("run restore private file"));
    }
    same_entry(&file, path)?;
    Ok(file)
}

pub(super) fn same_entry(file: &File, path: &Path) -> StorageResult<()> {
    let held = file.metadata()?;
    let current = fs::symlink_metadata(path)?;
    if (held.dev(), held.ino(), held.uid(), held.mode())
        != (current.dev(), current.ino(), current.uid(), current.mode())
    {
        return Err(invalid("run restore entry changed"));
    }
    Ok(())
}

pub(super) fn node_marker(root: &Path) -> StorageResult<()> {
    if !is_run_marker(root)? {
        return Err(invalid("run restore node marker differs"));
    }
    Ok(())
}

pub(super) fn is_run_marker(root: &Path) -> StorageResult<bool> {
    Ok(profile_version(root)?.is_some())
}

pub(super) fn profile_version(root: &Path) -> StorageResult<Option<u16>> {
    // The non-run branch belongs to the unchanged V1 validator. Apply the
    // stricter V2 ownership rules only after exact format selection.
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(root.join(NODE_MARKER))?;
    if !file.metadata()?.is_file() {
        return Err(invalid("restored node marker type"));
    }
    let mut bytes = Vec::new();
    Read::by_ref(&mut file).take(129).read_to_end(&mut bytes)?;
    Ok(super::super::run_admission::profile(&bytes))
}

pub(super) fn require_root_members(root: &Path) -> StorageResult<()> {
    root_members(root, false)
}

pub(super) fn require_identity_root_members(root: &Path) -> StorageResult<()> {
    root_members(root, true)
}

fn root_members(root: &Path, identity: bool) -> StorageResult<()> {
    let mut names = BTreeSet::new();
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let name = entry
            .file_name()
            .into_string()
            .map_err(|_| invalid("run restore name"))?;
        if !(matches!(
            name.as_str(),
            "laststore-pack-node-v1" | "disk-run" | "disk-cas"
        ) || (identity && name == "logical-device-capture-v1"))
            || !names.insert(name)
        {
            return Err(invalid("run restore wrapper closure"));
        }
    }
    if names.len() != if identity { 4 } else { 3 } {
        return Err(invalid("run restore wrapper closure"));
    }
    Ok(())
}

pub(super) fn home(
    root: &Path,
    name: &str,
    reader: &RunRead,
    expected: &RunBackupHome,
) -> StorageResult<Vec<RunRestoreFile>> {
    let path = root.join(name);
    prepare_home(root, name)?;
    let mut entries = Vec::new();
    for (file, digest) in [
        (RUN_HOME_MARKER, &expected.descriptor_sha256),
        ("CURRENT", &expected.current_sha256),
    ] {
        let bytes = hash(&path.join(file), digest)?;
        if file == RUN_HOME_MARKER && bytes != 64 {
            return Err(invalid("run restore descriptor length"));
        }
        entries.push(RunRestoreFile {
            path: format!("{name}/{file}"),
            bytes,
        });
    }
    let expected_runs: BTreeMap<_, _> = reader.referenced_runs().collect();
    let mut seen = BTreeSet::new();
    for entry in fs::read_dir(path.join("runs"))? {
        let entry = entry?;
        let name = entry
            .file_name()
            .into_string()
            .map_err(|_| invalid("run restore file name"))?;
        let id = name
            .strip_suffix(".ldrun")
            .ok_or_else(|| invalid("run restore file suffix"))?;
        let id = uuid::Uuid::parse_str(id).map_err(|_| invalid("run restore file identity"))?;
        let bytes = expected_runs
            .get(&id)
            .ok_or_else(|| invalid("run restore unreferenced file"))?;
        if name != format!("{id}.ldrun")
            || !seen.insert(id)
            || regular(&entry.path(), false)?.metadata()?.len() != *bytes
        {
            return Err(invalid("run restore file closure"));
        }
        entries.push(RunRestoreFile {
            path: format!("{}/{name}", root_name(&path)?),
            bytes: *bytes,
        });
    }
    if seen.len() != expected_runs.len() {
        return Err(invalid("run restore missing file"));
    }
    require_home_members(&path)?;
    let bytes = entries.iter().try_fold(0u64, |total, entry| {
        total
            .checked_add(entry.bytes)
            .ok_or_else(|| invalid("run restore byte sum"))
    })?;
    if entries.len() as u64 != expected.files || bytes != expected.bytes {
        return Err(invalid("run restore captured totals"));
    }
    Ok(entries)
}

pub(super) fn prepare_home(root: &Path, name: &str) -> StorageResult<HomeBinding> {
    let path = root.join(name);
    let home = directory(&path)?;
    let runs = directory(&path.join("runs"))?;
    let lock = regular(&path.join("LOCK"), false)?;
    if regular(&path.join(RUN_HOME_MARKER), false)?
        .metadata()?
        .len()
        != 64
        || lock.metadata()?.len() != 0
    {
        return Err(invalid("run restore descriptor or ephemeral lock length"));
    }
    regular(&path.join("CURRENT"), false)?;
    require_home_members(&path)?;
    Ok(HomeBinding { home, runs, lock })
}

fn root_name(path: &Path) -> StorageResult<String> {
    path.file_name()
        .and_then(|name| name.to_str())
        .map(|name| format!("{name}/runs"))
        .ok_or_else(|| invalid("run restore home name"))
}

fn require_home_members(path: &Path) -> StorageResult<()> {
    let mut names = BTreeSet::new();
    for entry in fs::read_dir(path)? {
        let name = entry?
            .file_name()
            .into_string()
            .map_err(|_| invalid("run restore home member"))?;
        if !matches!(
            name.as_str(),
            "laststore-run-home-v2" | "CURRENT" | "LOCK" | "runs"
        ) || !names.insert(name)
        {
            return Err(invalid("run restore home closure"));
        }
    }
    if names.len() != 4 {
        return Err(invalid("run restore home closure"));
    }
    Ok(())
}

fn hash(path: &Path, expected: &Digest) -> StorageResult<u64> {
    let (digest, bytes) = inspect(path, false)?;
    if digest != *expected {
        return Err(invalid("run restore file digest differs"));
    }
    Ok(bytes)
}

pub(super) fn profile_home(
    root: &Path,
    name: &str,
    reader: &RunRead,
) -> StorageResult<RunBackupHome> {
    profile(root, name, reader, false)
}

pub(super) fn source_profile_home(
    root: &Path,
    name: &str,
    reader: &RunRead,
) -> StorageResult<RunBackupHome> {
    profile(root, name, reader, true)
}

/// Build one paired profile and validate it. `captured` selects the source home.
pub(super) fn backup_profile(
    root: &Path,
    version: u16,
    main: &RunRead,
    cas: &RunRead,
    captured: bool,
) -> StorageResult<RunBackupProfile> {
    let home = if captured {
        source_profile_home
    } else {
        profile_home
    };
    let profile = RunBackupProfile {
        version,
        main: home(root, run_node::RUN_DIR, main)?,
        cas: home(root, run_node::CAS_DIR, cas)?,
    };
    profile.validate()?;
    Ok(profile)
}

fn profile(
    root: &Path,
    name: &str,
    reader: &RunRead,
    captured: bool,
) -> StorageResult<RunBackupHome> {
    let path = root.join(name);
    let (descriptor_sha256, descriptor_bytes) = inspect(&path.join(RUN_HOME_MARKER), captured)?;
    let (current_sha256, current_bytes) = inspect(&path.join("CURRENT"), captured)?;
    let mut files = 2u64;
    let mut bytes = descriptor_bytes
        .checked_add(current_bytes)
        .ok_or_else(|| invalid("run restore profile bytes"))?;
    for (_, length) in reader.referenced_runs() {
        files = files
            .checked_add(1)
            .ok_or_else(|| invalid("run restore profile file count"))?;
        bytes = bytes
            .checked_add(length)
            .ok_or_else(|| invalid("run restore profile bytes"))?;
    }
    Ok(RunBackupHome {
        home_id: Id::try_from(reader.home_id().to_string())
            .map_err(|_| invalid("run restore home identity"))?,
        generation: DecimalU64::new(reader.generation()),
        frontier: DecimalU64::new(reader.frontier()),
        descriptor_sha256,
        current_sha256,
        files,
        bytes,
    })
}

fn inspect(path: &Path, captured: bool) -> StorageResult<(Digest, u64)> {
    let mut file = if captured {
        captured_selector(path)?
    } else {
        regular(path, false)?
    };
    let before = file.metadata()?;
    let mut hasher = Sha256::new();
    let mut buffer =
        vec![0u8; lastdb_storage_policy::BACKUP_RESOURCES.stream_buffer_bytes as usize];
    let mut count = 0u64;
    loop {
        let bytes = file.read(&mut buffer)?;
        if bytes == 0 {
            break;
        }
        count = count
            .checked_add(bytes as u64)
            .ok_or_else(|| invalid("run restore hash bytes"))?;
        if count > before.len() {
            return Err(invalid("run restore file grew"));
        }
        hasher.update(&buffer[..bytes]);
    }
    let after = file.metadata()?;
    same_entry(&file, path)?;
    if count != before.len()
        || (before.len(), before.mtime(), before.mtime_nsec())
            != (after.len(), after.mtime(), after.mtime_nsec())
    {
        return Err(invalid("run restore file changed"));
    }
    let digest = Digest::try_from(crate::hex::hex_lower(hasher.finalize()))
        .map_err(|_| invalid("run restore file digest"))?;
    Ok((digest, count))
}
