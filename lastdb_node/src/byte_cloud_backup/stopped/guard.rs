use super::*;
use cloud_backup_protocol::{bytes_sha256, Digest};
use std::fs::{File, OpenOptions};
use std::io::Read;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};

pub(super) struct Ownership {
    _node: File,
    node_identity: (u64, u64, u32, u64, i64, i64),
    directories: Vec<(u64, u64, u32, u32)>,
    currents: [Digest; 2],
}

pub(super) fn admit(home: &Path, request: &StoppedBackupRequest) -> Result<Ownership> {
    if !home.is_absolute()
        || home.canonicalize()? != home
        || home.components().any(|p| {
            p.as_os_str()
                .to_string_lossy()
                .starts_with(".primary-lastdb-archive-")
        })
        || request.stopped_pid == 0
        || request.stopped_start_ts == 0
    {
        return Err(BackupError::Identity);
    }
    let mut directories = Vec::new();
    for path in [home.to_owned(), home.join("data")] {
        let metadata = std::fs::symlink_metadata(&path)?;
        if !metadata.is_dir()
            || metadata.uid() != unsafe { libc::geteuid() }
            || metadata.mode() & 0o022 != 0
            || path.canonicalize()? != path
        {
            return Err(BackupError::Identity);
        }
        directories.push((
            metadata.dev(),
            metadata.ino(),
            metadata.uid(),
            metadata.mode(),
        ));
    }
    check_session(home, request)?;
    let node = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(home.join("data/laststore-pack-node-v1"))?;
    let metadata = node.metadata()?;
    if !metadata.is_file()
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.mode() & 0o022 != 0
        || metadata.nlink() != 1
        || metadata.len() > 128
    {
        return Err(BackupError::Identity);
    }
    node.try_lock().map_err(|_| BackupError::Busy)?;
    let ownership = Ownership {
        node_identity: node_identity(&metadata),
        directories,
        _node: node,
        currents: currents(home)?,
    };
    ownership.check(home, request)?;
    Ok(ownership)
}

fn check_session(home: &Path, request: &StoppedBackupRequest) -> Result<()> {
    crate::offline_home::preflight_read_only_home(home, &home.join("data"))
        .map_err(|_| BackupError::Busy)?;
    let session = crate::session_ledger::read_last_record(home).ok_or(BackupError::Missing)?;
    let receipt = crate::session_ledger::read_shutdown_flush_receipt(home)
        .map_err(|_| BackupError::Source)?;
    if session.pid != request.stopped_pid
        || session.start_ts != request.stopped_start_ts
        || receipt.pid != request.stopped_pid
        || receipt.start_ts != request.stopped_start_ts
        || !receipt.flush_ok
        || home.join("cloud_sync.json").try_exists()?
    {
        return Err(BackupError::Identity);
    }
    Ok(())
}

fn current(home: &Path, name: &str) -> Result<Digest> {
    let path = home.join("data").join(name).join("CURRENT");
    let parent = path.parent().ok_or(BackupError::Identity)?;
    if parent.canonicalize()? != parent {
        return Err(BackupError::Identity);
    }
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(&path)?;
    let before = file.metadata()?;
    const MAXIMUM: u64 = 64 * 1024 * 1024;
    if !before.is_file()
        || before.uid() != unsafe { libc::geteuid() }
        || before.mode() & 0o077 != 0
        || before.len() == 0
        || before.len() > MAXIMUM
    {
        return Err(BackupError::Identity);
    }
    let mut bytes = Vec::new();
    (&mut file).take(MAXIMUM + 1).read_to_end(&mut bytes)?;
    let same = |actual: std::fs::Metadata| {
        actual.dev() == before.dev()
            && actual.ino() == before.ino()
            && actual.len() == before.len()
            && actual.mtime_nsec() == before.mtime_nsec()
            && actual.mtime() == before.mtime()
            && actual.mode() == before.mode()
    };
    if bytes.len() as u64 != before.len()
        || !same(file.metadata()?)
        || !same(std::fs::symlink_metadata(&path)?)
    {
        return Err(BackupError::Identity);
    }
    Ok(bytes_sha256(&bytes))
}

fn currents(home: &Path) -> Result<[Digest; 2]> {
    Ok([current(home, "disk-run")?, current(home, "disk-cas")?])
}

impl Ownership {
    pub fn check(&self, home: &Path, request: &StoppedBackupRequest) -> Result<()> {
        check_session(home, request)?;
        for (path, identity) in [home.to_owned(), home.join("data")]
            .iter()
            .zip(&self.directories)
        {
            let info = std::fs::symlink_metadata(path)?;
            if (info.dev(), info.ino(), info.uid(), info.mode()) != *identity {
                return Err(BackupError::Identity);
            }
        }
        if node_identity(&self._node.metadata()?) != self.node_identity
            || node_identity(&std::fs::symlink_metadata(
                home.join("data/laststore-pack-node-v1"),
            )?) != self.node_identity
        {
            return Err(BackupError::Identity);
        }
        if currents(home)? != self.currents {
            return Err(BackupError::Identity);
        }
        if let Some(policy) = super::super::store::read::<serde_json::Value>(
            &Store::base(home).join("automatic.json"),
        )? {
            if policy["version"].as_u64() != Some(2)
                || policy["account"].as_str() != Some(request.account_id.as_str())
                || policy["state"]["enabled"].as_bool() != Some(false)
                || !policy["pending"].is_null()
            {
                return Err(BackupError::Disabled);
            }
        }
        Ok(())
    }
    pub fn check_profile(
        &self,
        home: &Path,
        request: &StoppedBackupRequest,
        profile: &RunBackupProfile,
    ) -> Result<()> {
        self.check(home, request)?;
        if let Some(policy) = super::super::store::read::<serde_json::Value>(
            &Store::base(home).join("automatic.json"),
        )? {
            if policy["source_home"].as_str() != Some(profile.main.home_id.as_str()) {
                return Err(BackupError::Identity);
            }
        }
        if self.currents
            != [
                profile.main.current_sha256.clone(),
                profile.cas.current_sha256.clone(),
            ]
        {
            return Err(BackupError::Identity);
        }
        Ok(())
    }
}

fn node_identity(info: &std::fs::Metadata) -> (u64, u64, u32, u64, i64, i64) {
    (
        info.dev(),
        info.ino(),
        info.mode(),
        info.nlink(),
        info.mtime(),
        info.mtime_nsec(),
    )
}
