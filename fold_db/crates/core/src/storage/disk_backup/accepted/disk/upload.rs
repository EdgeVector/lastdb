//! Durable upload progress and private spool file I/O paired with the local accepted spool.

use super::{Error, Log};
use cloud_backup_protocol::CloudScope;
use serde::{Deserialize, Serialize};
use std::path::Path;

const FILE: &str = "cloud-upload.json";

pub(in crate::storage::disk_backup::accepted) mod io {
    use crate::storage::disk_backup::accepted::Error;
    use crate::storage::disk_pack::admit;
    use serde::de::DeserializeOwned;
    use serde::Serialize;
    use std::fs::{self, File};
    use std::io::{Read, Write};
    use std::os::unix::fs::MetadataExt;
    use std::path::Path;

    /// The largest control file the spool reads or writes. The pending record of
    /// one commit names every note of it, and a commit may hold `NOTES_MAX`
    /// (in the parent of this directory) notes, each named by about 470 bytes of JSON at most.
    const CONTROL_MAX: u64 = 1_048_576;

    pub(in crate::storage::disk_backup::accepted) fn ensure_dir(path: &Path) -> Result<(), Error> {
        if path.exists() {
            return require_dir(path);
        }
        let parent = path.parent().ok_or(Error::Unavailable)?;
        require_dir(parent)?;
        admit::create_private_dir(path).map_err(|_| Error::Unavailable)?;
        require_dir(path)?;
        sync_dir(parent)
    }

    pub(in crate::storage::disk_backup::accepted) fn require_dir(path: &Path) -> Result<(), Error> {
        match admit::private_dir_status(path) {
            Ok(Some(true)) => Ok(()),
            _ => Err(Error::Unavailable),
        }
    }

    pub(in crate::storage::disk_backup::accepted) fn read_json<T: DeserializeOwned>(
        path: &Path,
    ) -> Result<Option<T>, Error> {
        let Some(bytes) = read_limited(path, CONTROL_MAX)? else {
            return Ok(None);
        };
        serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|_| Error::Gap)
    }

    pub(in crate::storage::disk_backup::accepted) fn write_json<T: Serialize>(
        path: &Path,
        value: &T,
    ) -> Result<(), Error> {
        let bytes =
            cloud_backup_protocol::v2::hash::canonical_bytes(value).map_err(|_| Error::Gap)?;
        if bytes.len() as u64 > CONTROL_MAX {
            return Err(Error::Gap);
        }
        write_bytes(path, &bytes)
    }

    pub(in crate::storage::disk_backup::accepted) fn write_bytes(
        path: &Path,
        bytes: &[u8],
    ) -> Result<(), Error> {
        let parent = path.parent().ok_or(Error::Unavailable)?;
        require_dir(parent)?;
        let pending = path.with_extension("partial");
        let mut file =
            admit::open_private_write(&pending, false).map_err(|_| Error::Unavailable)?;
        file.write_all(bytes).map_err(|_| Error::Unavailable)?;
        file.sync_all().map_err(|_| Error::Unavailable)?;
        fs::rename(&pending, path).map_err(|_| Error::Unavailable)?;
        sync_dir(parent)
    }

    pub(in crate::storage::disk_backup::accepted) fn read_exact(
        path: &Path,
        max: u64,
    ) -> Result<Vec<u8>, Error> {
        read_limited(path, max)?.ok_or(Error::Gap)
    }

    pub(in crate::storage::disk_backup::accepted) fn read_limited(
        path: &Path,
        max: u64,
    ) -> Result<Option<Vec<u8>>, Error> {
        let info = match fs::symlink_metadata(path) {
            Ok(info) => info,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(_) => return Err(Error::Unavailable),
        };
        if !info.is_file()
            || !admit::is_private_meta(&info)
            || info.nlink() != 1
            || info.len() > max
        {
            return Err(Error::Gap);
        }
        let file = admit::open_nofollow_read(path).map_err(|_| Error::Unavailable)?;
        let mut bytes = Vec::new();
        file.take(max + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| Error::Unavailable)?;
        if bytes.len() as u64 != info.len() {
            return Err(Error::Gap);
        }
        Ok(Some(bytes))
    }

    pub(in crate::storage::disk_backup::accepted) fn sync_dir(path: &Path) -> Result<(), Error> {
        File::open(path)
            .and_then(|file| file.sync_all())
            .map_err(|_| Error::Unavailable)
    }

    pub(in crate::storage::disk_backup::accepted) fn remove_file(path: &Path) -> Result<(), Error> {
        match fs::symlink_metadata(path) {
            Ok(info) if info.is_file() => fs::remove_file(path).map_err(|_| Error::Unavailable),
            Ok(_) => Err(Error::Gap),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(_) => Err(Error::Unavailable),
        }
    }

    pub(in crate::storage::disk_backup::accepted) fn remove_dir(path: &Path) -> Result<(), Error> {
        match fs::symlink_metadata(path) {
            Ok(info) if info.is_dir() => fs::remove_dir_all(path).map_err(|_| Error::Unavailable),
            Ok(_) => Err(Error::Gap),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(_) => Err(Error::Unavailable),
        }
    }
}

pub(in crate::storage::disk_backup::accepted) fn stopped_confirmation(
    root: &Path,
    identity: &UploadIdentity,
) -> Result<UploadPosition, Error> {
    io::require_dir(root)?;
    match root.join("pending.json").symlink_metadata() {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(_) => return Err(Error::Unavailable),
        Ok(_) => return Err(Error::Gap),
    }
    let state = io::read_json::<super::State>(&root.join("state.json"))?.ok_or(Error::Gap)?;
    if state.version != 1
        || state.gap
        || state.armed
        || state.head != state.acked
        || state.retained_bytes != 0
    {
        return Err(Error::Gap);
    }
    match Checkpoint::read(root, identity)? {
        Some(position) if position.spool_through == state.head => Ok(position),
        _ => Err(Error::Behind),
    }
}

/// Identity of the destination this home publishes to.
pub struct UploadIdentity {
    /// Account and generation admitted by the cloud.
    pub scope: CloudScope,
    /// Cloud environment selected by the saved connection.
    pub environment: String,
    /// Database identifier in that scope.
    pub database_id: String,
    /// Device assigning sequence numbers to these edits.
    pub device_id: String,
    /// Signing identity of those cloud entries.
    pub writer_key: String,
}

/// The confirmed local coordinates of an upload.
#[derive(Clone, Copy)]
pub struct UploadPosition {
    /// Highest writer sequence known to have been saved for this device.
    pub writer_seq: u64,
    /// Accepted spool position confirmed or covered locally.
    pub spool_through: u64,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Checkpoint {
    version: u8,
    scope: CloudScope,
    environment: String,
    database_id: String,
    device_id: String,
    writer_key: String,
    writer_seq: u64,
    spool_through: u64,
}

impl Checkpoint {
    fn read(root: &Path, identity: &UploadIdentity) -> Result<Option<UploadPosition>, Error> {
        let Some(saved) = io::read_json::<Self>(&root.join(FILE))? else {
            return Ok(None);
        };
        if saved.version != 1 || saved.writer_seq == u64::MAX {
            return Err(Error::Gap);
        }
        if saved.scope != identity.scope
            || saved.environment != identity.environment
            || saved.database_id != identity.database_id
            || saved.device_id != identity.device_id
            || saved.writer_key != identity.writer_key
        {
            return Err(Error::Conflict);
        }
        Ok(Some(UploadPosition {
            writer_seq: saved.writer_seq,
            spool_through: saved.spool_through,
        }))
    }

    fn write(
        root: &Path,
        identity: &UploadIdentity,
        position: UploadPosition,
    ) -> Result<(), Error> {
        let checkpoint = Self {
            version: 1,
            scope: identity.scope.clone(),
            environment: identity.environment.clone(),
            database_id: identity.database_id.clone(),
            device_id: identity.device_id.clone(),
            writer_key: identity.writer_key.clone(),
            writer_seq: position.writer_seq,
            spool_through: position.spool_through,
        };
        io::write_json(&root.join(FILE), &checkpoint)
    }
}

impl Log {
    pub(in crate::storage) fn upload_position(
        &self,
        identity: &UploadIdentity,
    ) -> Result<Option<UploadPosition>, Error> {
        let mut inner = self.lock()?;
        inner.refuse_gap()?;
        let Some(position) = Checkpoint::read(&self.root, identity)? else {
            return Ok(None);
        };
        let status = inner.status();
        if position.spool_through < status.acked {
            return Ok(None);
        }
        if position.spool_through > status.acked {
            inner.acknowledge(&self.root, position.spool_through)?;
        }
        Ok(Some(position))
    }

    pub(in crate::storage) fn confirm_upload(
        &self,
        identity: &UploadIdentity,
        after: u64,
        position: UploadPosition,
    ) -> Result<(), Error> {
        let mut inner = self.lock()?;
        inner.refuse_gap()?;
        let status = inner.status();
        if after != status.acked || position.spool_through < after {
            return Err(Error::Behind);
        }
        if position.spool_through > status.head || position.writer_seq == u64::MAX {
            return Err(Error::Ahead);
        }
        if let Some(previous) = Checkpoint::read(&self.root, identity)? {
            if previous.spool_through > after || previous.writer_seq > position.writer_seq {
                return Err(Error::Conflict);
            }
        }
        Checkpoint::write(&self.root, identity, position)?;
        inner.acknowledge(&self.root, position.spool_through)
    }
}
