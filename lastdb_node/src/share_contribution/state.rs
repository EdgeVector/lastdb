//! Publisher grant and contributor outbox state use private atomic files.
use crate::byte_cloud_backup::connection::files::Directory;
use cloud_backup_protocol::Id;
use rustix::fs::{flock, FlockOperation};
use serde::{de::DeserializeOwned, Serialize};
use std::fs::File;
use std::path::Path;
use zeroize::Zeroizing;

type Loaded<T> = (T, Option<Zeroizing<Vec<u8>>>);

pub(super) struct State {
    directory: Directory,
    name: String,
    _lock: File,
}

impl State {
    pub(super) fn open(home: &Path, kind: &str, id: &Id) -> Result<Self, String> {
        let directory = Directory::open(home).map_err(|e| e.to_string())?;
        let name = format!("share-{kind}-{}", id.as_str());
        let lock = directory
            .open_lock(&format!("{name}.lock"))
            .map_err(|e| e.to_string())?;
        flock(&lock, FlockOperation::NonBlockingLockExclusive)
            .map_err(|_| "share contribution state is busy")?;
        Ok(Self {
            directory,
            name,
            _lock: lock,
        })
    }

    pub(super) fn load<T: DeserializeOwned + Default>(&self) -> Result<Loaded<T>, String> {
        let previous = self.directory.read(&self.name).map_err(|e| e.to_string())?;
        let value = previous
            .as_ref()
            .map(|bytes| {
                serde_json::from_slice(bytes).map_err(|_| "invalid local share contribution state")
            })
            .transpose()?
            .unwrap_or_default();
        Ok((value, previous))
    }

    pub(super) fn save<T: Serialize>(
        &self,
        value: &T,
        previous: Option<&[u8]>,
    ) -> Result<Zeroizing<Vec<u8>>, String> {
        let bytes = Zeroizing::new(
            serde_json::to_vec(value).map_err(|_| "invalid local share contribution state")?,
        );
        self.directory
            .publish(&self.name, previous, &bytes)
            .map_err(|e| e.to_string())?;
        Ok(bytes)
    }
}
