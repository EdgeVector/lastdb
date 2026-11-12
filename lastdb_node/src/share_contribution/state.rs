//! Publisher grant and contributor outbox state use private atomic files.
use crate::offline_home::Directory;
use cloud_backup_protocol::Id;
use rustix::fs::{flock, FlockOperation};
use serde::{de::DeserializeOwned, Serialize};
use std::fs::File;
use std::path::Path;
use zeroize::Zeroizing;

// Current live path: the policy can contain twelve writers with eight rules
// each (`MAX_WRITERS` in `share_contribution`). The unified mutation log
// supersedes that cap and has no contributor limit. See
// `docs/designs/unified-mutation-log.md`. This limit stays while this path
// is the one Share uses.
const MAX_STATE_BYTES: u64 = 128 * 1024;

type Loaded<T> = (T, Option<Zeroizing<Vec<u8>>>);

pub(super) struct State {
    directory: Directory,
    name: String,
    _lock: File,
}

impl State {
    pub(super) fn open(home: &Path, kind: &str, id: &Id) -> Result<Self, String> {
        let directory =
            Directory::open_with_limit(home, MAX_STATE_BYTES).map_err(|e| e.to_string())?;
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
