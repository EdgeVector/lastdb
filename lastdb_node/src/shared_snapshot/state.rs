//! Private durable publication intent. Never part of the exported checkpoint.

use crate::byte_cloud_restore::{local, RestoreError, Result};
use crate::shared_restore::recovery::MAX_MANIFEST_BYTES;
use cloud_backup_protocol::bytes_sha256;
use cloud_backup_protocol::shared::{SealedObject, SnapshotManifest, SnapshotReceipt};
use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

const MANIFEST: &str = "shared-snapshot-manifest.sealed";
const INTENT: &str = "shared-snapshot-intent.json";

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Intent {
    pub(super) manifest: SnapshotManifest,
    pub(super) phase: Phase,
}

#[derive(Serialize, Deserialize)]
#[serde(
    tag = "phase",
    content = "receipt",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub(super) enum Phase {
    Prepared,
    Submitting,
    Accepted(SnapshotReceipt),
    Abandoned,
}

pub(super) struct IntentStore {
    root: PathBuf,
    _owner: File,
}

impl IntentStore {
    pub(super) fn open(root: &Path) -> Result<Self> {
        local::directory(root)?;
        let lock = root.join("shared-snapshot.lock");
        let owner = match local::owner_file(&lock, true) {
            Ok(owner) => owner,
            Err(RestoreError::Io(error)) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                local::owner_file(&lock, false)?
            }
            Err(error) => return Err(error),
        };
        owner
            .try_lock()
            .map_err(|_| RestoreError::Invalid("shared snapshot already owned"))?;
        Ok(Self {
            root: root.to_path_buf(),
            _owner: owner,
        })
    }

    pub(super) fn load(&self) -> Result<Option<Intent>> {
        match local::read_json(
            &self.root.join(INTENT),
            cloud_backup_protocol::limits::REQUEST_JSON_MAX_BYTES,
        ) {
            Ok(intent) => Ok(Some(intent)),
            Err(RestoreError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }

    pub(super) fn save(&self, intent: &Intent) -> Result<()> {
        local::atomic_json(&self.root.join(INTENT), intent)
    }

    pub(super) fn write_manifest(&self, sealed: &[u8]) -> Result<()> {
        if sealed.len() as u64 > MAX_MANIFEST_BYTES {
            return Err(RestoreError::Limit("shared snapshot manifest bytes"));
        }
        let pending = self.root.join("shared-snapshot-manifest.pending");
        let mut file = local::writable_file(&pending, true)?;
        file.write_all(sealed)?;
        file.sync_all()?;
        fs::rename(pending, self.root.join(MANIFEST))?;
        File::open(&self.root)?.sync_all()?;
        Ok(())
    }

    pub(super) fn read_manifest(&self, expected: &SealedObject) -> Result<Vec<u8>> {
        expected.validate()?;
        let length = expected.bytes.get();
        if length > MAX_MANIFEST_BYTES {
            return Err(RestoreError::Limit("shared snapshot manifest bytes"));
        }
        let file = local::owner_file(&self.root.join(MANIFEST), false)?;
        let mut bytes = Vec::new();
        file.take(length + 1).read_to_end(&mut bytes)?;
        if bytes.len() as u64 != length || bytes_sha256(&bytes) != expected.sha256 {
            return Err(RestoreError::Invalid("saved shared snapshot manifest"));
        }
        Ok(bytes)
    }
}
