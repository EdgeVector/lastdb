//! A prior home is an untrusted byte cache, never a source of restore membership.
use crate::hex::sha256_hex;
use crate::storage::laststore::BackupChunkRef;
use std::fs;
use std::io::Read;
use std::path::{Component, Path, PathBuf};
use uuid::Uuid;

/// Read-only source of bytes for authenticated manifest references.
/// The restore destination remains fresh; this cache never opens LastStore.
#[derive(Clone, Debug)]
pub struct RestoreChunkCache {
    root: PathBuf,
}

impl RestoreChunkCache {
    /// Bind an existing node home. No key, cloud config, or data body is read.
    pub fn new(home: &Path) -> Result<Self, String> {
        let data = home.join("data");
        if fs::symlink_metadata(&data)
            .map_err(|_| "restore chunk cache data directory is unavailable")?
            .file_type()
            .is_symlink()
        {
            return Err("restore chunk cache data directory must not be a symlink".into());
        }
        let root = data
            .canonicalize()
            .map_err(|_| "restore chunk cache data directory is unavailable")?;
        if !root.is_dir() {
            return Err("restore chunk cache data path is not a directory".into());
        }
        Ok(Self { root })
    }

    /// Read the exact manifest prefix from the resolved installation address.
    /// A cache miss, unsupported form, or mismatch always falls back to cloud.
    /// The caller applies its usual download reservation before this read.
    pub(crate) fn read(&self, chunk: &BackupChunkRef, install_uuid: &str) -> Option<Vec<u8>> {
        let uuid = Uuid::parse_str(install_uuid).ok()?;
        let paths = laststore::restore_chunk_cache_paths(
            &self.root,
            &chunk.collection,
            chunk.shard,
            chunk.group_id,
            uuid,
        )
        .ok()?;
        paths
            .into_iter()
            .find_map(|path| self.read_prefix(&path, chunk))
    }

    fn read_prefix(&self, path: &Path, chunk: &BackupChunkRef) -> Option<Vec<u8>> {
        // Reject symlink components in the frozen cache. The selected home must
        // stay stopped and unchanged until the restore finishes.
        let relative = path.strip_prefix(&self.root).ok()?;
        let mut current = self.root.clone();
        for component in relative.components() {
            let Component::Normal(part) = component else {
                return None;
            };
            current.push(part);
            if fs::symlink_metadata(&current)
                .ok()?
                .file_type()
                .is_symlink()
            {
                return None;
            }
        }
        let mut options = fs::OpenOptions::new();
        options.read(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
        }
        let file = options.open(path).ok()?;
        let metadata = file.metadata().ok()?;
        if !metadata.is_file() || metadata.len() < chunk.bytes {
            return None;
        }
        let length = usize::try_from(chunk.bytes).ok()?;
        let mut bytes = Vec::new();
        bytes.try_reserve_exact(length).ok()?;
        file.take(chunk.bytes).read_to_end(&mut bytes).ok()?;
        if bytes.len() != length || sha256_hex(&bytes) != chunk.sha256 {
            return None;
        }
        Some(bytes)
    }
}
