//! Reuse a SHA proof only while the exact local file identity remains unchanged.

use crate::sync::{SyncError, SyncResult};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs::File,
    io::Read,
    path::{Path, PathBuf},
    sync::{Mutex, OnceLock},
};

#[derive(Clone, PartialEq, Eq)]
struct FileProof {
    bytes: u64,
    sha256: String,
    #[cfg(unix)]
    identity: (u64, u64, i64, i64, i64, i64),
}

fn proof(path: &Path, bytes: u64, sha: &str) -> SyncResult<FileProof> {
    let metadata = std::fs::symlink_metadata(path)?;
    if !metadata.is_file() || metadata.file_type().is_symlink() || metadata.len() != bytes {
        return Err(SyncError::Storage(
            "retained source file length or type differs".into(),
        ));
    }
    #[cfg(unix)]
    use std::os::unix::fs::MetadataExt;
    Ok(FileProof {
        bytes,
        sha256: sha.into(),
        #[cfg(unix)]
        identity: (
            metadata.dev(),
            metadata.ino(),
            metadata.mtime(),
            metadata.mtime_nsec(),
            metadata.ctime(),
            metadata.ctime_nsec(),
        ),
    })
}

pub(super) fn verify_file(path: &Path, bytes: u64, sha: &str) -> SyncResult<()> {
    static VERIFIED: OnceLock<Mutex<BTreeMap<PathBuf, FileProof>>> = OnceLock::new();
    let cache = VERIFIED.get_or_init(|| Mutex::new(BTreeMap::new()));
    let before = proof(path, bytes, sha)?;
    #[cfg(unix)]
    if cache
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(path)
        == Some(&before)
    {
        return Ok(());
    }
    let mut file = File::open(path)?;
    let mut digest = Sha256::new();
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    if crate::hex::hex_lower(digest.finalize()) != sha || proof(path, bytes, sha)? != before {
        return Err(SyncError::Crypto(
            "retained source file digest or identity differs".into(),
        ));
    }
    let mut cache = cache
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    // Only metadata proofs enter this bounded cache; no bytes or handles stay.
    if cache.len() == 4096 {
        cache.clear();
    }
    cache.insert(path.to_path_buf(), before);
    Ok(())
}
