//! Persisted memo of sealed-chunk sha256 digests, so a backup walk re-hashes only
//! files whose identity changed.

use super::*;

/// File name of the persisted chunk-sha memo, stored in the same sidecar
/// directory as the high-water marker.
pub(crate) const CHUNK_SHA_MEMO_FILE: &str = "laststore_chunk_sha_memo.json";

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
struct ChunkShaMemoEntry {
    mtime_nanos: u128,
    len: u64,
    sha256: String,
}

#[derive(Debug, Default)]
struct ChunkShaMemoState {
    loaded: bool,
    dirty: bool,
    entries: std::collections::HashMap<std::path::PathBuf, ChunkShaMemoEntry>,
    /// Paths consulted since the last flush — flush prunes to this set so
    /// compacted-away chunk files do not accumulate forever.
    seen: std::collections::HashSet<std::path::PathBuf>,
    /// Test/diagnostic counter: full-file hashes actually performed.
    hashes_performed: u64,
}

/// (path, mtime, len) → sha256 memo for sealed chunk files.
///
/// Lookup stats the file; on an identity match it returns the memoized sha
/// without reading the file. Optionally persisted (atomic tmp+rename) so a
/// restart does not re-pay a full-store hash pass.
#[derive(Debug)]
pub(crate) struct ChunkShaMemo {
    persist_path: Option<std::path::PathBuf>,
    state: std::sync::Mutex<ChunkShaMemoState>,
}

impl ChunkShaMemo {
    pub(crate) fn new(persist_path: Option<std::path::PathBuf>) -> Self {
        Self {
            persist_path,
            state: std::sync::Mutex::new(ChunkShaMemoState::default()),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, ChunkShaMemoState> {
        // A poisoned memo only ever costs re-hashing; never propagate.
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn file_identity(path: &std::path::Path) -> StorageResult<(u128, u64)> {
        let meta = fs::metadata(path).map_err(StorageError::IoError)?;
        let mtime_nanos = meta
            .modified()
            .ok()
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .map_or(0, |d| d.as_nanos());
        Ok((mtime_nanos, meta.len()))
    }

    pub(super) fn sha256_for(&self, path: &std::path::Path) -> StorageResult<(String, u64)> {
        let (mtime_nanos, len) = Self::file_identity(path)?;
        {
            let mut state = self.lock();
            if !state.loaded {
                state.loaded = true;
                if let Some(persisted) = self.load_persisted() {
                    state.entries = persisted;
                }
            }
            state.seen.insert(path.to_path_buf());
            if let Some(entry) = state.entries.get(path) {
                if entry.mtime_nanos == mtime_nanos && entry.len == len {
                    return Ok((entry.sha256.clone(), entry.len));
                }
            }
        }
        let (sha256, bytes) = sha256_file(path)?;
        // Guard against the file changing while we hashed it (an active
        // segment racing the walk): memoize only a stable identity.
        let stable = Self::file_identity(path)
            .is_ok_and(|(m, l)| m == mtime_nanos && l == len && l == bytes);
        let mut state = self.lock();
        state.hashes_performed = state.hashes_performed.saturating_add(1);
        if stable {
            state.entries.insert(
                path.to_path_buf(),
                ChunkShaMemoEntry {
                    mtime_nanos,
                    len,
                    sha256: sha256.clone(),
                },
            );
            state.dirty = true;
        }
        Ok((sha256, bytes))
    }

    fn load_persisted(
        &self,
    ) -> Option<std::collections::HashMap<std::path::PathBuf, ChunkShaMemoEntry>> {
        let path = self.persist_path.as_ref()?;
        let bytes = fs::read(path).ok()?;
        serde_json::from_slice(&bytes).ok()
    }

    /// Prune to the paths seen since the last flush and persist if changed.
    /// Best-effort: a failed flush only costs future re-hashing.
    pub(crate) fn flush_after_walk(&self) {
        let Some(path) = self.persist_path.as_ref() else {
            let mut state = self.lock();
            state.seen.clear();
            return;
        };
        let snapshot = {
            let mut state = self.lock();
            let before = state.entries.len();
            let seen = std::mem::take(&mut state.seen);
            state.entries.retain(|p, _| seen.contains(p));
            let pruned = before != state.entries.len();
            if !state.dirty && !pruned {
                return;
            }
            state.dirty = false;
            state.entries.clone()
        };
        let Ok(bytes) = serde_json::to_vec(&snapshot) else {
            return;
        };
        let tmp = path.with_extension("json.tmp");
        if fs::write(&tmp, bytes).is_ok() {
            let _ = fs::rename(&tmp, path);
        }
    }
}

pub(super) fn sha256_file(path: &std::path::Path) -> StorageResult<(String, u64)> {
    use std::io::Read;
    let mut file = fs::File::open(path).map_err(StorageError::IoError)?;
    let mut hasher = Sha256::new();
    let mut buf = [0u8; 64 * 1024];
    let mut total = 0u64;
    loop {
        let n = file.read(&mut buf).map_err(StorageError::IoError)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
        total = total.saturating_add(n as u64);
    }
    Ok((hex_lower(hasher.finalize()), total))
}
