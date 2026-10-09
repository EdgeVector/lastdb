//! Spike: [`DocumentStore`] over exportable **Last Store** (`laststore` crate).
//!
//! Layout under the home path:
//! ```text
//! <root>/
//!   engine.json                 # { "engine": "laststore", "codec": "..." }
//!   laststore/                  # laststore home (data/<collection>/…)
//!   blobs/sha256/<aa>/<bb>/<hex>  # CAS (same as SegmentStore)
//! ```
//!
//! Codec wraps document bodies (and blobs) the same way SegmentStore does.
//! Transactions use Last Store's group-commit flush (not SegmentStore's WAL
//! file) — crash mid-transaction may leave partial multi-doc state; Nano
//! still batches atom+tip writes in one `transaction` call.

use super::{BodyCodec, CompactStats, DocumentStore, TxnOp};
use crate::error::{Error, Result};
use laststore::{LastStore, LastStoreOptions};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

const ENGINE_NAME: &str = "laststore";
const ENGINE_FILE: &str = "engine.json";

#[derive(Debug, Serialize, Deserialize)]
struct EngineManifest {
    engine: String,
    codec: String,
}

/// Last Store–backed document engine with Nano codec + CAS blobs.
pub struct LastStoreEngine {
    root: PathBuf,
    inner: LastStore,
    codec: Box<dyn BodyCodec>,
    /// Serialize multi-op transactions (Last Store is multi-threaded safe
    /// per-shard, but we keep a simple txn mutex for clearer spike semantics).
    txn: Mutex<()>,
}

impl LastStoreEngine {
    /// Open or create a Last Store engine at `root` with the given codec.
    pub fn open(root: impl AsRef<Path>, codec: Box<dyn BodyCodec>) -> Result<Self> {
        Self::open_with(root, LastStoreOptions::default(), codec)
    }

    /// Open with explicit Last Store options (shard bits, group-commit, …).
    pub fn open_with(
        root: impl AsRef<Path>,
        opts: LastStoreOptions,
        codec: Box<dyn BodyCodec>,
    ) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(&root)?;
        fs::create_dir_all(root.join("blobs"))?;

        let manifest_path = root.join(ENGINE_FILE);
        if manifest_path.exists() {
            let m: EngineManifest = serde_json::from_slice(&fs::read(&manifest_path)?)?;
            if m.engine != ENGINE_NAME {
                return Err(Error::Manifest(format!(
                    "expected engine {ENGINE_NAME:?}, found {:?}",
                    m.engine
                )));
            }
            if m.codec != codec.name() {
                return Err(Error::Manifest(format!(
                    "codec mismatch: store has {:?}, opened with {:?}",
                    m.codec,
                    codec.name()
                )));
            }
        } else {
            let m = EngineManifest {
                engine: ENGINE_NAME.to_string(),
                codec: codec.name().to_string(),
            };
            let tmp = root.join(format!("{ENGINE_FILE}.tmp"));
            fs::write(&tmp, serde_json::to_vec_pretty(&m)?)?;
            fs::rename(&tmp, &manifest_path)?;
        }

        let inner = LastStore::open_with(root.join("laststore"), opts)?;
        Ok(Self {
            root,
            inner,
            codec,
            txn: Mutex::new(()),
        })
    }

    /// Home directory for this engine.
    pub fn path(&self) -> &Path {
        &self.root
    }

    fn blob_path(&self, hash_hex: &str) -> PathBuf {
        self.root
            .join("blobs")
            .join("sha256")
            .join(&hash_hex[..2])
            .join(&hash_hex[2..4])
            .join(hash_hex)
    }

    /// Bulk put with a single flush (import path).
    pub fn put_many(&self, collection: &str, items: Vec<(String, Vec<u8>)>) -> Result<()> {
        for (id, body) in items {
            let stored = self.codec.encode(&body)?;
            self.inner.put(collection, &id, &stored)?;
        }
        self.inner.flush()?;
        Ok(())
    }

    fn dir_size(path: &Path) -> u64 {
        fn walk(p: &Path) -> u64 {
            let mut t = 0u64;
            let Ok(rd) = fs::read_dir(p) else {
                return 0;
            };
            for e in rd.flatten() {
                let path = e.path();
                if path.is_dir() {
                    t += walk(&path);
                } else if let Ok(m) = e.metadata() {
                    t += m.len();
                }
            }
            t
        }
        walk(path)
    }

    fn count_seg_files(path: &Path) -> u64 {
        fn walk(p: &Path) -> u64 {
            let mut n = 0u64;
            let Ok(rd) = fs::read_dir(p) else {
                return 0;
            };
            for e in rd.flatten() {
                let path = e.path();
                if path.is_dir() {
                    n += walk(&path);
                } else if path.extension().and_then(|x| x.to_str()) == Some("seg") {
                    n += 1;
                }
            }
            n
        }
        walk(path)
    }
}

impl DocumentStore for LastStoreEngine {
    fn get(&self, collection: &str, id: &str) -> Result<Option<Vec<u8>>> {
        match self.inner.get(collection, id)? {
            None => Ok(None),
            Some(stored) => Ok(Some(self.codec.decode(&stored)?)),
        }
    }

    fn put(&self, collection: &str, id: &str, body: &[u8]) -> Result<()> {
        let stored = self.codec.encode(body)?;
        self.inner.put(collection, id, &stored)?;
        Ok(())
    }

    fn put_many(&self, collection: &str, items: Vec<(String, Vec<u8>)>) -> Result<()> {
        LastStoreEngine::put_many(self, collection, items)
    }

    fn delete(&self, collection: &str, id: &str) -> Result<()> {
        self.inner.delete(collection, id)?;
        Ok(())
    }

    fn exists(&self, collection: &str, id: &str) -> Result<bool> {
        Ok(self.inner.exists(collection, id)?)
    }

    fn list_prefix(&self, collection: &str, prefix: &str) -> Result<Vec<(String, Vec<u8>)>> {
        self.list_prefix_paged(collection, prefix, None, usize::MAX)
    }

    fn list_prefix_keys(&self, collection: &str, prefix: &str) -> Result<Vec<String>> {
        Ok(self.inner.list_prefix_keys(collection, prefix)?)
    }

    fn list_prefix_paged(
        &self,
        collection: &str,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<(String, Vec<u8>)>> {
        let raw = self
            .inner
            .list_prefix_paged(collection, prefix, after, limit)?;
        let mut out = Vec::with_capacity(raw.len());
        for (id, stored) in raw {
            out.push((id, self.codec.decode(&stored)?));
        }
        Ok(out)
    }

    fn list_prefix_keys_paged(
        &self,
        collection: &str,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<String>> {
        Ok(self
            .inner
            .list_prefix_keys_paged(collection, prefix, after, limit)?)
    }

    fn list_range(
        &self,
        collection: &str,
        start: &str,
        end: &str,
    ) -> Result<Vec<(String, Vec<u8>)>> {
        self.list_range_paged(collection, start, end, usize::MAX)
    }

    fn list_range_paged(
        &self,
        collection: &str,
        start: &str,
        end: &str,
        limit: usize,
    ) -> Result<Vec<(String, Vec<u8>)>> {
        let raw = self.inner.list_range_paged(collection, start, end, limit)?;
        let mut out = Vec::with_capacity(raw.len());
        for (id, stored) in raw {
            out.push((id, self.codec.decode(&stored)?));
        }
        Ok(out)
    }

    fn list_range_keys_paged(
        &self,
        collection: &str,
        start: &str,
        end: &str,
        limit: usize,
    ) -> Result<Vec<String>> {
        Ok(self
            .inner
            .list_range_keys_paged(collection, start, end, limit)?)
    }

    fn transaction(&self, ops: Vec<TxnOp>) -> Result<()> {
        let _g = self.txn.lock().expect("txn poisoned");
        let mut mapped = Vec::with_capacity(ops.len());
        for op in ops {
            match op {
                TxnOp::Put {
                    collection,
                    id,
                    body,
                } => {
                    let stored = self.codec.encode(&body)?;
                    mapped.push(laststore::TxnOp::put(&collection, &id, stored));
                }
                TxnOp::Delete { collection, id } => {
                    mapped.push(laststore::TxnOp::delete(&collection, &id));
                }
            }
        }
        self.inner.transaction(mapped)?;
        Ok(())
    }

    fn put_blob(&self, bytes: &[u8]) -> Result<String> {
        let hash_hex = hex::encode(Sha256::digest(bytes));
        let path = self.blob_path(&hash_hex);
        if !path.exists() {
            fs::create_dir_all(path.parent().expect("blob parent"))?;
            let stored = self.codec.encode(bytes)?;
            let tmp = path.with_extension(format!("tmp{}", std::process::id()));
            let mut file = File::create(&tmp)?;
            file.write_all(&stored)?;
            file.sync_all()?;
            drop(file);
            fs::rename(&tmp, &path)?;
        }
        Ok(format!("sha256:{hash_hex}"))
    }

    fn get_blob(&self, blob_ref: &str) -> Result<Option<Vec<u8>>> {
        let hash_hex = blob_ref
            .strip_prefix("sha256:")
            .ok_or_else(|| Error::Corrupt(format!("bad blob ref {blob_ref:?}")))?;
        if hash_hex.len() != 64 || !hash_hex.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(Error::Corrupt(format!("bad blob ref {blob_ref:?}")));
        }
        let path = self.blob_path(hash_hex);
        if !path.exists() {
            return Ok(None);
        }
        let plain = self.codec.decode(&fs::read(&path)?)?;
        let actual = hex::encode(Sha256::digest(&plain));
        if actual != hash_hex {
            return Err(Error::Corrupt(format!(
                "blob {blob_ref} content hash mismatch"
            )));
        }
        Ok(Some(plain))
    }

    fn compact_shard(&self, collection: &str, shard: u16) -> Result<CompactStats> {
        // Last Store compact is per-collection (all shards). Spike reports
        // shard 0 stats for the whole collection when shard==0; other shard
        // ids no-op with zero stats if multi-shard Last Store is in use.
        if shard != 0 && self.inner.options().shard_bits > 0 {
            return Ok(CompactStats {
                collection: collection.to_string(),
                shard,
                live_docs: 0,
                segments_removed: 0,
                bytes_before: 0,
                bytes_after: 0,
            });
        }
        self.compact(collection)
    }

    fn compact(&self, collection: &str) -> Result<CompactStats> {
        let coll_path = self.root.join("laststore").join("data").join(collection);
        let bytes_before = Self::dir_size(&coll_path);
        let segs_before = Self::count_seg_files(&coll_path);
        let live_docs = self.inner.list_prefix(collection, "")?.len() as u64;
        self.inner.compact_collection(collection)?;
        let bytes_after = Self::dir_size(&coll_path);
        // Last Store rewrites every open segment; count pre-compact segs as removed
        // (same accounting as SegmentStore's `old_segments.len()`).
        Ok(CompactStats {
            collection: collection.to_string(),
            shard: 0,
            live_docs,
            segments_removed: segs_before,
            bytes_before,
            bytes_after,
        })
    }
}
