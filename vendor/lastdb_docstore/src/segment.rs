//! Hash-sharded segment-file backend (design layout, candidate A).
//!
//! ```text
//! <root>/
//!   manifest.json                    # store_version, shard_bits, codec
//!   collections/<name>/<hh>/<seq>.seg
//! ```
//!
//! A segment is JSON-lines, one record per line:
//! `{"op":"put","id":"…","body":"<b64 of codec output>"}` or
//! `{"op":"del","id":"…"}`. Writers append to the open (last) segment and
//! roll to a new one past `max_segment_bytes`. Per-shard compaction folds
//! live puts into one new sealed segment, atomically renames it in, then
//! unlinks the old segments — deleted/overwritten bytes return to the OS.

use super::{BodyCodec, CompactStats, DocumentStore, TxnOp};
use crate::{Error, Result};
use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

const STORE_VERSION: u32 = 2;
const SEG_EXT: &str = "seg";

#[derive(Debug, Serialize, Deserialize)]
struct Manifest {
    store_version: u32,
    shard_bits: u8,
    codec: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct RecordLine {
    op: String,
    id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    body: Option<String>,
}

/// Location of the latest record for an id.
#[derive(Debug, Clone, Copy)]
struct Loc {
    seg: u64,
    offset: u64,
    len: u64,
}

#[derive(Debug, Default)]
struct Shard {
    dir: PathBuf,
    /// id → latest live record location. Deleted ids are absent.
    index: BTreeMap<String, Loc>,
    /// Sorted segment sequence numbers present on disk.
    segments: Vec<u64>,
    /// Byte length of the open (last) segment.
    open_len: u64,
    /// Kept open across puts so Grouped mode can batch without reopening.
    open_file: Option<File>,
    /// Ops written since last `sync_data` on the open segment.
    dirty_ops: u32,
    /// Bytes written since last `sync_data` on the open segment.
    dirty_bytes: u64,
}

/// When segment data becomes durable on disk.
///
/// Interactive [`DocumentStore::put`] used to call `sync_data` on every line
/// (~5 ms/op). That is still available as [`Durability::Strict`]. The default
/// [`Durability::Grouped`] coalesces fsyncs: appends reuse an open file handle
/// and only `sync_data` when dirty thresholds are hit, on [`SegmentStore::flush`],
/// on compact, or on drop.
///
/// **Grouped durability bar:** a crash may lose the most recent unsynced
/// appends (bounded by `max_dirty_ops` / `max_dirty_bytes`). WAL transactions
/// still fsync the WAL first, then apply with a single end-of-apply segment
/// flush before removing the WAL file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Durability {
    /// `sync_data` after every append (legacy interactive path).
    Strict,
    /// Batch fsyncs until dirty thresholds, then sync.
    Grouped {
        max_dirty_ops: u32,
        max_dirty_bytes: u64,
    },
}

impl Default for Durability {
    fn default() -> Self {
        // ~128 puts or 256 KiB between fsyncs → interactive put well above the
        // 10k ops/s gate while bounding crash loss to one batch.
        Self::Grouped {
            max_dirty_ops: 256,
            max_dirty_bytes: 512 * 1024,
        }
    }
}

/// Tuning knobs. `shard_bits` only applies when creating a new store —
/// an existing manifest wins on open.
#[derive(Debug, Clone, Copy)]
pub struct SegmentStoreOptions {
    pub shard_bits: u8,
    pub max_segment_bytes: u64,
    pub durability: Durability,
}

impl Default for SegmentStoreOptions {
    fn default() -> Self {
        Self {
            // 16 shards (was 256): group-commit flush must fsync each dirty
            // open segment; fewer shards keeps interactive batch flushes cheap
            // while still spreading large collections. Existing stores keep
            // their manifest shard_bits.
            shard_bits: 4,
            max_segment_bytes: 8 * 1024 * 1024,
            durability: Durability::default(),
        }
    }
}

type ShardMap = HashMap<(String, u16), Arc<Mutex<Shard>>>;

/// One WAL record on disk — a whole transaction, bodies already
/// codec-encoded (ciphertext at rest) and base64-wrapped.
#[derive(Debug, Serialize, Deserialize)]
struct WalRecord {
    ops: Vec<WalOp>,
}

#[derive(Debug, Serialize, Deserialize)]
struct WalOp {
    op: String,
    collection: String,
    id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    body: Option<String>,
}

/// Segment-file [`DocumentStore`] backend.
pub struct SegmentStore {
    root: PathBuf,
    shard_bits: u8,
    max_segment_bytes: u64,
    durability: Durability,
    codec: Box<dyn BodyCodec>,
    shards: Mutex<ShardMap>,
    /// Serializes transactions; holds the next WAL sequence number.
    txn_seq: Mutex<u64>,
}

impl SegmentStore {
    /// Open (or create) a store at `root` with the default plain codec.
    pub fn open(root: impl AsRef<Path>) -> Result<Self> {
        Self::open_with(
            root,
            SegmentStoreOptions::default(),
            Box::new(super::PlainCodec),
        )
    }

    pub fn open_with(
        root: impl AsRef<Path>,
        options: SegmentStoreOptions,
        codec: Box<dyn BodyCodec>,
    ) -> Result<Self> {
        if options.shard_bits == 0 || options.shard_bits > 12 {
            return Err(Error::Manifest(format!(
                "shard_bits must be 1..=12, got {}",
                options.shard_bits
            )));
        }
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(root.join("collections"))?;
        let manifest_path = root.join("manifest.json");
        let shard_bits = if manifest_path.exists() {
            let m: Manifest = serde_json::from_slice(&fs::read(&manifest_path)?)?;
            if m.store_version != STORE_VERSION {
                return Err(Error::Manifest(format!(
                    "store_version {} != {STORE_VERSION}",
                    m.store_version
                )));
            }
            if m.codec != codec.name() {
                return Err(Error::Manifest(format!(
                    "codec {} on disk, {} requested",
                    m.codec,
                    codec.name()
                )));
            }
            m.shard_bits
        } else {
            let m = Manifest {
                store_version: STORE_VERSION,
                shard_bits: options.shard_bits,
                codec: codec.name().to_string(),
            };
            let tmp = root.join("manifest.json.tmp");
            fs::write(&tmp, serde_json::to_vec_pretty(&m)?)?;
            fs::rename(&tmp, &manifest_path)?;
            m.shard_bits
        };
        fs::create_dir_all(root.join("wal"))?;
        let store = Self {
            root,
            shard_bits,
            max_segment_bytes: options.max_segment_bytes,
            durability: options.durability,
            codec,
            shards: Mutex::new(HashMap::new()),
            txn_seq: Mutex::new(1),
        };
        store.recover_wal()?;
        Ok(store)
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    fn shard_of(&self, id: &str) -> u16 {
        let digest = Sha256::digest(id.as_bytes());
        let wide = u16::from_be_bytes([digest[0], digest[1]]);
        wide >> (16 - self.shard_bits)
    }

    fn shard_dir_name(&self, shard: u16) -> String {
        let width = usize::from(self.shard_bits.div_ceil(4));
        format!("{shard:0width$x}")
    }

    fn collection_dir(&self, collection: &str) -> PathBuf {
        self.root.join("collections").join(collection)
    }

    fn shard_handle(&self, collection: &str, shard: u16) -> Result<Arc<Mutex<Shard>>> {
        let key = (collection.to_string(), shard);
        {
            let map = self.shards.lock().expect("shard map poisoned");
            if let Some(existing) = map.get(&key) {
                return Ok(existing.clone());
            }
        }
        let dir = self
            .collection_dir(collection)
            .join(self.shard_dir_name(shard));
        let loaded = load_shard(dir)?;
        let mut map = self.shards.lock().expect("shard map poisoned");
        Ok(map
            .entry(key)
            .or_insert_with(|| Arc::new(Mutex::new(loaded)))
            .clone())
    }

    /// Shard numbers with a directory on disk for `collection`.
    fn shards_on_disk(&self, collection: &str) -> Result<Vec<u16>> {
        let dir = self.collection_dir(collection);
        let mut out = Vec::new();
        if !dir.exists() {
            return Ok(out);
        }
        for entry in fs::read_dir(&dir)? {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if let Ok(shard) = u16::from_str_radix(&name, 16) {
                out.push(shard);
            }
        }
        out.sort_unstable();
        Ok(out)
    }

    fn read_body(&self, shard: &Shard, loc: Loc) -> Result<Vec<u8>> {
        let rec = read_record(shard, loc)?;
        let stored = rec
            .body
            .ok_or_else(|| Error::Corrupt(format!("indexed put for {:?} has no body", rec.id)))?;
        let raw = B64.decode(stored.as_bytes())?;
        self.codec.decode(&raw)
    }

    fn sync_open_segment(shard: &mut Shard) -> Result<()> {
        if shard.dirty_ops == 0 && shard.dirty_bytes == 0 {
            return Ok(());
        }
        if let Some(file) = shard.open_file.as_mut() {
            file.sync_data()?;
        }
        shard.dirty_ops = 0;
        shard.dirty_bytes = 0;
        Ok(())
    }

    fn append(&self, shard: &mut Shard, line: &[u8]) -> Result<Loc> {
        self.append_inner(shard, line, /*force_sync_policy=*/ true)
    }

    /// Append without applying Strict per-line fsync (used when a later
    /// explicit flush will cover durability, e.g. WAL apply tail).
    fn append_deferred(&self, shard: &mut Shard, line: &[u8]) -> Result<Loc> {
        self.append_inner(shard, line, /*force_sync_policy=*/ false)
    }

    fn append_inner(&self, shard: &mut Shard, line: &[u8], force_sync_policy: bool) -> Result<Loc> {
        fs::create_dir_all(&shard.dir)?;
        // Roll to a fresh segment when the open one is full.
        let roll = shard.segments.is_empty()
            || (shard.open_len > 0 && shard.open_len + line.len() as u64 > self.max_segment_bytes);
        if roll {
            Self::sync_open_segment(shard)?;
            let next = shard.segments.last().copied().unwrap_or(0) + 1;
            shard.segments.push(next);
            shard.open_len = 0;
            shard.open_file = None;
        }
        let seg = *shard.segments.last().expect("open segment");
        if shard.open_file.is_none() {
            let path = segment_path(&shard.dir, seg);
            let file = OpenOptions::new().create(true).append(true).open(&path)?;
            shard.open_file = Some(file);
        }
        let file = shard.open_file.as_mut().expect("open segment file");
        let offset = shard.open_len;
        file.write_all(line)?;
        shard.open_len += line.len() as u64;
        shard.dirty_ops = shard.dirty_ops.saturating_add(1);
        shard.dirty_bytes = shard.dirty_bytes.saturating_add(line.len() as u64);

        let loc = Loc {
            seg,
            offset,
            len: line.len() as u64,
        };

        if force_sync_policy {
            match self.durability {
                Durability::Strict => {
                    file.sync_data()?;
                    shard.dirty_ops = 0;
                    shard.dirty_bytes = 0;
                }
                Durability::Grouped {
                    max_dirty_ops,
                    max_dirty_bytes,
                } => {
                    if shard.dirty_ops >= max_dirty_ops || shard.dirty_bytes >= max_dirty_bytes {
                        file.sync_data()?;
                        shard.dirty_ops = 0;
                        shard.dirty_bytes = 0;
                    }
                }
            }
        }
        Ok(loc)
    }

    /// Make all in-memory dirty segment appends durable.
    pub fn flush(&self) -> Result<()> {
        let keys: Vec<(String, u16)> = {
            let map = self.shards.lock().expect("shard map poisoned");
            map.keys().cloned().collect()
        };
        for (collection, shard_no) in keys {
            let handle = self.shard_handle(&collection, shard_no)?;
            let mut shard = handle.lock().expect("shard poisoned");
            Self::sync_open_segment(&mut shard)?;
        }
        Ok(())
    }

    pub fn durability(&self) -> Durability {
        self.durability
    }

    fn delete_deferred(&self, collection: &str, id: &str) -> Result<()> {
        let handle = self.shard_handle(collection, self.shard_of(id))?;
        let mut shard = handle.lock().expect("shard poisoned");
        if !shard.index.contains_key(id) {
            return Ok(());
        }
        let line = record_line("del", id, None)?;
        self.append_deferred(&mut shard, &line)?;
        shard.index.remove(id);
        Ok(())
    }

    /// Append many lines to a shard with ONE `sync_data` per touched segment
    /// file instead of one per line — the bulk-import fast path. Durability
    /// bar: a crash mid-batch can lose the batch's tail; bulk callers are
    /// idempotent re-runs (airlock import), never the interactive write path.
    fn append_many(&self, shard: &mut Shard, lines: &[Vec<u8>]) -> Result<Vec<Loc>> {
        fs::create_dir_all(&shard.dir)?;
        let mut locs = Vec::with_capacity(lines.len());
        let mut open: Option<(u64, File)> = None;
        for line in lines {
            let roll = shard.segments.is_empty()
                || (shard.open_len > 0
                    && shard.open_len + line.len() as u64 > self.max_segment_bytes);
            if roll {
                if let Some((_, f)) = open.take() {
                    f.sync_data()?;
                }
                let next = shard.segments.last().copied().unwrap_or(0) + 1;
                shard.segments.push(next);
                shard.open_len = 0;
            }
            let seg = *shard.segments.last().expect("open segment");
            if open.as_ref().map(|(s, _)| *s) != Some(seg) {
                if let Some((_, f)) = open.take() {
                    f.sync_data()?;
                }
                let file = OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(segment_path(&shard.dir, seg))?;
                open = Some((seg, file));
            }
            let (_, file) = open.as_mut().expect("open segment file");
            file.write_all(line)?;
            locs.push(Loc {
                seg,
                offset: shard.open_len,
                len: line.len() as u64,
            });
            shard.open_len += line.len() as u64;
        }
        if let Some((_, f)) = open.take() {
            f.sync_data()?;
        }
        Ok(locs)
    }

    /// Bulk put — groups by shard and appends with one fsync per touched
    /// segment file. Bypasses the WAL: callers are idempotent bulk imports.
    /// (Also available via [`DocumentStore::put_many`].)
    pub fn put_many(&self, collection: &str, items: Vec<(String, Vec<u8>)>) -> Result<()> {
        self.put_many_sharded(collection, items)
    }

    fn put_many_sharded(&self, collection: &str, items: Vec<(String, Vec<u8>)>) -> Result<()> {
        let mut by_shard: HashMap<u16, Vec<(String, Vec<u8>)>> = HashMap::new();
        for (id, body) in items {
            by_shard
                .entry(self.shard_of(&id))
                .or_default()
                .push((id, body));
        }
        for (shard_no, group) in by_shard {
            let handle = self.shard_handle(collection, shard_no)?;
            let mut shard = handle.lock().expect("shard poisoned");
            let mut lines = Vec::with_capacity(group.len());
            for (id, body) in &group {
                let stored = self.codec.encode(body)?;
                lines.push(record_line("put", id, Some(B64.encode(&stored)))?);
            }
            let locs = self.append_many(&mut shard, &lines)?;
            for ((id, _), loc) in group.into_iter().zip(locs) {
                shard.index.insert(id, loc);
            }
        }
        Ok(())
    }

    /// Apply a put whose body is already codec-encoded + base64-wrapped
    /// (WAL replay / transaction apply path).
    fn put_stored(&self, collection: &str, id: &str, body_b64: String) -> Result<()> {
        let line = record_line("put", id, Some(body_b64))?;
        let handle = self.shard_handle(collection, self.shard_of(id))?;
        let mut shard = handle.lock().expect("shard poisoned");
        // Durability is owned by the WAL commit + a trailing flush before the
        // WAL file is removed (or by recovery re-apply).
        let loc = self.append_deferred(&mut shard, &line)?;
        shard.index.insert(id.to_string(), loc);
        Ok(())
    }

    fn wal_dir(&self) -> PathBuf {
        self.root.join("wal")
    }

    fn apply_wal_record(&self, record: WalRecord) -> Result<()> {
        for op in record.ops {
            match op.op.as_str() {
                "put" => {
                    let body = op.body.ok_or_else(|| {
                        Error::Corrupt(format!("wal put for {:?} has no body", op.id))
                    })?;
                    self.put_stored(&op.collection, &op.id, body)?;
                }
                "del" => self.delete(&op.collection, &op.id)?,
                other => {
                    return Err(Error::Corrupt(format!("unknown wal op {other:?}")));
                }
            }
        }
        Ok(())
    }

    /// Replay committed-but-unapplied transactions, drop torn ones.
    ///
    /// A `.txn` file that parses is a committed transaction: re-apply every
    /// op (idempotent — puts overwrite, deletes no-op). A file that does not
    /// parse was torn mid-commit: none of its ops were applied, so removing
    /// it is a clean rollback. `.tmp` leftovers are always discarded.
    fn recover_wal(&self) -> Result<()> {
        let dir = self.wal_dir();
        let mut txn_files = Vec::new();
        for entry in fs::read_dir(&dir)? {
            let path = entry?.path();
            match path.extension().and_then(|e| e.to_str()) {
                Some("txn") => txn_files.push(path),
                Some("tmp") => fs::remove_file(&path)?,
                _ => {}
            }
        }
        txn_files.sort();
        for path in txn_files {
            match serde_json::from_slice::<WalRecord>(&fs::read(&path)?) {
                Ok(record) => self.apply_wal_record(record)?,
                Err(_) => { /* torn commit — rolled back by removal */ }
            }
            fs::remove_file(&path)?;
        }
        // Seed the in-memory sequence past anything we just consumed.
        Ok(())
    }

    fn blob_path(&self, hash_hex: &str) -> PathBuf {
        self.root
            .join("blobs")
            .join("sha256")
            .join(&hash_hex[0..2])
            .join(&hash_hex[2..4])
            .join(hash_hex)
    }
}

fn segment_path(dir: &Path, seq: u64) -> PathBuf {
    dir.join(format!("{seq:010}.{SEG_EXT}"))
}

fn record_line(op: &str, id: &str, body: Option<String>) -> Result<Vec<u8>> {
    let mut line = serde_json::to_vec(&RecordLine {
        op: op.to_string(),
        id: id.to_string(),
        body,
    })?;
    line.push(b'\n');
    Ok(line)
}

fn read_record(shard: &Shard, loc: Loc) -> Result<RecordLine> {
    let mut file = File::open(segment_path(&shard.dir, loc.seg))?;
    file.seek(SeekFrom::Start(loc.offset))?;
    let mut buf = vec![0u8; loc.len as usize];
    file.read_exact(&mut buf)?;
    Ok(serde_json::from_slice(&buf)?)
}

/// Scan a shard directory and rebuild the id index by replaying segments in
/// sequence order. A truncated/partial tail line in the *last* segment is
/// tolerated (crash mid-append); corruption anywhere else is an error.
fn load_shard(dir: PathBuf) -> Result<Shard> {
    let mut shard = Shard {
        dir,
        ..Shard::default()
    };
    if !shard.dir.exists() {
        return Ok(shard);
    }
    let mut seqs = Vec::new();
    for entry in fs::read_dir(&shard.dir)? {
        let path = entry?.path();
        if path.extension().and_then(|e| e.to_str()) != Some(SEG_EXT) {
            continue;
        }
        let Some(stem) = path.file_stem().and_then(|s| s.to_str()) else {
            continue;
        };
        let Ok(seq) = stem.parse::<u64>() else {
            continue;
        };
        seqs.push(seq);
    }
    seqs.sort_unstable();

    for (pos, &seq) in seqs.iter().enumerate() {
        let is_last = pos + 1 == seqs.len();
        let data = fs::read(segment_path(&shard.dir, seq))?;
        let mut offset = 0u64;
        let mut clean_len = 0u64;
        for chunk in data.split_inclusive(|&b| b == b'\n') {
            let complete = chunk.last() == Some(&b'\n');
            let parsed = if complete {
                serde_json::from_slice::<RecordLine>(chunk).ok()
            } else {
                None
            };
            let Some(rec) = parsed else {
                if is_last {
                    break; // torn tail write — replay stops here
                }
                return Err(Error::Corrupt(format!(
                    "bad record in sealed segment {}",
                    segment_path(&shard.dir, seq).display()
                )));
            };
            let len = chunk.len() as u64;
            match rec.op.as_str() {
                "put" => {
                    shard.index.insert(
                        rec.id,
                        Loc {
                            seg: seq,
                            offset,
                            len,
                        },
                    );
                }
                "del" => {
                    shard.index.remove(&rec.id);
                }
                other => {
                    return Err(Error::Corrupt(format!(
                        "unknown op {other:?} in segment {}",
                        segment_path(&shard.dir, seq).display()
                    )));
                }
            }
            offset += len;
            clean_len = offset;
        }
        if is_last {
            // Drop torn tail bytes so future appends (which write at the
            // physical end of file) line up with the indexed offsets.
            if clean_len < data.len() as u64 {
                let file = OpenOptions::new()
                    .write(true)
                    .open(segment_path(&shard.dir, seq))?;
                file.set_len(clean_len)?;
                file.sync_data()?;
            }
            shard.open_len = clean_len;
        }
    }
    shard.segments = seqs;
    Ok(shard)
}

fn shard_bytes(shard: &Shard) -> Result<u64> {
    let mut total = 0;
    for &seq in &shard.segments {
        total += fs::metadata(segment_path(&shard.dir, seq))?.len();
    }
    Ok(total)
}

impl DocumentStore for SegmentStore {
    fn get(&self, collection: &str, id: &str) -> Result<Option<Vec<u8>>> {
        let handle = self.shard_handle(collection, self.shard_of(id))?;
        let shard = handle.lock().expect("shard poisoned");
        match shard.index.get(id) {
            None => Ok(None),
            Some(&loc) => Ok(Some(self.read_body(&shard, loc)?)),
        }
    }

    fn put(&self, collection: &str, id: &str, body: &[u8]) -> Result<()> {
        let stored = self.codec.encode(body)?;
        let line = record_line("put", id, Some(B64.encode(&stored)))?;
        let handle = self.shard_handle(collection, self.shard_of(id))?;
        let mut shard = handle.lock().expect("shard poisoned");
        let loc = self.append(&mut shard, &line)?;
        shard.index.insert(id.to_string(), loc);
        Ok(())
    }

    fn put_many(&self, collection: &str, items: Vec<(String, Vec<u8>)>) -> Result<()> {
        self.put_many_sharded(collection, items)
    }

    fn delete(&self, collection: &str, id: &str) -> Result<()> {
        let handle = self.shard_handle(collection, self.shard_of(id))?;
        let mut shard = handle.lock().expect("shard poisoned");
        if !shard.index.contains_key(id) {
            return Ok(()); // idempotent
        }
        let line = record_line("del", id, None)?;
        self.append(&mut shard, &line)?;
        shard.index.remove(id);
        Ok(())
    }

    fn list_prefix(&self, collection: &str, prefix: &str) -> Result<Vec<(String, Vec<u8>)>> {
        let mut merged: BTreeMap<String, Vec<u8>> = BTreeMap::new();
        for shard_no in self.shards_on_disk(collection)? {
            let handle = self.shard_handle(collection, shard_no)?;
            let shard = handle.lock().expect("shard poisoned");
            let ids: Vec<(String, Loc)> = shard
                .index
                .range(prefix.to_string()..)
                .take_while(|(id, _)| id.starts_with(prefix))
                .map(|(id, &loc)| (id.clone(), loc))
                .collect();
            for (id, loc) in ids {
                merged.insert(id, self.read_body(&shard, loc)?);
            }
        }
        Ok(merged.into_iter().collect())
    }

    fn transaction(&self, ops: Vec<TxnOp>) -> Result<()> {
        if ops.is_empty() {
            return Ok(());
        }
        // Encode bodies up front so the WAL is ciphertext at rest too.
        let mut wal_ops = Vec::with_capacity(ops.len());
        for op in &ops {
            wal_ops.push(match op {
                TxnOp::Put {
                    collection,
                    id,
                    body,
                } => WalOp {
                    op: "put".to_string(),
                    collection: collection.clone(),
                    id: id.clone(),
                    body: Some(B64.encode(self.codec.encode(body)?)),
                },
                TxnOp::Delete { collection, id } => WalOp {
                    op: "del".to_string(),
                    collection: collection.clone(),
                    id: id.clone(),
                    body: None,
                },
            });
        }

        let mut seq = self.txn_seq.lock().expect("txn seq poisoned");
        let wal_path = self.wal_dir().join(format!("{:016}.txn", *seq));
        *seq += 1;
        // Commit point: the fsynced rename. Before it, a crash leaves at
        // most a .tmp (discarded on recovery — rollback). After it,
        // recovery replays the whole record — all ops land.
        let tmp = wal_path.with_extension("txn.tmp");
        let mut file = File::create(&tmp)?;
        file.write_all(&serde_json::to_vec(&WalRecord { ops: wal_ops })?)?;
        file.sync_all()?;
        drop(file);
        fs::rename(&tmp, &wal_path)?;
        File::open(self.wal_dir())?.sync_all()?;

        for op in &ops {
            match op {
                TxnOp::Put {
                    collection,
                    id,
                    body,
                } => {
                    let stored = self.codec.encode(body)?;
                    self.put_stored(collection, id, B64.encode(&stored))?;
                }
                TxnOp::Delete { collection, id } => self.delete_deferred(collection, id)?,
            }
        }
        // Segment data must be durable before we drop the WAL commit record.
        self.flush()?;
        fs::remove_file(&wal_path)?;
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

    fn compact_shard(&self, collection: &str, shard_no: u16) -> Result<CompactStats> {
        let handle = self.shard_handle(collection, shard_no)?;
        let mut shard = handle.lock().expect("shard poisoned");
        let mut stats = CompactStats {
            collection: collection.to_string(),
            shard: shard_no,
            live_docs: shard.index.len() as u64,
            segments_removed: 0,
            bytes_before: shard_bytes(&shard)?,
            bytes_after: 0,
        };
        if shard.segments.is_empty() {
            return Ok(stats);
        }
        // Close any open append handle so we can rewrite/unlink cleanly.
        Self::sync_open_segment(&mut shard)?;
        shard.open_file = None;
        let old_segments = shard.segments.clone();
        let new_seq = old_segments.last().copied().unwrap_or(0) + 1;

        let mut new_index: BTreeMap<String, Loc> = BTreeMap::new();
        let mut new_len = 0u64;
        if !shard.index.is_empty() {
            // Fold live records into one new sealed segment, then swap it in
            // atomically before unlinking the old files.
            let tmp = shard.dir.join(format!("{new_seq:010}.{SEG_EXT}.tmp"));
            let mut out = File::create(&tmp)?;
            let entries: Vec<(String, Loc)> = shard
                .index
                .iter()
                .map(|(id, &loc)| (id.clone(), loc))
                .collect();
            for (id, loc) in entries {
                let rec = read_record(&shard, loc)?;
                let line = record_line("put", &id, rec.body)?;
                out.write_all(&line)?;
                new_index.insert(
                    id,
                    Loc {
                        seg: new_seq,
                        offset: new_len,
                        len: line.len() as u64,
                    },
                );
                new_len += line.len() as u64;
            }
            out.sync_all()?;
            drop(out);
            fs::rename(&tmp, segment_path(&shard.dir, new_seq))?;
        }
        for &seq in &old_segments {
            fs::remove_file(segment_path(&shard.dir, seq))?;
        }
        // Persist directory metadata (renames + unlinks) before reporting.
        File::open(&shard.dir)?.sync_all()?;

        shard.segments = if new_index.is_empty() {
            Vec::new()
        } else {
            vec![new_seq]
        };
        shard.index = new_index;
        shard.open_len = new_len;
        shard.open_file = None;
        shard.dirty_ops = 0;
        shard.dirty_bytes = 0;
        stats.segments_removed = old_segments.len() as u64;
        stats.bytes_after = new_len;
        Ok(stats)
    }

    fn compact(&self, collection: &str) -> Result<CompactStats> {
        let mut merged = CompactStats {
            collection: collection.to_string(),
            shard: 0,
            live_docs: 0,
            segments_removed: 0,
            bytes_before: 0,
            bytes_after: 0,
        };
        for shard_no in self.shards_on_disk(collection)? {
            let stats = self.compact_shard(collection, shard_no)?;
            merged.merge(&stats);
        }
        Ok(merged)
    }
}

impl Drop for SegmentStore {
    fn drop(&mut self) {
        let _ = self.flush();
    }
}
