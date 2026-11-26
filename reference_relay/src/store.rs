//! Filesystem store for opaque log entries and content-addressed blobs.

use std::collections::HashMap;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use serde_json::Value;
use tokio::fs;

use crate::error::{ApiError, ApiResult};
use crate::heads::{self, Heads};

pub(crate) const MAX_BATCH: usize = 90;
static TMP_COUNTER: AtomicU64 = AtomicU64::new(1);

/// On-disk relay state under `--data-dir`.
#[derive(Clone)]
pub(crate) struct Store {
    pub(crate) root: PathBuf,
    pub(crate) heads: Heads,
}

impl Store {
    pub(crate) async fn open(root: PathBuf) -> ApiResult<Self> {
        for sub in ["logs", "blobs", "staging"] {
            fs::create_dir_all(root.join(sub))
                .await
                .map_err(|_| ApiError::invalid("data_dir"))?;
        }
        let heads = Heads::scan(&root.join("logs")).await?;
        Ok(Self { root, heads })
    }

    /// The streams of one writer with their contiguous heads, sorted by device id.
    pub(crate) fn list_streams(
        &self,
        database_id: &str,
        writer_key: &str,
    ) -> Vec<relay_protocol::StreamHead> {
        self.heads.list(database_id, writer_key)
    }

    pub(crate) fn root(&self) -> &Path {
        &self.root
    }

    /// Append opaque entries. Pre-validates and checks conflicts across the whole batch before
    /// writing any file so a partial batch never lands on disk.
    pub(crate) async fn append_entries(
        &self,
        database_id: &str,
        entries: &[Value],
    ) -> ApiResult<(u32, bool)> {
        if entries.is_empty() {
            return Err(ApiError::invalid("entries"));
        }
        if entries.len() > MAX_BATCH {
            return Err(ApiError::batch_too_large("entries"));
        }
        let prepared = self.prepare_batch(database_id, entries).await?;
        let mut stored = 0u32;
        let mut any_dup = false;
        for item in prepared {
            match self.commit_prepared(item).await? {
                AppendOutcome::Stored => stored += 1,
                AppendOutcome::Duplicate => {
                    stored += 1;
                    any_dup = true;
                }
            }
        }
        Ok((stored, any_dup))
    }

    async fn prepare_batch(
        &self,
        database_id: &str,
        entries: &[Value],
    ) -> ApiResult<Vec<PreparedEntry>> {
        let mut seen: HashMap<(String, String, u64), &Value> =
            HashMap::with_capacity(entries.len());
        let mut out = Vec::with_capacity(entries.len());
        for entry in entries {
            let meta = EntryKey::from_value(entry)?;
            if meta.database_id != database_id {
                return Err(ApiError::invalid("entries.database_id"));
            }
            let hash = heads::hash_of(entry)?;
            let stream_key = (
                meta.node_key.clone(),
                meta.device_id.clone(),
                meta.writer_seq,
            );
            if let Some(prev) = seen.get(&stream_key) {
                if *prev != entry {
                    return Err(ApiError::conflict("entries.writer_seq"));
                }
                out.push(PreparedEntry::BatchDuplicate);
                continue;
            }
            seen.insert(stream_key, entry);
            let state = self.check_existing(&meta, entry, &hash).await?;
            out.push(state);
        }
        Ok(out)
    }

    async fn check_existing(
        &self,
        meta: &EntryKey,
        entry: &Value,
        hash: &str,
    ) -> ApiResult<PreparedEntry> {
        let path = self.entry_path(meta);
        match fs::read(&path).await {
            Ok(raw) => {
                let existing: Value =
                    serde_json::from_slice(&raw).map_err(|_| ApiError::invalid("entries"))?;
                // A segment that ends here holds the entry: a repeat, as on the Exemem relay.
                if existing.get("segment").is_some() {
                    return Ok(PreparedEntry::BatchDuplicate);
                }
                if &existing != entry {
                    return Err(ApiError::conflict("entries.writer_seq"));
                }
                Ok(PreparedEntry::DiskDuplicate {
                    meta: meta.clone(),
                    hash: hash.to_owned(),
                })
            }
            Err(err) if err.kind() == ErrorKind::NotFound => {
                if self.check_floor_admission(meta, hash)? {
                    return Ok(PreparedEntry::BatchDuplicate);
                }
                // Absent at or below the contiguous head: a segment holds it. A repeat.
                let head = self
                    .heads
                    .head_seq(&meta.database_id, &meta.node_key, &meta.device_id);
                if head.is_some_and(|head| head >= meta.writer_seq) {
                    return Ok(PreparedEntry::BatchDuplicate);
                }
                let bytes = serde_json::to_vec(entry).map_err(|_| ApiError::invalid("entries"))?;
                Ok(PreparedEntry::Write {
                    meta: meta.clone(),
                    hash: hash.to_owned(),
                    path,
                    bytes,
                })
            }
            Err(_) => Err(ApiError::invalid("data_dir")),
        }
    }

    /// Returns `Ok(true)` if `meta` is at or below the stream's trimmed floor (duplicate),
    /// `Ok(false)` if above the floor, or `Err(conflict)` if at `floor` with a different hash.
    fn check_floor_admission(&self, meta: &EntryKey, hash: &str) -> ApiResult<bool> {
        let (floor, floor_hash) =
            self.heads
                .floor_info(&meta.database_id, &meta.node_key, &meta.device_id);
        if meta.writer_seq > floor {
            return Ok(false);
        }
        if meta.writer_seq == floor && floor_hash.as_deref().is_some_and(|fh| fh != hash) {
            return Err(ApiError::conflict("entries.writer_seq"));
        }
        Ok(true)
    }

    async fn commit_prepared(&self, item: PreparedEntry) -> ApiResult<AppendOutcome> {
        match item {
            PreparedEntry::BatchDuplicate => Ok(AppendOutcome::Duplicate),
            PreparedEntry::DiskDuplicate { meta, hash } => {
                self.heads.record(
                    &meta.database_id,
                    &meta.node_key,
                    &meta.device_id,
                    meta.writer_seq,
                    hash,
                );
                Ok(AppendOutcome::Duplicate)
            }
            PreparedEntry::Write {
                meta,
                hash,
                path,
                bytes,
            } => {
                if let Some(parent) = path.parent() {
                    fs::create_dir_all(parent)
                        .await
                        .map_err(|_| ApiError::invalid("entries"))?;
                }
                atomic_write(&path, &bytes).await?;
                if self.check_floor_admission(&meta, &hash)? {
                    let _ = fs::remove_file(&path).await;
                    return Ok(AppendOutcome::Duplicate);
                }
                self.heads.record(
                    &meta.database_id,
                    &meta.node_key,
                    &meta.device_id,
                    meta.writer_seq,
                    hash,
                );
                Ok(AppendOutcome::Stored)
            }
        }
    }

    pub(crate) fn entry_path(&self, meta: &EntryKey) -> PathBuf {
        self.root.join(format!(
            "logs/{}/{}/{}/{}.json",
            meta.database_id, meta.node_key, meta.device_id, meta.writer_seq
        ))
    }
}

enum AppendOutcome {
    Stored,
    Duplicate,
}

enum PreparedEntry {
    BatchDuplicate,
    DiskDuplicate {
        meta: EntryKey,
        hash: String,
    },
    Write {
        meta: EntryKey,
        hash: String,
        path: PathBuf,
        bytes: Vec<u8>,
    },
}

#[derive(Clone)]
pub(crate) struct EntryKey {
    pub(crate) database_id: String,
    pub(crate) node_key: String,
    pub(crate) device_id: String,
    pub(crate) writer_seq: u64,
}

impl EntryKey {
    fn from_value(entry: &Value) -> ApiResult<Self> {
        let database_id = str_field(entry, "database_id")?;
        let node_key = str_field(entry, "node_key")?;
        let device_id = str_field(entry, "device_id")?;
        require_hex64(&database_id, "entries.database_id")?;
        require_hex64(&node_key, "entries.node_key")?;
        require_hex64(&device_id, "entries.device_id")?;
        let writer_seq = entry
            .get("writer_seq")
            .and_then(Value::as_u64)
            .filter(|&s| s >= 1)
            .ok_or_else(|| ApiError::invalid("entries.writer_seq"))?;
        Ok(Self {
            database_id,
            node_key,
            device_id,
            writer_seq,
        })
    }
}

fn str_field(entry: &Value, field: &str) -> ApiResult<String> {
    entry
        .get(field)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| ApiError::invalid(format!("entries.{field}")))
}

pub(crate) fn is_hex64(s: &str) -> bool {
    s.len() == 64
        && s.bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

pub(crate) fn require_hex64(s: &str, field: &str) -> ApiResult<()> {
    if is_hex64(s) {
        Ok(())
    } else {
        Err(ApiError::invalid(field))
    }
}

pub(crate) async fn read_json(path: &Path) -> ApiResult<Value> {
    let bytes = fs::read(path)
        .await
        .map_err(|_| ApiError::not_found("entries"))?;
    serde_json::from_slice(&bytes).map_err(|_| ApiError::invalid("entries"))
}

pub(crate) async fn atomic_write(path: &Path, bytes: &[u8]) -> ApiResult<()> {
    let id = TMP_COUNTER.fetch_add(1, Ordering::Relaxed);
    let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("write");
    let tmp = path.with_file_name(format!(".{name}.{id}.tmp"));
    if fs::write(&tmp, bytes).await.is_err() {
        let _ = fs::remove_file(&tmp).await;
        return Err(ApiError::invalid("data_dir"));
    }
    if fs::rename(&tmp, path).await.is_err() {
        let _ = fs::remove_file(&tmp).await;
        return Err(ApiError::invalid("data_dir"));
    }
    Ok(())
}

pub(crate) fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}
