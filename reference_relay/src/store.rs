//! Filesystem store for opaque log entries and content-addressed blobs.

use std::path::{Path, PathBuf};

use serde_json::Value;
use tokio::fs;

use crate::error::{ApiError, ApiResult};
use crate::heads::{self, Heads};

const MAX_BATCH: usize = 90;

/// On-disk relay state under `--data-dir`.
#[derive(Clone)]
pub(crate) struct Store {
    pub(crate) root: PathBuf,
    pub(crate) heads: Heads,
}

impl Store {
    pub(crate) async fn open(root: PathBuf) -> ApiResult<Self> {
        fs::create_dir_all(&root)
            .await
            .map_err(|_| ApiError::invalid("data_dir"))?;
        fs::create_dir_all(root.join("logs"))
            .await
            .map_err(|_| ApiError::invalid("data_dir"))?;
        fs::create_dir_all(root.join("blobs"))
            .await
            .map_err(|_| ApiError::invalid("data_dir"))?;
        fs::create_dir_all(root.join("staging"))
            .await
            .map_err(|_| ApiError::invalid("data_dir"))?;
        let heads = Heads::scan(&root.join("logs")).await;
        Ok(Self { root, heads })
    }

    /// The streams of one writer with their contiguous heads, sorted by device id.
    pub(crate) fn list_streams(
        &self,
        database_id: &str,
        writer_key: &str,
    ) -> Vec<heads::ListedStream> {
        self.heads.list(database_id, writer_key)
    }

    pub(crate) fn root(&self) -> &Path {
        &self.root
    }

    /// Append opaque entries. Identical duplicates are ok; conflicting seq → 409.
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
        let mut stored = 0u32;
        let mut any_dup = false;
        for entry in entries {
            match self.append_one(database_id, entry).await? {
                AppendOutcome::Stored => stored += 1,
                AppendOutcome::Duplicate => {
                    stored += 1;
                    any_dup = true;
                }
            }
        }
        Ok((stored, any_dup))
    }

    async fn append_one(&self, database_id: &str, entry: &Value) -> ApiResult<AppendOutcome> {
        let meta = EntryKey::from_value(entry)?;
        if meta.database_id != database_id {
            return Err(ApiError::invalid("entries.database_id"));
        }
        let hash = heads::hash_of(entry)?;
        let path = self.entry_path(&meta);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .await
                .map_err(|_| ApiError::invalid("entries"))?;
        }
        if path.exists() {
            let existing = read_json(&path).await?;
            if &existing == entry {
                // The file can be there with no head: a client that hangs up mid-append cancels
                // this handler after the rename has begun and before the head is recorded. The
                // client then sends the same bytes again, and this is the one place that sees
                // the entry, so the head takes it here. It is a no-op when the head has it.
                self.heads.record(
                    &meta.database_id,
                    &meta.node_key,
                    &meta.device_id,
                    meta.writer_seq,
                    hash,
                );
                return Ok(AppendOutcome::Duplicate);
            }
            return Err(ApiError::conflict("entries.writer_seq"));
        }
        // The entry is absent but the stream's contiguous head is at or past it: a trim dropped
        // it. It was on the relay (a head only counts stored entries), so this is a repeat of
        // an append that was already taken. It is not stored again.
        if self
            .heads
            .head_seq(&meta.database_id, &meta.node_key, &meta.device_id)
            .is_some_and(|head| head >= meta.writer_seq)
        {
            return Ok(AppendOutcome::Duplicate);
        }
        let bytes = serde_json::to_vec(entry).map_err(|_| ApiError::invalid("entries"))?;
        atomic_write(&path, &bytes).await?;
        // The floor is part of admission. A trim sets the floor first and then lists and
        // removes the entry files at or below it; this append writes the file first and then
        // looks at the floor. Either it sees the floor and takes its own file back, or the
        // floor was set after it looked, which is after the file was written, so the trim's
        // listing finds the file. No order leaves the entry stored below a floor.
        if self
            .heads
            .floor(&meta.database_id, &meta.node_key, &meta.device_id)
            >= meta.writer_seq
        {
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

    /// Read a device chain from `from_seq`, returning opaque JSON values.
    pub(crate) async fn read_entries(
        &self,
        database_id: &str,
        node_key: &str,
        device_id: &str,
        from_seq: u64,
        limit: usize,
    ) -> ApiResult<(Vec<Value>, Option<String>)> {
        require_hex64(device_id, "entries.device_id")?;
        let limit = limit.clamp(1, MAX_BATCH);
        let mut entries = Vec::new();
        let mut seq = from_seq.max(1);
        // A trim dropped the entries up to the floor of this stream. A reader that asks
        // for one of them is behind, and must be told, not answered with nothing.
        if seq <= self.heads.floor(database_id, node_key, device_id) {
            return Err(ApiError::trimmed("from_seq"));
        }
        while entries.len() < limit {
            let meta = EntryKey {
                database_id: database_id.to_owned(),
                node_key: node_key.to_owned(),
                device_id: device_id.to_owned(),
                writer_seq: seq,
            };
            let path = self.entry_path(&meta);
            if !path.exists() {
                break;
            }
            entries.push(read_json(&path).await?);
            seq += 1;
        }
        let cursor = if entries.len() == limit {
            Some(format!("{node_key}:{device_id}:{seq}"))
        } else {
            None
        };
        Ok((entries, cursor))
    }

    /// Read every device under `writer_key`, ordered by device id then seq.
    ///
    /// `from_device` resumes mid-writer (from a prior cursor). Later devices
    /// always start at seq 1 once the resume device is exhausted.
    pub(crate) async fn read_writer_entries(
        &self,
        database_id: &str,
        writer_key: &str,
        from_device: Option<&str>,
        from_seq: u64,
        limit: usize,
    ) -> ApiResult<(Vec<Value>, Option<String>)> {
        require_hex64(writer_key, "writer_key")?;
        if let Some(device) = from_device {
            require_hex64(device, "cursor")?;
        }
        let limit = limit.clamp(1, MAX_BATCH);
        let devices = self.list_devices(database_id, writer_key).await?;
        let start = match from_device {
            Some(device) => devices.iter().position(|d| d == device).unwrap_or(0),
            None => 0,
        };
        let mut entries = Vec::new();
        let mut seq = from_seq.max(1);
        for (index, device_id) in devices.iter().enumerate().skip(start) {
            if index > start {
                seq = 1;
            }
            while entries.len() < limit {
                let meta = EntryKey {
                    database_id: database_id.to_owned(),
                    node_key: writer_key.to_owned(),
                    device_id: device_id.clone(),
                    writer_seq: seq,
                };
                let path = self.entry_path(&meta);
                if !path.exists() {
                    break;
                }
                entries.push(read_json(&path).await?);
                seq += 1;
            }
            if entries.len() == limit {
                return Ok((entries, Some(format!("{writer_key}:{device_id}:{seq}"))));
            }
        }
        Ok((entries, None))
    }

    async fn list_devices(&self, database_id: &str, writer_key: &str) -> ApiResult<Vec<String>> {
        let dir = self.root.join(format!("logs/{database_id}/{writer_key}"));
        if !dir.exists() {
            return Ok(Vec::new());
        }
        let mut devices = Vec::new();
        let mut rd = fs::read_dir(&dir)
            .await
            .map_err(|_| ApiError::invalid("writer_key"))?;
        while let Some(ent) = rd
            .next_entry()
            .await
            .map_err(|_| ApiError::invalid("writer_key"))?
        {
            if !ent
                .file_type()
                .await
                .map_err(|_| ApiError::invalid("writer_key"))?
                .is_dir()
            {
                continue;
            }
            let name = ent.file_name().to_string_lossy().into_owned();
            if name.len() == 64 && name.bytes().all(|b| b.is_ascii_hexdigit()) {
                devices.push(name);
            }
        }
        devices.sort();
        Ok(devices)
    }

    fn entry_path(&self, meta: &EntryKey) -> PathBuf {
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

struct EntryKey {
    database_id: String,
    node_key: String,
    device_id: String,
    writer_seq: u64,
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
            .and_then(serde_json::Value::as_u64)
            .filter(|&s| s >= 1)
            .ok_or(ApiError::invalid("entries.writer_seq"))?;
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

pub(crate) fn require_hex64(s: &str, field: &str) -> ApiResult<()> {
    if s.len() == 64 && s.bytes().all(|b| b.is_ascii_hexdigit()) {
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
    let tmp = path.with_extension("tmp");
    fs::write(&tmp, bytes)
        .await
        .map_err(|_| ApiError::invalid("data_dir"))?;
    fs::rename(&tmp, path)
        .await
        .map_err(|_| ApiError::invalid("data_dir"))
}

pub(crate) fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}

/// Parse an opaque delivery cursor `node_key:device_id:next_seq`.
pub(crate) fn parse_cursor(cursor: &str) -> ApiResult<(String, String, u64)> {
    let parts: Vec<&str> = cursor.splitn(3, ':').collect();
    if parts.len() != 3 {
        return Err(ApiError::invalid("cursor"));
    }
    let seq: u64 = parts[2].parse().map_err(|_| ApiError::invalid("cursor"))?;
    if seq < 1 {
        return Err(ApiError::invalid("cursor"));
    }
    require_hex64(parts[0], "cursor")?;
    require_hex64(parts[1], "cursor")?;
    Ok((parts[0].to_owned(), parts[1].to_owned(), seq))
}
