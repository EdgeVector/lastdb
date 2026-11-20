//! Filesystem store for opaque log entries and content-addressed blobs.

use std::path::{Path, PathBuf};

use app_identity_crypto::hex_lower;
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio::fs;

use crate::error::{ApiError, ApiResult};

const MAX_BATCH: usize = 90;

/// On-disk relay state under `--data-dir`.
#[derive(Clone)]
pub(crate) struct Store {
    root: PathBuf,
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
        Ok(Self { root })
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
        let path = self.entry_path(&meta);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .await
                .map_err(|_| ApiError::invalid("entries"))?;
        }
        if path.exists() {
            let existing = read_json(&path).await?;
            if &existing == entry {
                return Ok(AppendOutcome::Duplicate);
            }
            return Err(ApiError::conflict("entries.writer_seq"));
        }
        let bytes = serde_json::to_vec(entry).map_err(|_| ApiError::invalid("entries"))?;
        atomic_write(&path, &bytes).await?;
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

    fn entry_path(&self, meta: &EntryKey) -> PathBuf {
        self.root.join(format!(
            "logs/{}/{}/{}/{}.json",
            meta.database_id, meta.node_key, meta.device_id, meta.writer_seq
        ))
    }

    pub(crate) async fn stage_begin(&self, sha256: &str, bytes: u64) -> ApiResult<String> {
        require_hex64(sha256, "sha256")?;
        if bytes == 0 {
            return Err(ApiError::invalid("bytes"));
        }
        let token = format!("{sha256}-{}", now_ms());
        let meta = serde_json::json!({ "sha256": sha256, "bytes": bytes });
        let path = self.root.join("staging").join(format!("{token}.meta.json"));
        atomic_write(
            &path,
            &serde_json::to_vec(&meta).map_err(|_| ApiError::invalid("sha256"))?,
        )
        .await?;
        Ok(token)
    }

    pub(crate) async fn stage_put(&self, token: &str, body: &[u8]) -> ApiResult<()> {
        let meta_path = self.root.join("staging").join(format!("{token}.meta.json"));
        if !meta_path.exists() {
            return Err(ApiError::not_found("upload.token"));
        }
        let meta = read_json(&meta_path).await?;
        let expected = meta
            .get("bytes")
            .and_then(serde_json::Value::as_u64)
            .ok_or(ApiError::invalid("bytes"))?;
        if body.len() as u64 != expected {
            return Err(ApiError::invalid("bytes"));
        }
        let path = self.root.join("staging").join(token);
        atomic_write(&path, body).await
    }

    pub(crate) async fn stage_commit(&self, sha256: &str, bytes: u64) -> ApiResult<()> {
        require_hex64(sha256, "sha256")?;
        let staging = self.find_staging(sha256).await?;
        let data = fs::read(&staging)
            .await
            .map_err(|_| ApiError::not_found("sha256"))?;
        if data.len() as u64 != bytes {
            return Err(ApiError::invalid("bytes"));
        }
        let digest = hex_lower(&Sha256::digest(&data));
        if digest != sha256 {
            return Err(ApiError::invalid("sha256"));
        }
        let dest = self.blob_path(sha256);
        atomic_write(&dest, &data).await?;
        let _ = fs::remove_file(&staging).await;
        let _ = fs::remove_file(staging.with_extension("meta.json")).await;
        Ok(())
    }

    async fn find_staging(&self, sha256: &str) -> ApiResult<PathBuf> {
        let dir = self.root.join("staging");
        let mut rd = fs::read_dir(&dir)
            .await
            .map_err(|_| ApiError::not_found("sha256"))?;
        while let Some(ent) = rd
            .next_entry()
            .await
            .map_err(|_| ApiError::not_found("sha256"))?
        {
            let name = ent.file_name();
            let name = name.to_string_lossy();
            if name.starts_with(sha256) && !name.ends_with(".meta.json") {
                return Ok(ent.path());
            }
        }
        Err(ApiError::not_found("sha256"))
    }

    pub(crate) async fn blob_meta(&self, sha256: &str) -> ApiResult<(u64, PathBuf)> {
        require_hex64(sha256, "sha256")?;
        let path = self.blob_path(sha256);
        let meta = fs::metadata(&path)
            .await
            .map_err(|_| ApiError::not_found("sha256"))?;
        Ok((meta.len(), path))
    }

    pub(crate) async fn read_blob(&self, sha256: &str) -> ApiResult<Vec<u8>> {
        let (_, path) = self.blob_meta(sha256).await?;
        fs::read(&path)
            .await
            .map_err(|_| ApiError::not_found("sha256"))
    }

    fn blob_path(&self, sha256: &str) -> PathBuf {
        self.root.join("blobs").join(sha256)
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

fn require_hex64(s: &str, field: &str) -> ApiResult<()> {
    if s.len() == 64 && s.bytes().all(|b| b.is_ascii_hexdigit()) {
        Ok(())
    } else {
        Err(ApiError::invalid(field))
    }
}

async fn read_json(path: &Path) -> ApiResult<Value> {
    let bytes = fs::read(path)
        .await
        .map_err(|_| ApiError::not_found("entries"))?;
    serde_json::from_slice(&bytes).map_err(|_| ApiError::invalid("entries"))
}

async fn atomic_write(path: &Path, bytes: &[u8]) -> ApiResult<()> {
    let tmp = path.with_extension("tmp");
    fs::write(&tmp, bytes)
        .await
        .map_err(|_| ApiError::invalid("data_dir"))?;
    fs::rename(&tmp, path)
        .await
        .map_err(|_| ApiError::invalid("data_dir"))
}

fn now_ms() -> u64 {
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
