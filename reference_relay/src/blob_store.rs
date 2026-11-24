//! Content-addressed blob storage isolated per `database_id`. A blob is never held whole in memory.
//!
//! Upload: `stage_begin` records the claimed size and digest under `staging/<database_id>/`,
//! `stage_put` streams the request body to a unique `.part.<id>` file while hashing it, and
//! refuses the file unless `x-amz-checksum-sha256`, byte count, and SHA-256 match the claim.
//! `stage_commit` moves the verified file into `blobs/<sha256>` and records per-database
//! ownership at `blobs/<database_id>/<sha256>`, and removes the pending records of the digest.
//! Download opens the file only when that database owns the digest, and the HTTP layer streams
//! it in chunks. `blob_reclaim` lists the blobs a database owns and drops them.

use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use app_identity_crypto::hex_lower;
use axum::body::Bytes;
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;
use futures::{Stream, StreamExt};
use sha2::{Digest, Sha256};
use tokio::fs;
use tokio::io::AsyncWriteExt;

use crate::error::{ApiError, ApiResult};
use crate::store::{atomic_write, is_hex64, now_ms, read_json, require_hex64, Store};

static TOKEN_COUNTER: AtomicU64 = AtomicU64::new(1);
static PART_COUNTER: AtomicU64 = AtomicU64::new(1);

/// Removes an in-flight `.part.<id>` file if the upload fails or its connection is dropped.
struct PartGuard(Option<PathBuf>);

impl PartGuard {
    fn disarm(&mut self) {
        self.0 = None;
    }
}

impl Drop for PartGuard {
    fn drop(&mut self) {
        if let Some(path) = self.0.take() {
            let _ = std::fs::remove_file(path);
        }
    }
}

/// The `x-amz-checksum-sha256` value of a blob: base64 of the 32 digest bytes of the
/// lowercase hex `sha256`, which the caller has already checked with `require_hex64`.
pub(crate) fn checksum_header(sha256: &str) -> String {
    let nibble = |b: u8| {
        if b.is_ascii_digit() {
            b - b'0'
        } else {
            b - b'a' + 10
        }
    };
    let raw: Vec<u8> = sha256
        .as_bytes()
        .chunks(2)
        .map(|pair| (nibble(pair[0]) << 4) | nibble(pair[1]))
        .collect();
    BASE64.encode(raw)
}

impl Store {
    /// True when `database_id` already committed `sha256` with length `bytes`.
    /// Fails with 400 `bytes` if `database_id` owns `sha256` at a different length.
    pub(crate) async fn has_blob(
        &self,
        database_id: &str,
        sha256: &str,
        bytes: u64,
    ) -> ApiResult<bool> {
        require_hex64(database_id, "database_id")?;
        require_hex64(sha256, "sha256")?;
        if fs::metadata(self.db_blob_marker(database_id, sha256))
            .await
            .is_err()
        {
            return Ok(false);
        }
        let Ok(meta) = fs::metadata(self.blob_path(sha256)).await else {
            return Ok(false);
        };
        if meta.len() != bytes {
            return Err(ApiError::invalid("bytes"));
        }
        Ok(true)
    }

    pub(crate) async fn stage_begin(
        &self,
        database_id: &str,
        sha256: &str,
        bytes: u64,
    ) -> ApiResult<String> {
        require_hex64(database_id, "database_id")?;
        require_hex64(sha256, "sha256")?;
        if bytes == 0 {
            return Err(ApiError::invalid("bytes"));
        }
        let seq = TOKEN_COUNTER.fetch_add(1, Ordering::Relaxed);
        let token = format!("{sha256}-{}-{seq}", now_ms());
        let dir = self.staging_dir(database_id);
        fs::create_dir_all(&dir)
            .await
            .map_err(|_| ApiError::invalid("data_dir"))?;
        let meta = serde_json::json!({ "sha256": sha256, "bytes": bytes });
        let path = dir.join(format!("{token}.meta.json"));
        atomic_write(
            &path,
            &serde_json::to_vec(&meta).map_err(|_| ApiError::invalid("sha256"))?,
        )
        .await?;
        Ok(token)
    }

    /// Stream an upload body into a unique `.part.<id>` file for `(database_id, token)`, hashing
    /// as the bytes pass. Only a complete, digest-verified upload is renamed to `token`.
    pub(crate) async fn stage_put<S>(
        &self,
        database_id: &str,
        token: &str,
        checksum: Option<&str>,
        mut body: S,
    ) -> ApiResult<()>
    where
        S: Stream<Item = Result<Bytes, axum::Error>> + Unpin,
    {
        let (sha256, expected) = self.staged_claim(database_id, token).await?;
        if checksum != Some(checksum_header(&sha256).as_str()) {
            return Err(ApiError::invalid("x-amz-checksum-sha256"));
        }
        let attempt = PART_COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir = self.staging_dir(database_id);
        let part = dir.join(format!("{token}.part.{attempt}"));
        let mut guard = PartGuard(Some(part.clone()));
        let (count, digest) = receive(&part, &mut body, expected).await?;
        if count != expected {
            return Err(ApiError::invalid("bytes"));
        }
        if digest != sha256 {
            return Err(ApiError::invalid("sha256"));
        }
        fs::rename(&part, dir.join(token))
            .await
            .map_err(|_| ApiError::invalid("data_dir"))?;
        guard.disarm();
        Ok(())
    }

    /// The `(sha256, bytes)` that `stage_begin` recorded for `(database_id, token)`.
    async fn staged_claim(&self, database_id: &str, token: &str) -> ApiResult<(String, u64)> {
        require_hex64(database_id, "database_id")?;
        if !valid_token(token) {
            return Err(ApiError::not_found("upload.token"));
        }
        let meta_path = self
            .staging_dir(database_id)
            .join(format!("{token}.meta.json"));
        let meta = read_json(&meta_path)
            .await
            .map_err(|_| ApiError::not_found("upload.token"))?;
        let sha256 = meta
            .get("sha256")
            .and_then(serde_json::Value::as_str)
            .filter(|s| is_hex64(s) && token.starts_with(s))
            .ok_or_else(|| ApiError::invalid("sha256"))?
            .to_owned();
        let bytes = meta
            .get("bytes")
            .and_then(serde_json::Value::as_u64)
            .ok_or_else(|| ApiError::invalid("bytes"))?;
        Ok((sha256, bytes))
    }

    /// Publish a staged upload for `database_id`. Repeat commits of an already-committed
    /// `(database_id, sha256, bytes)` succeed idempotently.
    pub(crate) async fn stage_commit(
        &self,
        database_id: &str,
        sha256: &str,
        bytes: u64,
    ) -> ApiResult<()> {
        require_hex64(database_id, "database_id")?;
        require_hex64(sha256, "sha256")?;
        if bytes == 0 {
            return Err(ApiError::invalid("bytes"));
        }
        let Some(staging) = self.find_staging(database_id, sha256).await? else {
            if self.has_blob(database_id, sha256, bytes).await? {
                return Ok(());
            }
            return Err(ApiError::not_found("sha256"));
        };
        let len = fs::metadata(&staging)
            .await
            .map_err(|_| ApiError::not_found("sha256"))?
            .len();
        if len != bytes {
            return Err(ApiError::invalid("bytes"));
        }
        fs::rename(&staging, self.blob_path(sha256))
            .await
            .map_err(|_| ApiError::invalid("data_dir"))?;
        let marker = self.db_blob_marker(database_id, sha256);
        if let Some(parent) = marker.parent() {
            fs::create_dir_all(parent)
                .await
                .map_err(|_| ApiError::invalid("data_dir"))?;
        }
        atomic_write(&marker, b"").await?;
        // No pending record of the digest outlives its commit.
        self.clear_pending(database_id, sha256).await;
        Ok(())
    }

    /// Find a completed staging file for `(database_id, sha256)` if one exists.
    async fn find_staging(&self, database_id: &str, sha256: &str) -> ApiResult<Option<PathBuf>> {
        let dir = self.staging_dir(database_id);
        let mut rd = match fs::read_dir(&dir).await {
            Ok(rd) => rd,
            Err(err) if err.kind() == ErrorKind::NotFound => return Ok(None),
            Err(_) => return Err(ApiError::invalid("data_dir")),
        };
        let prefix = format!("{sha256}-");
        while let Some(ent) = rd
            .next_entry()
            .await
            .map_err(|_| ApiError::invalid("data_dir"))?
        {
            let name = ent.file_name();
            let name = name.to_string_lossy();
            if name.starts_with(&prefix) && !name.contains('.') {
                return Ok(Some(ent.path()));
            }
        }
        Ok(None)
    }

    pub(crate) async fn blob_meta(
        &self,
        database_id: &str,
        sha256: &str,
    ) -> ApiResult<(u64, PathBuf)> {
        require_hex64(database_id, "database_id")?;
        require_hex64(sha256, "sha256")?;
        fs::metadata(self.db_blob_marker(database_id, sha256))
            .await
            .map_err(|_| ApiError::not_found("blob.sha256"))?;
        let path = self.blob_path(sha256);
        let meta = fs::metadata(&path)
            .await
            .map_err(|_| ApiError::not_found("blob.sha256"))?;
        Ok((meta.len(), path))
    }

    /// Open a stored blob owned by `database_id` for chunked reading, with its length.
    pub(crate) async fn open_blob(
        &self,
        database_id: &str,
        sha256: &str,
    ) -> ApiResult<(fs::File, u64)> {
        let (len, path) = self.blob_meta(database_id, sha256).await?;
        let file = fs::File::open(&path)
            .await
            .map_err(|_| ApiError::not_found("blob.sha256"))?;
        Ok((file, len))
    }

    pub(crate) fn blob_path(&self, sha256: &str) -> PathBuf {
        self.root.join("blobs").join(sha256)
    }

    pub(crate) fn db_blob_marker(&self, database_id: &str, sha256: &str) -> PathBuf {
        self.root.join("blobs").join(database_id).join(sha256)
    }

    pub(crate) fn staging_dir(&self, database_id: &str) -> PathBuf {
        self.root.join("staging").join(database_id)
    }
}

fn valid_token(token: &str) -> bool {
    token.split_once('-').is_some_and(|(sha, suffix)| {
        is_hex64(sha)
            && !suffix.is_empty()
            && !suffix.starts_with('-')
            && !suffix.ends_with('-')
            && suffix.bytes().all(|b| b.is_ascii_digit() || b == b'-')
    })
}

/// Write `body` to `part`, stopping with an error once more than `limit` bytes
/// arrive. Returns the byte count and the lowercase-hex SHA-256 of what passed.
async fn receive<S>(part: &Path, body: &mut S, limit: u64) -> ApiResult<(u64, String)>
where
    S: Stream<Item = Result<Bytes, axum::Error>> + Unpin,
{
    let mut file = fs::File::create(part)
        .await
        .map_err(|_| ApiError::invalid("data_dir"))?;
    let mut hasher = Sha256::new();
    let mut count = 0u64;
    while let Some(chunk) = body.next().await {
        let chunk = chunk.map_err(|_| ApiError::invalid("body"))?;
        count += chunk.len() as u64;
        if count > limit {
            return Err(ApiError::invalid("bytes"));
        }
        hasher.update(&chunk);
        file.write_all(&chunk)
            .await
            .map_err(|_| ApiError::invalid("data_dir"))?;
    }
    file.flush()
        .await
        .map_err(|_| ApiError::invalid("data_dir"))?;
    Ok((count, hex_lower(&hasher.finalize())))
}
