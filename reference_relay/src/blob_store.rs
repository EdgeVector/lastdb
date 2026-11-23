//! Content-addressed blob storage. A blob is never held whole in memory.
//!
//! Upload: `stage_begin` records the claimed size and digest, `stage_put`
//! streams the request body to a staging file while hashing it, and refuses the
//! file unless the byte count and SHA-256 match the claim. Like an object store that
//! verifies a signed `x-amz-checksum-sha256`, it also refuses an upload that does not carry
//! that header with the base64 SHA-256 of the claim (see [`checksum_header`]). `stage_commit` then
//! moves the verified file into `blobs/` by rename. Download opens the file and
//! the HTTP layer reads it in chunks.

use std::path::PathBuf;

use app_identity_crypto::hex_lower;
use axum::body::Bytes;
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;
use futures::{Stream, StreamExt};
use sha2::{Digest, Sha256};
use tokio::fs;
use tokio::io::AsyncWriteExt;

use crate::error::{ApiError, ApiResult};
use crate::store::{atomic_write, now_ms, read_json, require_hex64, Store};

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

    /// Stream an upload body into the staging file for `token`, hashing as the
    /// bytes pass. A wrong size or digest removes the partial file and fails.
    pub(crate) async fn stage_put<S>(
        &self,
        token: &str,
        checksum: Option<&str>,
        mut body: S,
    ) -> ApiResult<()>
    where
        S: Stream<Item = Result<Bytes, axum::Error>> + Unpin,
    {
        let (sha256, expected) = self.staged_claim(token).await?;
        if checksum != Some(checksum_header(&sha256).as_str()) {
            return Err(ApiError::invalid("x-amz-checksum-sha256"));
        }
        let part = self.root.join("staging").join(format!("{token}.part"));
        let received = receive(&part, &mut body, expected).await;
        let outcome = match received {
            Ok((count, _)) if count != expected => Err(ApiError::invalid("bytes")),
            Ok((_, digest)) if digest != sha256 => Err(ApiError::invalid("sha256")),
            Ok(_) => Ok(()),
            Err(error) => Err(error),
        };
        match outcome {
            Ok(()) => fs::rename(&part, self.root.join("staging").join(token))
                .await
                .map_err(|_| ApiError::invalid("data_dir")),
            Err(error) => {
                let _ = fs::remove_file(&part).await;
                Err(error)
            }
        }
    }

    /// The `(sha256, bytes)` that `stage_begin` recorded for `token`.
    async fn staged_claim(&self, token: &str) -> ApiResult<(String, u64)> {
        let valid = token.split_once('-').is_some_and(|(sha, ms)| {
            sha.len() == 64
                && sha.bytes().all(|b| b.is_ascii_hexdigit())
                && !ms.is_empty()
                && ms.bytes().all(|b| b.is_ascii_digit())
        });
        if !valid {
            return Err(ApiError::not_found("upload.token"));
        }
        let meta_path = self.root.join("staging").join(format!("{token}.meta.json"));
        if !meta_path.exists() {
            return Err(ApiError::not_found("upload.token"));
        }
        let meta = read_json(&meta_path).await?;
        let sha256 = meta
            .get("sha256")
            .and_then(serde_json::Value::as_str)
            .ok_or(ApiError::invalid("sha256"))?
            .to_owned();
        let bytes = meta
            .get("bytes")
            .and_then(serde_json::Value::as_u64)
            .ok_or(ApiError::invalid("bytes"))?;
        Ok((sha256, bytes))
    }

    /// Publish a staged upload. `stage_put` already checked its size and digest,
    /// so this only confirms the size and renames the file into `blobs/`.
    pub(crate) async fn stage_commit(&self, sha256: &str, bytes: u64) -> ApiResult<()> {
        require_hex64(sha256, "sha256")?;
        let staging = self.find_staging(sha256).await?;
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
        let _ = fs::remove_file(staging.with_extension("meta.json")).await;
        Ok(())
    }

    /// The finished upload for `sha256`: a staging file named like its token.
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
            if name.starts_with(sha256) && !name.contains('.') {
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

    /// Open a stored blob for chunked reading, with its length.
    pub(crate) async fn open_blob(&self, sha256: &str) -> ApiResult<(fs::File, u64)> {
        let (len, path) = self.blob_meta(sha256).await?;
        let file = fs::File::open(&path)
            .await
            .map_err(|_| ApiError::not_found("sha256"))?;
        Ok((file, len))
    }

    fn blob_path(&self, sha256: &str) -> PathBuf {
        self.root.join("blobs").join(sha256)
    }
}

/// Write `body` to `part`, stopping with an error once more than `limit` bytes
/// arrive. Returns the byte count and the lowercase-hex SHA-256 of what passed.
async fn receive<S>(part: &std::path::Path, body: &mut S, limit: u64) -> ApiResult<(u64, String)>
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
