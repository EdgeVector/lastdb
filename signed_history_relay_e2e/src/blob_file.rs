//! `blob.put` and `blob.get` for a file on disk, streamed in chunks.
//!
//! The bundle path in `relay.rs` holds each blob in memory (a bundle is at most
//! 64 MiB and is generated in memory). These two methods move a file of any
//! size: the upload reads the file in chunks, and the download writes to a file
//! while hashing, so memory does not grow with the file.

use std::path::Path;

use app_identity_crypto::hex_lower;
use futures::StreamExt;
use serde_json::json;
use sha2::{Digest, Sha256};
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::relay::{header_pairs, RelayClient};

const CHUNK_BYTES: usize = 64 * 1024;

impl RelayClient {
    /// `blob.put` begin, the presigned upload streamed from `source`, commit.
    pub async fn blob_put_file(&self, sha256: &str, source: &Path) -> Result<(), String> {
        let file = fs::File::open(source)
            .await
            .map_err(|e| format!("blob file: {e}"))?;
        let size = file
            .metadata()
            .await
            .map_err(|e| format!("blob file: {e}"))?
            .len();
        let stage_body = |stage: &str| {
            json!({
                "database_id": self.database_hex,
                "sha256": sha256,
                "bytes": size,
                "stage": stage,
            })
        };
        let begin = self
            .post("/relay/v1/blob/put", &stage_body("begin"))
            .await?;
        let url = begin["upload"]["url"]
            .as_str()
            .ok_or("blob.put begin has no upload.url")?;
        let mut req = self
            .http
            .put(url)
            .header("content-length", size)
            .body(file_body(file));
        for (name, value) in header_pairs(&begin["upload"]["headers"]) {
            req = req.header(name, value);
        }
        let status = req
            .send()
            .await
            .map_err(|e| format!("blob upload: {e}"))?
            .status();
        if !status.is_success() {
            return Err(format!("blob upload HTTP {status}"));
        }
        self.post("/relay/v1/blob/put", &stage_body("commit"))
            .await?;
        Ok(())
    }

    /// `blob.get`, then the presigned download written to `dest`. Returns the
    /// SHA-256 (lowercase hex) of the bytes that were written.
    pub async fn blob_get_file(
        &self,
        sha256: &str,
        expect_size: u64,
        dest: &Path,
    ) -> Result<String, String> {
        let body = json!({ "database_id": self.database_hex, "sha256": sha256 });
        let meta = self.post("/relay/v1/blob/get", &body).await?;
        let url = meta["url"].as_str().ok_or("blob.get has no url")?;
        if meta["bytes"].as_u64().is_some_and(|n| n != expect_size) {
            return Err(format!("blob.get size differs from the entry for {sha256}"));
        }
        let mut req = self.http.get(url);
        for (name, value) in header_pairs(&meta["headers"]) {
            req = req.header(name, value);
        }
        let resp = req
            .send()
            .await
            .map_err(|e| format!("blob download: {e}"))?;
        if !resp.status().is_success() {
            return Err(format!("blob download HTTP {}", resp.status()));
        }
        let mut file = fs::File::create(dest)
            .await
            .map_err(|e| format!("blob file: {e}"))?;
        let mut hasher = Sha256::new();
        let mut chunks = resp.bytes_stream();
        while let Some(chunk) = chunks.next().await {
            let chunk = chunk.map_err(|e| format!("blob download body: {e}"))?;
            hasher.update(&chunk);
            file.write_all(&chunk)
                .await
                .map_err(|e| format!("blob file: {e}"))?;
        }
        file.flush().await.map_err(|e| format!("blob file: {e}"))?;
        Ok(hex_lower(&hasher.finalize()))
    }
}

fn file_body(file: fs::File) -> reqwest::Body {
    let chunks = futures::stream::unfold(file, |mut file| async move {
        let mut buf = vec![0u8; CHUNK_BYTES];
        match file.read(&mut buf).await {
            Ok(0) => None,
            Ok(n) => {
                buf.truncate(n);
                Some((Ok::<Vec<u8>, std::io::Error>(buf), file))
            }
            Err(error) => Some((Err(error), file)),
        }
    });
    reqwest::Body::wrap_stream(chunks)
}
