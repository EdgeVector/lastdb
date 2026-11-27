//! `blob.put` and `blob.get` for a file on disk, streamed in chunks.
//!
//! The bundle path in `relay.rs` holds each blob in memory (a bundle is at most
//! 64 MiB and is generated in memory). These two methods move a file of any
//! size: the upload reads the file in chunks, and the download writes to a file
//! while hashing, so memory does not grow with the file.

use std::path::Path;

use app_identity_crypto::hex_lower;
use futures::StreamExt;
use relay_protocol::{BlobGet, BlobGetRequest, BlobPut, BlobPutStage};
use sha2::{Digest, Sha256};
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::ledger::Ledger;
use crate::relay::RelayClient;

const CHUNK_BYTES: usize = 64 * 1024;

impl RelayClient {
    /// Prove that the store, not the client, verifies an upload: begin a blob of `body`, then send
    /// the upload three wrong ways and expect each refused with a 4xx (the header left out, other
    /// bytes of the same length under the returned checksum, a different checksum value), then send
    /// it right and commit. Returns how many wrong uploads were refused.
    pub async fn prove_upload_verified(&self, body: &[u8], ledger: &Ledger) -> Result<u32, String> {
        let sha256 = hex_lower(&Sha256::digest(body));
        let size = body.len() as u64;
        let begin = self
            .call::<BlobPut>(&self.blob_put_request(&sha256, size, BlobPutStage::Begin))
            .await?;
        let upload = begin.upload.ok_or("blob.put begin has no upload.url")?;
        let url = upload.url;
        let headers: Vec<(String, String)> = upload
            .headers
            .into_iter()
            .filter(|(name, _)| !name.eq_ignore_ascii_case("content-length"))
            .collect();
        let is_checksum = |name: &str| name.eq_ignore_ascii_case("x-amz-checksum-sha256");
        let checksum = headers
            .iter()
            .find(|(name, _)| is_checksum(name))
            .map(|(_, value)| value.clone())
            .ok_or("blob.put begin returned no x-amz-checksum-sha256 header")?;
        let mut other_bytes = body.to_vec();
        other_bytes[0] ^= 0xff;
        let flipped = flip_first_base64_char(&checksum)?;
        let mut other_checksum = headers.clone();
        for (name, value) in &mut other_checksum {
            if is_checksum(name) {
                *value = flipped.clone();
            }
        }
        let without: Vec<_> = headers
            .iter()
            .filter(|(name, _)| !is_checksum(name))
            .cloned()
            .collect();
        let mut refused = 0;
        for (label, wrong) in [
            (
                "header left out",
                self.send_put_once(&url, &without, body.to_vec()).await?,
            ),
            (
                "other bytes",
                self.send_put_once(&url, &headers, other_bytes).await?,
            ),
            (
                "other checksum",
                self.send_put_once(&url, &other_checksum, body.to_vec())
                    .await?,
            ),
        ] {
            if !wrong.is_client_error() {
                return Err(format!(
                    "upload with {label} must be refused with a 4xx, got HTTP {wrong}"
                ));
            }
            refused += 1;
        }
        let right = self.send_put_once(&url, &headers, body.to_vec()).await?;
        if !right.is_success() {
            return Err(format!("upload with the returned headers: HTTP {right}"));
        }
        self.call::<BlobPut>(&self.blob_put_request(&sha256, size, BlobPutStage::Commit))
            .await?;
        ledger.blob(&self.database_hex, &sha256, size);
        Ok(refused)
    }

    async fn send_put_once(
        &self,
        url: &str,
        headers: &[(String, String)],
        data: Vec<u8>,
    ) -> Result<reqwest::StatusCode, String> {
        let mut req = self.http.put(url).body(data);
        for (name, value) in headers {
            req = req.header(name.as_str(), value.as_str());
        }
        req.send()
            .await
            .map(|resp| resp.status())
            .map_err(|e| format!("blob upload: {e}"))
    }

    /// `blob.put` begin, the presigned upload streamed from `source`, commit.
    pub async fn blob_put_file(
        &self,
        sha256: &str,
        source: &Path,
        ledger: &Ledger,
    ) -> Result<(), String> {
        let file = fs::File::open(source)
            .await
            .map_err(|e| format!("blob file: {e}"))?;
        let size = file
            .metadata()
            .await
            .map_err(|e| format!("blob file: {e}"))?
            .len();
        let begin = self
            .call::<BlobPut>(&self.blob_put_request(sha256, size, BlobPutStage::Begin))
            .await?;
        if begin.upload.is_none() && begin.sha256 == sha256 {
            ledger.blob(&self.database_hex, sha256, size);
            return Ok(());
        }
        let upload = begin.upload.ok_or("blob.put begin has no upload.url")?;
        let mut req = self
            .http
            .put(&upload.url)
            .header("content-length", size)
            .body(file_body(file));
        for (name, value) in upload.headers {
            // The content-length is set above; a second one makes the object store answer 400.
            if name.eq_ignore_ascii_case("content-length") {
                continue;
            }
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
        self.call::<BlobPut>(&self.blob_put_request(sha256, size, BlobPutStage::Commit))
            .await?;
        ledger.blob(&self.database_hex, sha256, size);
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
        let request = BlobGetRequest {
            database_id: self.database_hex.clone(),
            sha256: sha256.to_owned(),
        };
        let meta = self.call::<BlobGet>(&request).await?;
        if meta.bytes != expect_size {
            return Err(format!("blob.get size differs from the entry for {sha256}"));
        }
        let mut req = self.http.get(&meta.url);
        for (name, value) in meta.headers {
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

fn flip_first_base64_char(checksum: &str) -> Result<String, String> {
    let mut chars = checksum.chars();
    let first = chars
        .next()
        .ok_or_else(|| "empty x-amz-checksum-sha256 header".to_owned())?;
    let replacement = if first == 'A' { 'B' } else { 'A' };
    Ok(format!("{replacement}{}", chars.as_str()))
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
