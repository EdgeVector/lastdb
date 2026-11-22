//! Large-file proof of `blob.put` and `blob.get`. Opt-in
//! (`LASTDB_RELAY_E2E_BLOB_FILE_BYTES`, bytes): it generates a file of that size
//! in the work directory, puts it through the relay from disk, gets it back to
//! disk, and compares the SHA-256 of the file that went up, the file that came
//! back, and the digest the relay was told. Neither side holds the file in
//! memory; the CI job reads the relay's peak memory from `/proc` afterwards.

use std::io::Write;
use std::path::Path;
use std::time::Instant;

use app_identity_crypto::hex_lower;
use rand::RngCore;
use sha2::{Digest, Sha256};

use crate::chain::{Chain, Publisher};
use crate::identity::{new_device_id, Actor, DbBirth};
use crate::relay::RelayClient;

const GENERATE_CHUNK: usize = 1024 * 1024;

/// What the step measured.
pub struct BlobStreamReport {
    pub bytes: u64,
    pub sha256: String,
    pub put_seconds: f64,
    pub get_seconds: f64,
}

pub async fn prove(
    http: &reqwest::Client,
    relay_url: &str,
    work: &Path,
    bytes: u64,
) -> Result<BlobStreamReport, String> {
    let owner = Actor::generate();
    let birth = DbBirth::generate(owner.node_key);
    let publisher = Publisher {
        actor: owner,
        database_id: birth.database_id,
    };
    let device = new_device_id();
    let client = RelayClient::new(relay_url, http, &publisher.actor, birth.database_id, device);
    let mut chain = Chain::new(device);
    let opening = publisher.birth(&mut chain, &birth)?;
    client.append(std::slice::from_ref(&opening)).await?;

    let source = work.join("blob-source");
    let dest = work.join("blob-download");
    let (src, size) = (source.clone(), bytes);
    let sha256 = tokio::task::spawn_blocking(move || generate(&src, size))
        .await
        .map_err(|e| format!("generate worker: {e}"))??;

    let started = Instant::now();
    client.blob_put_file(&sha256, &source).await?;
    let put_seconds = started.elapsed().as_secs_f64();
    let started = Instant::now();
    let got = client.blob_get_file(&sha256, bytes, &dest).await?;
    let get_seconds = started.elapsed().as_secs_f64();

    let downloaded = std::fs::metadata(&dest)
        .map_err(|e| format!("downloaded file: {e}"))?
        .len();
    let _ = std::fs::remove_file(&source);
    let _ = std::fs::remove_file(&dest);
    if downloaded != bytes {
        return Err(format!("downloaded {downloaded} bytes, uploaded {bytes}"));
    }
    if got != sha256 {
        return Err(format!(
            "downloaded digest {got} differs from uploaded {sha256}"
        ));
    }
    Ok(BlobStreamReport {
        bytes,
        sha256,
        put_seconds,
        get_seconds,
    })
}

/// Write `bytes` random bytes to `path`; return their SHA-256 as lowercase hex.
fn generate(path: &Path, bytes: u64) -> Result<String, String> {
    let mut file = std::io::BufWriter::new(
        std::fs::File::create(path).map_err(|e| format!("blob source: {e}"))?,
    );
    let mut hasher = Sha256::new();
    let mut rng = rand::thread_rng();
    let mut buf = vec![0u8; GENERATE_CHUNK];
    let mut left = bytes;
    while left > 0 {
        let n = usize::try_from(left).map_or(GENERATE_CHUNK, |l| l.min(GENERATE_CHUNK));
        rng.fill_bytes(&mut buf[..n]);
        hasher.update(&buf[..n]);
        file.write_all(&buf[..n])
            .map_err(|e| format!("blob source: {e}"))?;
        left -= n as u64;
    }
    file.flush().map_err(|e| format!("blob source: {e}"))?;
    Ok(hex_lower(&hasher.finalize()))
}
