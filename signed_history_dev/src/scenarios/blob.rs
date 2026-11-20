//! F. ~300MB blob.put + data entry ref; upload and restore/download each <60s.

use std::time::Instant;

use mutation_log::{author_data, data_record, MutationFileRef, StreamKind, VerifiedHistory};

use crate::device::{bootstrap_owner, must_accept, now_ms, stream_head_of, DbBirth, Device};
use crate::relay::{sha256_hex, RelayClient, RelayHandle};

const LIMIT_SECS: f64 = 60.0;

pub async fn run(relay: &RelayHandle, blob_bytes: u64) -> Result<String, String> {
    if blob_bytes == 0 {
        return Err("blob_bytes must be > 0".into());
    }
    let owner = Device::generate();
    let birth = DbBirth::generate(owner.node_key);
    let t0 = now_ms();
    let (mut history, birth_entry) = bootstrap_owner(&owner, &birth, t0)?;
    let client = RelayClient::new(&relay.base_url, &owner);
    let db = birth.database_id.to_hex();
    client.append(&db, &[birth_entry]).await?;

    let payload = build_blob(blob_bytes as usize);
    let digest = sha256_hex(&payload);

    let upload_secs = upload_blob(&client, &db, &digest, &payload).await?;
    if upload_secs >= LIMIT_SECS {
        return Err(format!(
            "upload took {upload_secs:.3}s (≥ {LIMIT_SECS}s limit)"
        ));
    }

    let entry = author_blob_ref(&mut history, &owner, birth.database_id, &digest, t0 + 1)?;
    client.append(&db, &[entry]).await?;

    let download_secs = restore_download(&client, &db, &digest, payload.len()).await?;
    if download_secs >= LIMIT_SECS {
        return Err(format!(
            "restore/download took {download_secs:.3}s (≥ {LIMIT_SECS}s limit)"
        ));
    }

    // Fresh history accepts birth+data (blob bytes fetched lazily by hash).
    let mut restored = VerifiedHistory::new(birth.database_id);
    let remote = client
        .read(&db, &owner.node_key_hex(), &owner.device_id_str(), 1, 90)
        .await?;
    for e in remote {
        must_accept(&mut restored, e)?;
    }
    if restored.records().len() != 1 {
        return Err(format!(
            "expected 1 materialized record after restore, got {}",
            restored.records().len()
        ));
    }

    Ok(format!(
        "blob_bytes={blob_bytes} upload_s={upload_secs:.3} download_s={download_secs:.3}"
    ))
}

fn build_blob(len: usize) -> Vec<u8> {
    // Deterministic pattern (not all-zero) so sha256 is stable and compressible-hostile.
    let mut out = Vec::with_capacity(len);
    let mut x = 0xA5u8;
    for i in 0..len {
        out.push(x.wrapping_add((i % 251) as u8));
        x = x.wrapping_mul(31).wrapping_add(17);
    }
    out
}

async fn upload_blob(
    client: &RelayClient,
    database_id: &str,
    digest: &str,
    payload: &[u8],
) -> Result<f64, String> {
    let start = Instant::now();
    let begin = client
        .blob_put_begin(database_id, digest, payload.len() as u64)
        .await?;
    let url = begin["upload"]["url"]
        .as_str()
        .ok_or_else(|| "blob.put begin missing upload.url".to_owned())?
        .to_owned();
    client.blob_upload(&url, payload).await?;
    client
        .blob_put_commit(database_id, digest, payload.len() as u64)
        .await?;
    Ok(start.elapsed().as_secs_f64())
}

async fn restore_download(
    client: &RelayClient,
    database_id: &str,
    digest: &str,
    expect_len: usize,
) -> Result<f64, String> {
    let start = Instant::now();
    let meta = client.blob_get(database_id, digest).await?;
    let url = meta["url"]
        .as_str()
        .ok_or_else(|| "blob.get missing url".to_owned())?
        .to_owned();
    let bytes = client.blob_download(&url).await?;
    if bytes.len() != expect_len {
        return Err(format!(
            "downloaded {} bytes, expected {expect_len}",
            bytes.len()
        ));
    }
    if sha256_hex(&bytes) != digest {
        return Err("downloaded blob sha256 mismatch".into());
    }
    Ok(start.elapsed().as_secs_f64())
}

fn author_blob_ref(
    history: &mut VerifiedHistory,
    owner: &Device,
    database_id: mutation_log::DatabaseId,
    digest: &str,
    authored_at_ms: u64,
) -> Result<mutation_log::SignedEntry, String> {
    let head = stream_head_of(history, owner);
    let ctx = owner.author_ctx(database_id, head, Vec::new(), Vec::new(), authored_at_ms);
    let mut body = data_record(StreamKind::Direct, "files", "big", b"meta");
    body.file = Some(MutationFileRef {
        file_handle: "blob-perf".into(),
        sha256: mutation_log::Hex32::from_hex(digest).map_err(|e| e.to_string())?,
    });
    let entry = author_data(&ctx, &owner.signing_key, body).map_err(|e| e.to_string())?;
    must_accept(history, entry.clone())?;
    Ok(entry)
}
