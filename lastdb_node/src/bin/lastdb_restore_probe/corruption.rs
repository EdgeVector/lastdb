use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use fold_db::hex::sha256_hex;
use lastdb_node::offline_home::refuse_primary;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CorruptRestoreEvidence {
    version: u32,
    control_chunks_installed: usize,
    control_value_restored: bool,
    corrupt_chunk_responses: usize,
    expected_sha256: String,
    served_sha256: String,
    digest_refused: bool,
    corrupt_chunk_not_installed: bool,
    restore_error: String,
}

impl CorruptRestoreEvidence {
    pub(crate) fn proven(&self) -> bool {
        let is_sha =
            |value: &str| value.len() == 64 && value.bytes().all(|b| b.is_ascii_hexdigit());
        self.version == 1
            && self.control_chunks_installed > 0
            && self.control_value_restored
            && self.corrupt_chunk_responses == 1
            && is_sha(&self.expected_sha256)
            && is_sha(&self.served_sha256)
            && self.expected_sha256 != self.served_sha256
            && self.digest_refused
            && self.corrupt_chunk_not_installed
            && !self.restore_error.is_empty()
    }
}

pub(crate) fn prove_corrupt_restore(out: &Path) -> Result<(), String> {
    #[cfg(not(feature = "cloud-sync"))]
    {
        let _ = out;
        Err("the corruption proof requires cloud-sync".to_string())
    }
    #[cfg(feature = "cloud-sync")]
    {
        refuse_primary(out)?;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| e.to_string())?;
        let evidence = runtime.block_on(proof::run())?;
        let json = serde_json::to_vec_pretty(&evidence).map_err(|e| e.to_string())?;
        std::fs::write(out, &json).map_err(|e| format!("write {}: {e}", out.display()))?;
        println!("{}", String::from_utf8_lossy(&json));
        if evidence.proven() {
            Ok(())
        } else {
            Err("corruption criteria not met".to_string())
        }
    }
}

/// Offline proof of the public restore boundary. All stores are generated here;
/// no primary files, credentials, or cloud endpoints enter this fixture.
#[cfg(feature = "cloud-sync")]
mod proof {
    use super::*;
    use fold_db::storage::{laststore::BackupManifest, LastStoreNamespacedStore, NamespacedStore};
    use fold_db::sync::{
        auth::{AuthClient, SyncAuth},
        engine::{restore_laststore_cloud_backup, LastStoreCloudRestoreReport},
        error::{SyncError, SyncResult},
        s3::S3Client,
    };
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    const MAX_FIXTURE_BYTES: u64 = 1024 * 1024;
    const KEY: &[u8] = b"atom:restore-corruption-proof";
    const VALUE: &[u8] = b"restore control value";

    struct Scratch(PathBuf);
    impl Drop for Scratch {
        fn drop(&mut self) {
            // Only the exact fresh UUID directory created by this proof.
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn open(root: &Path) -> Result<LastStoreNamespacedStore, String> {
        LastStoreNamespacedStore::open_with_high_water(
            &root.join("data"),
            root.join("high-water.json"),
        )
        .map_err(|e| e.to_string())
    }

    pub(super) async fn run() -> Result<CorruptRestoreEvidence, String> {
        let path =
            std::env::temp_dir().join(format!("lastdb-corrupt-restore-{}", uuid::Uuid::new_v4()));
        refuse_primary(&path)?;
        std::fs::create_dir(&path).map_err(|e| e.to_string())?;
        let scratch = Scratch(path);
        let source = open(&scratch.0.join("source"))?;
        source
            .open_namespace("main")
            .await
            .map_err(|e| e.to_string())?
            .put(KEY, VALUE.to_vec())
            .await
            .map_err(|e| e.to_string())?;
        let manifest = source
            .cut_backup_manifest(None)
            .map_err(|e| e.to_string())?;
        let chunk = manifest
            .atom_chunks
            .first()
            .ok_or("fixture has no atom chunk")?;
        let mut objects = BTreeMap::new();
        let mut total = 0u64;
        for candidate in source
            .enumerate_backup_chunk_candidates(None)
            .map_err(|e| e.to_string())?
        {
            total += std::fs::metadata(&candidate.path)
                .map_err(|e| e.to_string())?
                .len();
            if total > MAX_FIXTURE_BYTES {
                return Err("fixture exceeds 1 MiB".to_string());
            }
            objects.insert(
                candidate.chunk.sha256,
                std::fs::read(candidate.path).map_err(|e| e.to_string())?,
            );
        }
        let control = open(&scratch.0.join("control"))?;
        let (result, _) = attempt(&manifest, &objects, &control, None).await?;
        let control_report = result.map_err(|e| format!("valid control restore failed: {e}"))?;
        let control_value_restored = control
            .open_namespace("main")
            .await
            .map_err(|e| e.to_string())?
            .get(KEY)
            .await
            .map_err(|e| e.to_string())?
            .as_deref()
            == Some(VALUE);

        let corrupt = objects
            .get_mut(&chunk.sha256)
            .ok_or("fixture chunk absent")?;
        let last = corrupt.last_mut().ok_or("fixture chunk empty")?;
        *last ^= 1; // Same length: the download cap cannot stand in for the SHA guard.
        let served_sha256 = sha256_hex(corrupt);
        let target = open(&scratch.0.join("corrupt"))?;
        let (result, corrupt_chunk_responses) =
            attempt(&manifest, &objects, &target, Some(&chunk.sha256)).await?;
        // Demand the download boundary's typed Crypto error. If that guard is
        // removed, the install layer's Storage error must not satisfy the proof.
        let digest_refused = matches!(&result, Err(SyncError::Crypto(message))
            if message.contains("backup chunk sha256 mismatch")
                && message.contains(&chunk.sha256) && message.contains(&served_sha256));
        let restore_error = match result {
            Err(e) => e.to_string(),
            Ok(_) => String::new(),
        };
        let corrupt_chunk_not_installed = !target
            .enumerate_backup_chunk_candidates(None)
            .map_err(|e| e.to_string())?
            .iter()
            .any(|candidate| candidate.chunk.chunk_uuid == chunk.chunk_uuid);
        Ok(CorruptRestoreEvidence {
            version: 1,
            control_chunks_installed: control_report.chunks_installed,
            control_value_restored,
            corrupt_chunk_responses,
            expected_sha256: chunk.sha256.clone(),
            served_sha256,
            digest_refused,
            corrupt_chunk_not_installed,
            restore_error,
        })
    }

    async fn attempt(
        manifest: &BackupManifest,
        objects: &BTreeMap<String, Vec<u8>>,
        target: &LastStoreNamespacedStore,
        corrupt_sha: Option<&str>,
    ) -> Result<(SyncResult<LastStoreCloudRestoreReport>, usize), String> {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(|e| e.to_string())?;
        let base = format!(
            "http://{}",
            listener.local_addr().map_err(|e| e.to_string())?
        );
        let http = Arc::new(
            reqwest::Client::builder()
                .no_proxy()
                .build()
                .map_err(|e| e.to_string())?,
        );
        let auth = AuthClient::new(
            Arc::clone(&http),
            base.clone(),
            SyncAuth::ApiKey("local-proof-only".to_string()),
        )
        .with_request_timeout(Duration::from_secs(3));
        let s3 = S3Client::with_request_timeout(http, Duration::from_secs(3));
        let served = AtomicUsize::new(0);
        // Both futures are scoped. Completion or timeout drops the listener and
        // any accepted connection; no server task survives a failed restore.
        let result = tokio::time::timeout(Duration::from_secs(15), async {
            tokio::select! {
                result = restore_laststore_cloud_backup(&auth, &s3, target) => Ok(result),
                result = serve(listener, &base, manifest, objects, corrupt_sha, &served) =>
                    Err(format!("mock server ended before restore: {result:?}")),
            }
        })
        .await
        .map_err(|_| "local restore exceeded 15 seconds".to_string())??;
        Ok((result, served.load(Ordering::Relaxed)))
    }

    async fn serve(
        listener: TcpListener,
        base: &str,
        manifest: &BackupManifest,
        objects: &BTreeMap<String, Vec<u8>>,
        corrupt_sha: Option<&str>,
        served: &AtomicUsize,
    ) -> Result<(), String> {
        let manifest_bytes = serde_json::to_vec(manifest).map_err(|e| e.to_string())?;
        let manifest_sha = sha256_hex(&manifest_bytes);
        for _ in 0..32 {
            let (mut stream, _) = listener.accept().await.map_err(|e| e.to_string())?;
            let (route, body) = request(&mut stream).await?;
            let response = if route == "/api/sync/presign" {
                let body: Value = serde_json::from_slice(&body).map_err(|e| e.to_string())?;
                let response = match body["action"].as_str() {
                    Some("backup_latest_get") => {
                        serde_json::json!({"ok": true, "key": "local/latest",
                        "latest": {"store_uuid": manifest.store_uuid, "epoch": manifest.epoch,
                        "counter": manifest.counter, "manifest_sha256": manifest_sha, "updated_at_unix_secs": 1}})
                    }
                    Some("presign_backup_manifest_download")
                        if body["manifest_sha256"] == manifest_sha =>
                    {
                        serde_json::json!({"ok": true, "urls": [{"url": format!("{base}/manifest"), "method": "GET", "expires_in_secs": 60}]})
                    }
                    Some("presign_backup_chunk_download") => {
                        let sha = body["chunk_sha256"].as_str().ok_or("absent chunk SHA")?;
                        if !objects.contains_key(sha) {
                            return Err("unknown chunk SHA".to_string());
                        }
                        serde_json::json!({"ok": true, "urls": [{"url": format!("{base}/chunk/{sha}"), "method": "GET", "expires_in_secs": 60}]})
                    }
                    _ => return Err("unexpected local auth request".to_string()),
                };
                serde_json::to_vec(&response).map_err(|e| e.to_string())?
            } else if route == "/manifest" {
                manifest_bytes.clone()
            } else if let Some(sha) = route.strip_prefix("/chunk/") {
                let bytes = objects.get(sha).ok_or("unknown object")?;
                if Some(sha) == corrupt_sha {
                    served.fetch_add(1, Ordering::Relaxed);
                }
                bytes.clone()
            } else {
                return Err("unexpected local object route".to_string());
            };
            let header = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                response.len()
            );
            stream
                .write_all(header.as_bytes())
                .await
                .map_err(|e| e.to_string())?;
            stream
                .write_all(&response)
                .await
                .map_err(|e| e.to_string())?;
            stream.shutdown().await.map_err(|e| e.to_string())?;
        }
        Err("local request budget exhausted".to_string())
    }

    async fn request(stream: &mut TcpStream) -> Result<(String, Vec<u8>), String> {
        let mut bytes = Vec::new();
        loop {
            if bytes.len() >= 16 * 1024 {
                return Err("local request exceeds 16 KiB".to_string());
            }
            let mut buffer = [0u8; 1024];
            let n = stream.read(&mut buffer).await.map_err(|e| e.to_string())?;
            if n == 0 {
                return Err("local request ended early".to_string());
            }
            bytes.extend_from_slice(&buffer[..n]);
            if let Some(end) = bytes.windows(4).position(|b| b == b"\r\n\r\n") {
                let header = std::str::from_utf8(&bytes[..end]).map_err(|e| e.to_string())?;
                let length = header
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length")
                            .then_some(value.trim())
                    })
                    .unwrap_or("0")
                    .parse::<usize>()
                    .map_err(|e| e.to_string())?;
                if length > 8 * 1024 {
                    return Err("local request body exceeds 8 KiB".to_string());
                }
                if bytes.len() >= end + 4 + length {
                    let route = header
                        .lines()
                        .next()
                        .and_then(|line| line.split_whitespace().nth(1))
                        .ok_or("absent local request route")?
                        .to_string();
                    return Ok((route, bytes[end + 4..end + 4 + length].to_vec()));
                }
            }
        }
    }
}
