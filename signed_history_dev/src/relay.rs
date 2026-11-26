//! HTTP client and optional child-process spawn for reference_relay.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use app_identity_crypto::{hex_lower, sign, SigningKey};
use base64::Engine as _;
use mutation_log::SignedEntry;
use relay_protocol::{
    headers, BlobGet, BlobGetAnswer, BlobGetRequest, BlobPut, BlobPutAnswer, BlobPutRequest,
    BlobPutStage, Endpoint, LogAppend, LogAppendAnswer, LogAppendRequest, LogRead, LogReadRequest,
    ReadItem,
};
use sha2::{Digest, Sha256};

use crate::device::{nonce_hex, now_ms, Device};

/// Running relay: either spawned child or external `RELAY_URL`.
pub struct RelayHandle {
    pub base_url: String,
    child: Option<Child>,
    data_dir: Option<PathBuf>,
}

impl RelayHandle {
    /// Connect to an already-running relay.
    pub fn connect(url: &str) -> Self {
        Self {
            base_url: url.trim_end_matches('/').to_owned(),
            child: None,
            data_dir: None,
        }
    }

    /// Spawn `reference_relay --listen 127.0.0.1:0 --data-dir DIR`.
    pub fn spawn(relay_bin: &Path, data_dir: PathBuf) -> Result<Self, String> {
        std::fs::create_dir_all(&data_dir).map_err(|e| format!("relay data-dir: {e}"))?;
        let mut child = Command::new(relay_bin)
            .arg("--listen")
            .arg("127.0.0.1:0")
            .arg("--data-dir")
            .arg(&data_dir)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| format!("spawn reference_relay: {e}"))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| "reference_relay missing stdout".to_owned())?;
        let addr = read_bound_addr(stdout)?;
        // Listener is bound before the addr line; brief pause until serve starts.
        std::thread::sleep(Duration::from_millis(100));
        Ok(Self {
            base_url: format!("http://{addr}"),
            child: Some(child),
            data_dir: Some(data_dir),
        })
    }
}

impl Drop for RelayHandle {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
        if let Some(dir) = self.data_dir.take() {
            let _ = std::fs::remove_dir_all(dir);
        }
    }
}

fn read_bound_addr(stdout: impl std::io::Read) -> Result<String, String> {
    use std::io::{BufRead, BufReader};
    let mut reader = BufReader::new(stdout);
    let mut line = String::new();
    // Child prints the bound SocketAddr on the first line.
    for _ in 0..50 {
        line.clear();
        match reader.read_line(&mut line) {
            Ok(0) => return Err("reference_relay exited before printing bind address".into()),
            Ok(_) => {
                let trimmed = line.trim();
                if !trimmed.is_empty() {
                    return Ok(trimmed.to_owned());
                }
            }
            Err(e) => return Err(format!("read relay bind address: {e}")),
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    Err("timed out waiting for reference_relay bind address".into())
}

/// Signed HTTP client for one device identity.
pub struct RelayClient {
    base: String,
    signing_key: SigningKey,
    node_key_hex: String,
    device_id: String,
    http: reqwest::Client,
}

impl RelayClient {
    pub fn new(base: &str, device: &Device) -> Self {
        Self {
            base: base.trim_end_matches('/').to_owned(),
            signing_key: device.signing_key.clone(),
            node_key_hex: device.node_key_hex(),
            device_id: device.device_id_str(),
            // trace-egress: public — local reference_relay only; no credentials.
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(300))
                .build()
                .expect("reqwest client"),
        }
    }

    pub async fn append(
        &self,
        database_id: &str,
        entries: &[SignedEntry],
    ) -> Result<LogAppendAnswer, String> {
        let values = entries
            .iter()
            .map(serde_json::to_value)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| format!("encode body: {e}"))?;
        self.call::<LogAppend>(&LogAppendRequest::entries(database_id.to_owned(), values))
            .await
    }

    pub async fn read(
        &self,
        database_id: &str,
        writer_key: &str,
        device_id: &str,
        from_seq: u64,
        limit: u32,
    ) -> Result<Vec<SignedEntry>, String> {
        let request = LogReadRequest {
            database_id: database_id.to_owned(),
            writer_key: Some(writer_key.to_owned()),
            device_id: Some(device_id.to_owned()),
            from_seq: Some(from_seq),
            cursor: None,
            limit: Some(limit),
        };
        let answer = self.call::<LogRead>(&request).await?;
        answer
            .entries
            .into_iter()
            .map(|item| match item {
                ReadItem::Entry(value) => serde_json::from_value::<SignedEntry>(value)
                    .map_err(|e| format!("log.read entries: {e}")),
                ReadItem::Segment { .. } => {
                    Err("log.read entries: a log segment, which this client does not read".into())
                }
            })
            .collect()
    }

    pub async fn blob_put_begin(
        &self,
        database_id: &str,
        sha256: &str,
        bytes: u64,
    ) -> Result<BlobPutAnswer, String> {
        self.call::<BlobPut>(&blob_put(database_id, sha256, bytes, BlobPutStage::Begin))
            .await
    }

    pub async fn blob_put_commit(
        &self,
        database_id: &str,
        sha256: &str,
        bytes: u64,
    ) -> Result<(), String> {
        self.call::<BlobPut>(&blob_put(database_id, sha256, bytes, BlobPutStage::Commit))
            .await?;
        Ok(())
    }

    /// PUT `bytes` to the presigned `url` with the headers `blob.put` begin returned (`headers` is
    /// the begin answer's `upload.headers`): the object store checks the signed ones, among them
    /// `x-amz-checksum-sha256`. The client sets the content length itself, so a returned
    /// `content-length` is skipped.
    pub async fn blob_upload(
        &self,
        url: &str,
        headers: &BTreeMap<String, String>,
        bytes: &[u8],
    ) -> Result<(), String> {
        let mut req = self.http.put(url).body(bytes.to_vec());
        for (name, value) in headers {
            if !name.eq_ignore_ascii_case("content-length") {
                req = req.header(name.as_str(), value.as_str());
            }
        }
        let resp = req.send().await.map_err(|e| format!("blob upload: {e}"))?;
        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(format!("blob upload HTTP {status}: {text}"));
        }
        Ok(())
    }

    pub async fn blob_get(&self, database_id: &str, sha256: &str) -> Result<BlobGetAnswer, String> {
        let request = BlobGetRequest {
            database_id: database_id.to_owned(),
            sha256: sha256.to_owned(),
        };
        self.call::<BlobGet>(&request).await
    }

    pub async fn blob_download(&self, url: &str) -> Result<Vec<u8>, String> {
        let resp = self
            .http
            .get(url)
            .send()
            .await
            .map_err(|e| format!("blob download: {e}"))?;
        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(format!("blob download HTTP {status}: {text}"));
        }
        resp.bytes()
            .await
            .map(|b| b.to_vec())
            .map_err(|e| format!("blob download body: {e}"))
    }

    /// Send `request` to the route of `E` and parse its 200 answer.
    async fn call<E: Endpoint>(&self, request: &E::Request) -> Result<E::Answer, String> {
        let path = E::ROUTE.path();
        let database_id = E::database_id(request);
        let raw = serde_json::to_vec(request).map_err(|e| format!("encode body: {e}"))?;
        let headers = sign_headers(
            &self.signing_key,
            &self.node_key_hex,
            &self.device_id,
            database_id,
            path,
            &raw,
        )?;
        let url = format!("{}{path}", self.base);
        let mut req = self.http.post(url).body(raw);
        for (name, value) in headers {
            req = req.header(name, value);
        }
        let resp = req.send().await.map_err(|e| format!("relay {path}: {e}"))?;
        let status = resp.status();
        let text = resp
            .text()
            .await
            .map_err(|e| format!("relay {path} body: {e}"))?;
        if !status.is_success() {
            return Err(format!("relay {path} HTTP {status}: {text}"));
        }
        serde_json::from_str(&text).map_err(|e| format!("relay {path} json: {e}"))
    }
}

fn sign_headers(
    key: &SigningKey,
    node_key_hex: &str,
    device_id: &str,
    database_id: &str,
    path: &str,
    body: &[u8],
) -> Result<Vec<(&'static str, String)>, String> {
    let issued_at = (now_ms() / 1000).to_string();
    let nonce = nonce_hex();
    let body_hash = hex_lower(&Sha256::digest(body));
    let message = relay_protocol::signed_message(
        path,
        node_key_hex,
        database_id,
        &body_hash,
        &issued_at,
        &nonce,
    );
    let sig = sign(key, message.as_bytes());
    let signature = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(sig);
    Ok(vec![
        (headers::PROTOCOL, relay_protocol::PROTOCOL.into()),
        (headers::NODE_KEY, node_key_hex.to_owned()),
        (headers::DATABASE_ID, database_id.to_owned()),
        (headers::DEVICE_ID, device_id.to_owned()),
        (headers::ISSUED_AT, issued_at),
        (headers::NONCE, nonce),
        (headers::SIGNATURE, signature),
        ("content-type", "application/json".into()),
    ])
}

/// A `blob.put` request of one stage.
fn blob_put(database_id: &str, sha256: &str, bytes: u64, stage: BlobPutStage) -> BlobPutRequest {
    BlobPutRequest {
        database_id: database_id.to_owned(),
        sha256: sha256.to_owned(),
        bytes,
        stage,
    }
}

/// SHA-256 of bytes as lowercase hex.
pub fn sha256_hex(bytes: &[u8]) -> String {
    hex_lower(&Sha256::digest(bytes))
}
