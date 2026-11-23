//! Relay client: OpenAPI `nodeSignature` over `log.*` and `blob.*`, plus presigned transfers.

use std::time::Duration;

use app_identity_crypto::{hex_lower, sign, SigningKey};
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use bytes::Bytes;
use mutation_log::{DatabaseId, DeviceId, SignedEntry};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::identity::{nonce_hex, now_ms, Actor};

const PROTOCOL: &str = "1";
const ATTEMPTS: u32 = 4;
const APPEND_BATCH: usize = 90;
/// Largest `log.read` page the relay protocol allows.
pub const READ_LIMIT: u64 = 90;

/// Shared HTTP client for relay calls and presigned transfers.
pub fn http_client() -> Result<reqwest::Client, String> {
    // trace-egress: public — relay origin comes from RELAY_URL; only signed headers, no secrets.
    reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(20))
        .timeout(Duration::from_secs(180))
        .build()
        .map_err(|e| format!("http client: {e}"))
}

/// One line of `log.streams`.
#[derive(Debug, PartialEq, Eq)]
pub struct StreamHead {
    pub device_id: String,
    pub head_seq: u64,
    pub head_hash: String,
}

/// One page of `log.read`.
pub struct LogPage {
    pub entries: Vec<SignedEntry>,
    /// The relay returned a continuation cursor.
    pub more: bool,
}

struct Failure {
    message: String,
    retryable: bool,
}

/// Signed relay client for one `(node_key, device_id)` caller and one database.
#[derive(Clone)]
pub struct RelayClient {
    base: String,
    pub(crate) http: reqwest::Client,
    signing_key: SigningKey,
    node_key_hex: String,
    pub(crate) database_hex: String,
    device_hex: String,
}

impl RelayClient {
    pub fn new(
        base: &str,
        http: &reqwest::Client,
        actor: &Actor,
        database_id: DatabaseId,
        device_id: DeviceId,
    ) -> Self {
        Self {
            base: base.trim_end_matches('/').to_owned(),
            http: http.clone(),
            signing_key: actor.signing_key.clone(),
            node_key_hex: actor.node_key_hex(),
            database_hex: database_id.to_hex(),
            device_hex: device_id.to_hex(),
        }
    }

    /// `log.append`, split into relay-sized batches.
    pub async fn append(&self, entries: &[SignedEntry]) -> Result<(), String> {
        for chunk in entries.chunks(APPEND_BATCH) {
            let body = json!({ "database_id": self.database_hex, "entries": chunk });
            self.post("/relay/v1/log/append", &body).await?;
        }
        Ok(())
    }

    /// `log.read` of one device stream from `from_seq`.
    pub async fn read(
        &self,
        writer_key: &str,
        device_hex: &str,
        from_seq: u64,
    ) -> Result<LogPage, String> {
        let body = json!({
            "database_id": self.database_hex,
            "writer_key": writer_key,
            "device_id": device_hex,
            "from_seq": from_seq,
            "limit": READ_LIMIT,
        });
        let value = self.post("/relay/v1/log/read", &body).await?;
        let entries = serde_json::from_value(value["entries"].clone())
            .map_err(|e| format!("log.read entries: {e}"))?;
        Ok(LogPage {
            entries,
            more: value["cursor"].is_string(),
        })
    }

    /// `log.read` of every device of `writer_key` from seq 1 (no `device_id`).
    pub async fn read_writer(&self, writer_key: &str) -> Result<LogPage, String> {
        let body = json!({
            "database_id": self.database_hex,
            "writer_key": writer_key,
            "limit": READ_LIMIT,
        });
        let value = self.post("/relay/v1/log/read", &body).await?;
        let entries = serde_json::from_value(value["entries"].clone())
            .map_err(|e| format!("log.read entries: {e}"))?;
        Ok(LogPage {
            entries,
            more: value["cursor"].is_string(),
        })
    }

    /// `log.streams`: the device streams of `writer_key`, sorted by device id.
    pub async fn streams(&self, writer_key: &str) -> Result<Vec<StreamHead>, String> {
        let body = json!({ "database_id": self.database_hex, "writer_key": writer_key });
        let value = self.post("/relay/v1/log/streams", &body).await?;
        let list = value["streams"]
            .as_array()
            .ok_or("log.streams has no streams list")?;
        list.iter()
            .map(|item| {
                Ok(StreamHead {
                    device_id: item["device_id"]
                        .as_str()
                        .ok_or("log.streams item has no device_id")?
                        .to_owned(),
                    head_seq: item["head_seq"]
                        .as_u64()
                        .ok_or("log.streams item has no head_seq")?,
                    head_hash: item["head_hash"]
                        .as_str()
                        .ok_or("log.streams item has no head_hash")?
                        .to_owned(),
                })
            })
            .collect()
    }

    /// `blob.put` begin, presigned upload, commit.
    pub async fn blob_put(&self, sha256: &str, body: Bytes) -> Result<(), String> {
        let size = body.len() as u64;
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
        if begin["upload"].is_null() && begin["sha256"].as_str() == Some(sha256) {
            return Ok(());
        }
        let url = begin["upload"]["url"]
            .as_str()
            .ok_or("blob.put begin has no upload.url")?
            .to_owned();
        let headers = header_pairs(&begin["upload"]["headers"]);
        self.upload(&url, &headers, body).await?;
        self.post("/relay/v1/blob/put", &stage_body("commit"))
            .await?;
        Ok(())
    }

    /// `blob.get` then presigned download.
    pub async fn blob_get(&self, sha256: &str, expect_size: u64) -> Result<Bytes, String> {
        let body = json!({ "database_id": self.database_hex, "sha256": sha256 });
        let meta = self.post("/relay/v1/blob/get", &body).await?;
        let url = meta["url"]
            .as_str()
            .ok_or("blob.get has no url")?
            .to_owned();
        if meta["bytes"].as_u64().is_some_and(|n| n != expect_size) {
            return Err(format!(
                "blob.get size differs from the bundle entry for {sha256}"
            ));
        }
        self.download(&url, &header_pairs(&meta["headers"])).await
    }

    pub(crate) async fn post(&self, path: &str, body: &Value) -> Result<Value, String> {
        let raw = serde_json::to_vec(body).map_err(|e| format!("encode body: {e}"))?;
        let mut last = String::new();
        for attempt in 0..ATTEMPTS {
            if attempt > 0 {
                backoff(attempt).await;
            }
            match self.post_once(path, &raw).await {
                Ok(value) => return Ok(value),
                Err(failure) if failure.retryable => last = failure.message,
                Err(failure) => return Err(failure.message),
            }
        }
        Err(last)
    }

    async fn post_once(&self, path: &str, raw: &[u8]) -> Result<Value, Failure> {
        let mut req = self
            .http
            .post(format!("{}{path}", self.base))
            .body(raw.to_vec());
        for (name, value) in self.sign_headers(path, raw) {
            req = req.header(name, value);
        }
        let resp = req.send().await.map_err(|e| Failure {
            message: format!("relay {path}: {e}"),
            retryable: true,
        })?;
        let status = resp.status();
        let text = resp.text().await.map_err(|e| Failure {
            message: format!("relay {path} body: {e}"),
            retryable: true,
        })?;
        if !status.is_success() {
            return Err(Failure {
                message: format!("relay {path} HTTP {status}: {text}"),
                retryable: retry_status(status),
            });
        }
        serde_json::from_str(&text).map_err(|e| Failure {
            message: format!("relay {path} json: {e}"),
            retryable: false,
        })
    }

    /// `POST\n<path>\n1\n<node_key>\n<database_id>\n<body_sha256>\n<issued_at>\n<nonce>`.
    fn sign_headers(&self, path: &str, raw: &[u8]) -> [(&'static str, String); 8] {
        let issued_at = (now_ms() / 1000).to_string();
        let nonce = nonce_hex();
        let body_hash = hex_lower(&Sha256::digest(raw));
        let message = format!(
            "POST\n{path}\n{PROTOCOL}\n{}\n{}\n{body_hash}\n{issued_at}\n{nonce}",
            self.node_key_hex, self.database_hex
        );
        let signature = URL_SAFE_NO_PAD.encode(sign(&self.signing_key, message.as_bytes()));
        [
            ("X-LastDB-Relay-Protocol", PROTOCOL.to_owned()),
            ("X-LastDB-Node-Key", self.node_key_hex.clone()),
            ("X-LastDB-Database-Id", self.database_hex.clone()),
            ("X-LastDB-Device-Id", self.device_hex.clone()),
            ("X-LastDB-Issued-At", issued_at),
            ("X-LastDB-Nonce", nonce),
            ("X-LastDB-Signature", signature),
            ("content-type", "application/json".to_owned()),
        ]
    }

    async fn upload(
        &self,
        url: &str,
        headers: &[(String, String)],
        body: Bytes,
    ) -> Result<(), String> {
        let mut last = String::new();
        for attempt in 0..ATTEMPTS {
            if attempt > 0 {
                backoff(attempt).await;
            }
            let mut req = self.http.put(url).body(body.clone());
            for (name, value) in headers {
                if name.eq_ignore_ascii_case("content-length") {
                    continue;
                }
                req = req.header(name.as_str(), value.as_str());
            }
            match req.send().await {
                Ok(resp) if resp.status().is_success() => return Ok(()),
                Ok(resp) => {
                    let status = resp.status();
                    last = format!("blob upload HTTP {status}");
                    if !retry_status(status) {
                        return Err(last);
                    }
                }
                Err(e) => last = format!("blob upload: {e}"),
            }
        }
        Err(last)
    }

    async fn download(&self, url: &str, headers: &[(String, String)]) -> Result<Bytes, String> {
        let mut last = String::new();
        for attempt in 0..ATTEMPTS {
            if attempt > 0 {
                backoff(attempt).await;
            }
            let mut req = self.http.get(url);
            for (name, value) in headers {
                req = req.header(name.as_str(), value.as_str());
            }
            match req.send().await {
                Ok(resp) if resp.status().is_success() => match resp.bytes().await {
                    Ok(bytes) => return Ok(bytes),
                    Err(e) => last = format!("blob download body: {e}"),
                },
                Ok(resp) => {
                    let status = resp.status();
                    last = format!("blob download HTTP {status}");
                    if !retry_status(status) {
                        return Err(last);
                    }
                }
                Err(e) => last = format!("blob download: {e}"),
            }
        }
        Err(last)
    }
}

fn retry_status(status: reqwest::StatusCode) -> bool {
    status.is_server_error() || status == reqwest::StatusCode::TOO_MANY_REQUESTS
}

async fn backoff(attempt: u32) {
    tokio::time::sleep(Duration::from_millis(250u64 << attempt)).await;
}

pub(crate) fn header_pairs(value: &Value) -> Vec<(String, String)> {
    let Some(map) = value.as_object() else {
        return Vec::new();
    };
    map.iter()
        .filter_map(|(name, val)| val.as_str().map(|v| (name.clone(), v.to_owned())))
        .collect()
}
