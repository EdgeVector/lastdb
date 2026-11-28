//! Relay client: OpenAPI `nodeSignature` over `log.*` and `blob.*`. The presigned transfers go
//! through [`relay_transfer`], the node's own uploader, so this proof measures what users run.

use std::sync::Arc;
use std::time::{Duration, Instant};

use app_identity_crypto::{hex_lower, sign, SigningKey};
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use bytes::Bytes;
use mutation_log::{DatabaseId, DeviceId, SignedEntry};
use relay_protocol::{
    headers, BlobGet, BlobGetRequest, BlobPut, BlobPutRequest, BlobPutStage, Endpoint, LogAppend,
    LogAppendRequest, LogRead, LogReadRequest, LogStreams, LogStreamsRequest, ReadItem,
};
use sha2::{Digest, Sha256};

use crate::identity::{nonce_hex, now_ms, Actor};
pub use crate::transfer::BlobPutTiming;

const ATTEMPTS: u32 = 4;
const APPEND_BATCH: usize = 90;
/// Largest `log.read` page the relay protocol allows.
pub const READ_LIMIT: u32 = 90;

/// Shared HTTP client for relay calls. Presigned transfers use [`relay_transfer`] clients.
pub fn http_client() -> Result<reqwest::Client, String> {
    // trace-egress: public — relay origin comes from RELAY_URL; only signed headers, no secrets.
    reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(20))
        .timeout(Duration::from_secs(180))
        .build()
        .map_err(|e| format!("http client: {e}"))
}

// One line of `log.streams`.
pub use relay_protocol::StreamHead;

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
            let values = chunk
                .iter()
                .map(serde_json::to_value)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| format!("encode body: {e}"))?;
            let request = LogAppendRequest::entries(self.database_hex.clone(), values);
            self.call::<LogAppend>(&request).await?;
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
        self.read_page(LogReadRequest {
            database_id: self.database_hex.clone(),
            writer_key: Some(writer_key.to_owned()),
            device_id: Some(device_hex.to_owned()),
            from_seq: Some(from_seq),
            cursor: None,
            limit: Some(READ_LIMIT),
        })
        .await
    }

    /// `log.read` of every device of `writer_key` from seq 1 (no `device_id`).
    pub async fn read_writer(&self, writer_key: &str) -> Result<LogPage, String> {
        self.read_page(LogReadRequest {
            database_id: self.database_hex.clone(),
            writer_key: Some(writer_key.to_owned()),
            limit: Some(READ_LIMIT),
            ..LogReadRequest::default()
        })
        .await
    }

    /// One `log.read` page of inline entries. This client never appends a log segment, so a
    /// segment in the page is an error.
    async fn read_page(&self, request: LogReadRequest) -> Result<LogPage, String> {
        let answer = self.call::<LogRead>(&request).await?;
        let entries = answer
            .entries
            .into_iter()
            .map(|item| match item {
                ReadItem::Entry(value) => serde_json::from_value::<SignedEntry>(value)
                    .map_err(|e| format!("log.read entries: {e}")),
                ReadItem::Segment { .. } => {
                    Err("log.read entries: a log segment, which this client does not read".into())
                }
            })
            .collect::<Result<Vec<_>, String>>()?;
        Ok(LogPage {
            entries,
            more: answer.cursor.is_some(),
        })
    }

    /// `log.streams`: the device streams of `writer_key`, sorted by device id.
    pub async fn streams(&self, writer_key: &str) -> Result<Vec<StreamHead>, String> {
        let request = LogStreamsRequest {
            database_id: self.database_hex.clone(),
            writer_key: writer_key.to_owned(),
        };
        Ok(self.call::<LogStreams>(&request).await?.streams)
    }

    /// A `blob.put` request of one stage for this client's database.
    pub(crate) fn blob_put_request(
        &self,
        sha256: &str,
        bytes: u64,
        stage: BlobPutStage,
    ) -> BlobPutRequest {
        BlobPutRequest {
            database_id: self.database_hex.clone(),
            sha256: sha256.to_owned(),
            bytes,
            stage,
        }
    }

    /// `blob.put` begin, presigned upload, commit.
    pub async fn blob_put(&self, sha256: &str, body: Bytes) -> Result<(), String> {
        self.blob_put_timed(sha256, Arc::from(body.as_ref()))
            .await
            .map(|_| ())
    }

    /// [`Self::blob_put`] with the time of each stage and every try of the upload.
    pub async fn blob_put_timed(
        &self,
        sha256: &str,
        body: Arc<[u8]>,
    ) -> Result<BlobPutTiming, String> {
        let size = body.len() as u64;
        let started = Instant::now();
        let begin = self
            .call::<BlobPut>(&self.blob_put_request(sha256, size, BlobPutStage::Begin))
            .await?;
        let begin_seconds = started.elapsed().as_secs_f64();
        if begin.upload.is_none() && begin.sha256 == sha256 {
            return Ok(BlobPutTiming {
                begin_seconds,
                ..BlobPutTiming::default()
            });
        }
        let upload = begin.upload.ok_or("blob.put begin has no upload.url")?;
        let put_started = Instant::now();
        let attempts = self
            .upload(&upload.url, &upload.headers, body, size)
            .await?;
        let put_seconds = put_started.elapsed().as_secs_f64();
        let commit_started = Instant::now();
        self.call::<BlobPut>(&self.blob_put_request(sha256, size, BlobPutStage::Commit))
            .await?;
        Ok(BlobPutTiming {
            begin_seconds,
            put_seconds,
            commit_seconds: commit_started.elapsed().as_secs_f64(),
            attempts,
        })
    }

    /// `blob.get` then presigned download.
    pub async fn blob_get(&self, sha256: &str, expect_size: u64) -> Result<Bytes, String> {
        let request = BlobGetRequest {
            database_id: self.database_hex.clone(),
            sha256: sha256.to_owned(),
        };
        let meta = self.call::<BlobGet>(&request).await?;
        if meta.bytes != expect_size {
            return Err(format!(
                "blob.get size differs from the bundle entry for {sha256}"
            ));
        }
        self.download(&meta.url, &meta.headers).await
    }

    /// Send `request` to the route of `E` and parse its 200 answer, retrying a transport failure,
    /// a 5xx and a 429.
    pub(crate) async fn call<E: Endpoint>(
        &self,
        request: &E::Request,
    ) -> Result<E::Answer, String> {
        let path = E::ROUTE.path();
        let raw = serde_json::to_vec(request).map_err(|e| format!("encode body: {e}"))?;
        let mut last = String::new();
        for attempt in 0..ATTEMPTS {
            if attempt > 0 {
                backoff(attempt).await;
            }
            match self.post_once(path, &raw).await {
                Ok(text) => {
                    return serde_json::from_str(&text)
                        .map_err(|e| format!("relay {path} json: {e}"));
                }
                Err(failure) if failure.retryable => {
                    eprintln!("relay {path} try {}: {}", attempt + 1, failure.message);
                    last = failure.message;
                }
                Err(failure) => return Err(failure.message),
            }
        }
        Err(last)
    }

    /// One signed POST: the text of a 200 answer.
    async fn post_once(&self, path: &str, raw: &[u8]) -> Result<String, Failure> {
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
        Ok(text)
    }

    /// [`relay_protocol::signed_message`] signed by this client's node key.
    fn sign_headers(&self, path: &str, raw: &[u8]) -> [(&'static str, String); 8] {
        let issued_at = (now_ms() / 1000).to_string();
        let nonce = nonce_hex();
        let body_hash = hex_lower(&Sha256::digest(raw));
        let message = relay_protocol::signed_message(
            path,
            &self.node_key_hex,
            &self.database_hex,
            &body_hash,
            &issued_at,
            &nonce,
        );
        let signature = URL_SAFE_NO_PAD.encode(sign(&self.signing_key, message.as_bytes()));
        [
            (headers::PROTOCOL, relay_protocol::PROTOCOL.to_owned()),
            (headers::NODE_KEY, self.node_key_hex.clone()),
            (headers::DATABASE_ID, self.database_hex.clone()),
            (headers::DEVICE_ID, self.device_hex.clone()),
            (headers::ISSUED_AT, issued_at),
            (headers::NONCE, nonce),
            (headers::SIGNATURE, signature),
            ("content-type", "application/json".to_owned()),
        ]
    }
}

fn retry_status(status: reqwest::StatusCode) -> bool {
    status.is_server_error() || status == reqwest::StatusCode::TOO_MANY_REQUESTS
}

async fn backoff(attempt: u32) {
    tokio::time::sleep(Duration::from_millis(250u64 << attempt)).await;
}
