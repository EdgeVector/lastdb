//! HTTP client for `/relay/v1/log/*` and `/relay/v1/blob/*`.

use std::time::{Duration, Instant};

use app_identity_crypto::{hex_lower, sign, SigningKey};
use base64::Engine as _;
use mutation_log::{DatabaseId, DeviceId, SignedEntry};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

/// Bound a hung peer so the sync loop can return and re-push the outbox.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
/// CI probe budget: request timeout plus a little slack.
const HUNG_PEER_BUDGET: Duration = Duration::from_secs(45);

pub(super) struct RelayClient {
    pub(super) base: String,
    pub(super) signing_key: SigningKey,
    pub(super) node_key_hex: String,
    pub(super) device_id: String,
    http: reqwest::Client,
}

impl RelayClient {
    pub(super) fn new(
        base: &str,
        signing_key: SigningKey,
        node_key_hex: String,
        device_id: &mutation_log::DeviceId,
    ) -> Result<Self, String> {
        // trace-egress: public — relay gates by grants; node key only in signed headers.
        let http = reqwest::Client::builder()
            .connect_timeout(CONNECT_TIMEOUT)
            .timeout(REQUEST_TIMEOUT)
            .build()
            .map_err(|e| format!("relay http client: {e}"))?;
        Ok(Self {
            base: base.trim_end_matches('/').to_owned(),
            signing_key,
            node_key_hex,
            device_id: device_id.to_string(),
            http,
        })
    }

    pub(super) async fn append(
        &self,
        database_id: &str,
        entries: &[SignedEntry],
    ) -> Result<(u64, u64), String> {
        let body = json!({
            "database_id": database_id,
            "entries": entries,
        });
        let value = self
            .post("/relay/v1/log/append", database_id, &body)
            .await?;
        Ok((
            value["stored"].as_u64().unwrap_or(0),
            value["duplicate"].as_u64().unwrap_or(0),
        ))
    }

    /// Read one writer's entries. Omit `device_id` to receive that writer's
    /// entries across every device; the response cursor carries the device.
    pub(super) async fn read(
        &self,
        database_id: &str,
        writer_key: &str,
        device_id: Option<&str>,
        from_seq: u64,
        cursor: Option<&str>,
        limit: u32,
    ) -> Result<(Vec<SignedEntry>, Option<String>), String> {
        // OpenAPI LogReadBody names the stream key `writer_key` (hex node key).
        let mut body = json!({
            "database_id": database_id,
            "limit": limit,
        });
        if let Some(cursor) = cursor {
            body["cursor"] = Value::String(cursor.to_owned());
        } else {
            body["writer_key"] = Value::String(writer_key.to_owned());
            body["from_seq"] = json!(from_seq);
            if let Some(device_id) = device_id {
                body["device_id"] = Value::String(device_id.to_owned());
            }
        }
        let value = self.post("/relay/v1/log/read", database_id, &body).await?;
        let entries: Vec<SignedEntry> = serde_json::from_value(value["entries"].clone())
            .map_err(|_| "relay log.read returned invalid entries".to_owned())?;
        let cursor = value["cursor"].as_str().map(str::to_owned);
        Ok((entries, cursor))
    }

    #[allow(dead_code)]
    pub(super) async fn blob_put_begin(
        &self,
        database_id: &str,
        sha256: &str,
        bytes: u64,
    ) -> Result<Value, String> {
        let body = json!({
            "database_id": database_id,
            "sha256": sha256,
            "bytes": bytes,
            "stage": "begin",
        });
        self.post("/relay/v1/blob/put", database_id, &body).await
    }

    #[allow(dead_code)]
    pub(super) async fn blob_put_commit(
        &self,
        database_id: &str,
        sha256: &str,
        bytes: u64,
    ) -> Result<(), String> {
        let body = json!({
            "database_id": database_id,
            "sha256": sha256,
            "bytes": bytes,
            "stage": "commit",
        });
        self.post("/relay/v1/blob/put", database_id, &body).await?;
        Ok(())
    }

    #[allow(dead_code)]
    pub(super) async fn blob_get(&self, database_id: &str, sha256: &str) -> Result<Value, String> {
        let body = json!({
            "database_id": database_id,
            "sha256": sha256,
        });
        self.post("/relay/v1/blob/get", database_id, &body).await
    }

    async fn post(&self, path: &str, database_id: &str, body: &Value) -> Result<Value, String> {
        let raw = serde_json::to_vec(body).map_err(|_| "cannot encode relay body")?;
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
        let resp = req
            .send()
            .await
            .map_err(|e| format!("relay request failed: {e}"))?;
        let status = resp.status();
        let text = resp
            .text()
            .await
            .map_err(|e| format!("relay response body: {e}"))?;
        if !status.is_success() {
            return Err(format!("relay {path} HTTP {status}: {text}"));
        }
        serde_json::from_str(&text).map_err(|_| "relay response is not JSON".into())
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
    let issued_at = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or_else(|_| "0".into(), |d| d.as_secs().to_string());
    let nonce = hex_lower(&rand_32());
    let body_hash = hex_lower(&Sha256::digest(body));
    let message = format!(
        "POST\n{path}\n1\n{node_key_hex}\n{database_id}\n{body_hash}\n{issued_at}\n{nonce}"
    );
    let sig = sign(key, message.as_bytes());
    let signature = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(sig);
    Ok(vec![
        ("X-LastDB-Relay-Protocol", "1".into()),
        ("X-LastDB-Node-Key", node_key_hex.to_owned()),
        ("X-LastDB-Database-Id", database_id.to_owned()),
        ("X-LastDB-Device-Id", device_id.to_owned()),
        ("X-LastDB-Issued-At", issued_at),
        ("X-LastDB-Nonce", nonce),
        ("X-LastDB-Signature", signature),
        ("content-type", "application/json".into()),
    ])
}

fn rand_32() -> [u8; 32] {
    use rand::RngCore;
    let mut out = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut out);
    out
}

/// CI: append against a hung `base` must error inside [`HUNG_PEER_BUDGET`].
pub async fn hung_relay_must_timeout(base: &str) -> Result<Duration, String> {
    let signing_key = SigningKey::from_bytes(&[7u8; 32]);
    let node_key_hex =
        mutation_log::Hex32::from_bytes(signing_key.verifying_key().to_bytes()).to_hex();
    let device_id = DeviceId::from_bytes([3u8; 32]);
    let client = RelayClient::new(base, signing_key, node_key_hex, &device_id)?;
    let database_id = DatabaseId::from_bytes([9u8; 32]).to_hex();
    let started = Instant::now();
    match client.append(&database_id, &[]).await {
        Err(_) => {
            let elapsed = started.elapsed();
            if elapsed <= HUNG_PEER_BUDGET {
                Ok(elapsed)
            } else {
                Err(format!(
                    "hung peer error took {:.3}s, over {}s",
                    elapsed.as_secs_f64(),
                    HUNG_PEER_BUDGET.as_secs()
                ))
            }
        }
        Ok(_) => Err("hung peer accepted an empty append".into()),
    }
}
