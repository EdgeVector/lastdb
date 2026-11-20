//! Grant-blind HTTP client for `/relay/v1/log/*` and `/relay/v1/blob/*`.

use app_identity_crypto::{hex_lower, sign, SigningKey};
use base64::Engine as _;
use mutation_log::SignedEntry;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

pub(super) struct RelayClient {
    pub(super) base: String,
    pub(super) signing_key: SigningKey,
    pub(super) node_key_hex: String,
    pub(super) device_id: String,
    http: reqwest::Client,
}

impl RelayClient {
    pub(super) fn new(
        base: String,
        signing_key: SigningKey,
        node_key_hex: String,
        device_id: mutation_log::DeviceId,
    ) -> Self {
        Self {
            base: base.trim_end_matches('/').to_owned(),
            signing_key,
            node_key_hex,
            device_id: device_id.to_string(),
            // trace-egress: public — grant-blind relay; node key only in signed headers.
            http: reqwest::Client::new(),
        }
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
        let value = self.post("/relay/v1/log/append", &body).await?;
        Ok((
            value["stored"].as_u64().unwrap_or(0),
            value["duplicate"].as_u64().unwrap_or(0),
        ))
    }

    pub(super) async fn read(
        &self,
        database_id: &str,
        node_key: &str,
        device_id: &str,
        from_seq: u64,
        limit: u32,
    ) -> Result<(Vec<SignedEntry>, Option<String>), String> {
        // OpenAPI LogReadBody names the stream key `writer_key` (hex node key).
        let body = json!({
            "database_id": database_id,
            "writer_key": node_key,
            "device_id": device_id,
            "from_seq": from_seq,
            "limit": limit,
        });
        let value = self.post("/relay/v1/log/read", &body).await?;
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
        self.post("/relay/v1/blob/put", &body).await
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
        self.post("/relay/v1/blob/put", &body).await?;
        Ok(())
    }

    #[allow(dead_code)]
    pub(super) async fn blob_get(&self, database_id: &str, sha256: &str) -> Result<Value, String> {
        let body = json!({
            "database_id": database_id,
            "sha256": sha256,
        });
        self.post("/relay/v1/blob/get", &body).await
    }

    async fn post(&self, path: &str, body: &Value) -> Result<Value, String> {
        let raw = serde_json::to_vec(body).map_err(|_| "cannot encode relay body")?;
        let headers = sign_headers(
            &self.signing_key,
            &self.node_key_hex,
            &self.device_id,
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
    path: &str,
    body: &[u8],
) -> Result<Vec<(&'static str, String)>, String> {
    let issued_at = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis().to_string())
        .unwrap_or_else(|_| "0".into());
    let nonce = hex_lower(&rand_32());
    let body_hash = hex_lower(&Sha256::digest(body));
    let message =
        format!("POST\n{path}\n1\n{node_key_hex}\n{device_id}\n{issued_at}\n{nonce}\n{body_hash}");
    let sig = sign(key, message.as_bytes());
    let signature = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(sig);
    Ok(vec![
        ("X-LastDB-Relay-Protocol", "1".into()),
        ("X-LastDB-Node-Key", node_key_hex.to_owned()),
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
