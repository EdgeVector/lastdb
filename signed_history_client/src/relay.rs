//! HTTP client for `/relay/v1/log/*`.

use app_identity_crypto::{hex_lower, sign, SigningKey};
use base64::Engine as _;
use mutation_log::SignedEntry;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

/// Signed HTTP client for one node/device identity.
pub struct RelayClient {
    base: String,
    signing_key: SigningKey,
    node_key_hex: String,
    device_id: String,
    http: reqwest::Client,
}

impl RelayClient {
    /// Build a client. `base` is the relay origin (no trailing slash required).
    #[must_use]
    pub fn new(
        base: impl Into<String>,
        signing_key: SigningKey,
        node_key_hex: impl Into<String>,
        device_id: impl Into<String>,
    ) -> Self {
        Self {
            base: base.into().trim_end_matches('/').to_owned(),
            signing_key,
            node_key_hex: node_key_hex.into(),
            device_id: device_id.into(),
            // trace-egress: public — relay gates by grants; node key only in signed headers.
            http: reqwest::Client::new(),
        }
    }

    /// Append signed entries to the relay log.
    pub async fn append(
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

    /// Read one device stream page. `writer_key` is the hex node key.
    pub async fn read(
        &self,
        database_id: &str,
        writer_key: &str,
        device_id: &str,
        from_seq: u64,
        limit: u32,
    ) -> Result<(Vec<SignedEntry>, Option<String>), String> {
        let body = json!({
            "database_id": database_id,
            "writer_key": writer_key,
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
        .map_or_else(|_| "0".into(), |d| d.as_millis().to_string());
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
