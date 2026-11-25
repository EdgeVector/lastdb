//! Request signing, bounded response reading, and retry classification for [`super::RelayClient`].

use std::time::{Duration, Instant};

use app_identity_crypto::{hex_lower, sign, SigningKey};
use base64::Engine as _;
use serde_json::Value;
use sha2::{Digest, Sha256};

use super::RelayClient;

/// Bounded retries with 250/500/1000 ms jittered waits (at most 1.75 s total).
const RETRIES: u32 = 3;
const RETRY_BASE: Duration = Duration::from_millis(250);
const FAST_TRANSPORT_LIMIT: Duration = Duration::from_secs(5);
/// Maximum JSON response payload read from `/relay/v1/*` routes (8 MiB above the 6 MiB wire cap).
const MAX_RESPONSE_BYTES: usize = 8 * 1024 * 1024;

impl RelayClient {
    pub(in crate::log_sync) async fn post_tracked(
        &self,
        path: &str,
        database_id: &str,
        body: &Value,
    ) -> Result<(Value, bool), String> {
        let raw = serde_json::to_vec(body).map_err(|_| "cannot encode relay body")?;
        let url = format!("{}{path}", self.base);
        let mut retried = 0;
        loop {
            // Every try is signed again: the relay consumes the nonce of a request it refuses,
            // and it rejects a replayed nonce and a request issued too long ago.
            let headers = sign_headers(
                &self.signing_key,
                &self.node_key_hex,
                &self.device_id,
                database_id,
                path,
                &raw,
            )?;
            let mut req = self.http.post(&url).body(raw.clone());
            for (name, value) in headers {
                req = req.header(name, value);
            }
            let started = Instant::now();
            let resp = match req.send().await {
                Ok(resp) => resp,
                Err(e) if retried < RETRIES && retryable_transport(&e, started.elapsed()) => {
                    tokio::time::sleep(retry_wait(retried)).await;
                    retried += 1;
                    continue;
                }
                Err(e) => {
                    return Err(format!(
                        "relay request failed: {}",
                        crate::log_sync::blob_transfer::describe(e, &url)
                    ));
                }
            };
            let status = resp.status();
            let text = match read_bounded_text(resp, path).await {
                Ok(Ok(text)) => text,
                Ok(Err(limit_err)) => return Err(limit_err),
                Err(e) if retried < RETRIES && retryable_transport(&e, started.elapsed()) => {
                    tokio::time::sleep(retry_wait(retried)).await;
                    retried += 1;
                    continue;
                }
                Err(e) => {
                    return Err(format!(
                        "relay response body: {}",
                        crate::log_sync::blob_transfer::describe(e, &url)
                    ));
                }
            };
            if status.is_success() {
                let parsed = serde_json::from_str(&text)
                    .map_err(|_| "relay response is not JSON".to_owned())?;
                return Ok((parsed, retried > 0));
            }
            if retried < RETRIES && is_transient(status.as_u16(), &text) {
                tokio::time::sleep(retry_wait(retried)).await;
                retried += 1;
                continue;
            }
            return Err(format!("relay {path} HTTP {status}: {text}"));
        }
    }
}

async fn read_bounded_text(
    mut resp: reqwest::Response,
    path: &str,
) -> Result<Result<String, String>, reqwest::Error> {
    let mut buf = Vec::new();
    while let Some(chunk) = resp.chunk().await? {
        if buf.len().saturating_add(chunk.len()) > MAX_RESPONSE_BYTES {
            return Ok(Err(format!(
                "relay {path} response exceeded {MAX_RESPONSE_BYTES} bytes"
            )));
        }
        buf.extend_from_slice(&chunk);
    }
    Ok(String::from_utf8(buf).map_err(|_| "relay response is not UTF-8".into()))
}

fn retryable_transport(error: &reqwest::Error, elapsed: Duration) -> bool {
    !error.is_timeout()
        && elapsed < FAST_TRANSPORT_LIMIT
        && (error.is_connect() || error.is_request() || error.is_body())
}

/// Whether the relay says that the same request can succeed a moment later: a gateway failure
/// (502, 503, 504), `unavailable` (500), and `rate_limited` (429) with a field that names a
/// provider or a read that failed or collided. A quota or payment refusal is a 429 too, and
/// any other field, and it is final.
fn is_transient(status: u16, body: &str) -> bool {
    if matches!(status, 502..=504) {
        return true;
    }
    let reply: Value = serde_json::from_str(body).unwrap_or(Value::Null);
    let (code, field) = (
        reply["code"].as_str(),
        reply["field"].as_str().unwrap_or(""),
    );
    match (status, code) {
        (500, Some("unavailable")) => true,
        (429, Some("rate_limited")) => {
            !field.ends_with(".configuration")
                && (field.starts_with("provider.")
                    || field.starts_with("log.streams.")
                    || matches!(
                        field,
                        "mutation_log.read" | "quota.read" | "payment.read" | "payment.clock"
                    ))
        }
        _ => false,
    }
}

/// The wait before retry number `retried` (0 first): 250 ms, doubled each time, cut to between
/// half and all of it.
fn retry_wait(retried: u32) -> Duration {
    use rand::Rng;
    let full = RETRY_BASE * 2u32.pow(retried);
    let low = full / 2;
    rand::thread_rng().gen_range(low..=full)
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
