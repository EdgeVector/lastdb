//! HTTP client for `/relay/v1/log/*`.

use std::time::{Duration, Instant};

use app_identity_crypto::{hex_lower, sign, SigningKey};
use base64::Engine as _;
use mutation_log::{DatabaseId, DeviceId, SignedEntry};
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

/// Bound a hung peer so the sync loop can return and re-push the log.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
/// A blob transfer fails when it makes no progress for this long, however slow the link.
pub(super) const STALL_TIMEOUT: Duration = Duration::from_secs(30);
/// CI probe budget: request timeout plus a little slack.
const HUNG_PEER_BUDGET: Duration = Duration::from_secs(45);
/// A relay call that the relay marks as transient is sent again at most this many times. The waits
/// before the retries are 250, 500 and 1000 ms, each cut to a random value between half and all
/// of it, so one call adds at most 1.75 s of waiting. That is long enough for a colliding
/// transaction to clear and short enough that a restore does not hang, and the waits stay small
/// while a relay that keeps refusing is not pressed: a call that is still refused fails.
const RETRIES: u32 = 3;
const RETRY_BASE: Duration = Duration::from_millis(250);

/// Starts the error of a read of entries that the relay dropped after a snapshot.
pub(super) const BEHIND_THE_TRIM: &str = "this device is behind history that the relay dropped after a snapshot; restore the database into a new home with `lastdb backup restore`";

/// One device stream of a writer as `log.streams` lists it.
#[derive(Debug, Deserialize)]
pub(super) struct StreamHead {
    pub(super) device_id: String,
    pub(super) head_seq: u64,
    pub(super) head_hash: String,
}

pub(super) struct RelayClient {
    pub(super) base: String,
    pub(super) signing_key: SigningKey,
    pub(super) node_key_hex: String,
    pub(super) device_id: String,
    pub(super) http: reqwest::Client,
    /// For the presigned upload of a blob. No total timeout and no read timeout: reqwest arms its
    /// read timeout when the request starts and keeps it until the response headers arrive, which
    /// for a PUT is after the whole body is sent, so it would cut off every upload longer than
    /// the timeout however fast it moves. The upload watches its own body instead (see
    /// `blob_transfer`). What still applies: connect timeout 10 s; no idle pool (each upload opens its
    /// own connection); TCP keepalive 15 s (keeps a quiet connection open, ends nothing);
    /// no HTTP/2 keep-alive ping and no other timeout.
    pub(super) transfer: reqwest::Client,
    /// For the presigned download of a blob: the same, with [`STALL_TIMEOUT`] for each read of the
    /// answer. The body of a download is the answer, so the timeout is a stall timeout there.
    pub(super) download: reqwest::Client,
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
        // trace-egress: public — presigned blob URLs; the signed headers go only to the relay.
        let transfer = reqwest::Client::builder()
            .connect_timeout(CONNECT_TIMEOUT)
            // A fresh connection for each upload: a body cannot be sent again on a pooled
            // connection that the store closed while it was idle ("connection closed before
            // message completed").
            .pool_max_idle_per_host(0)
            .build()
            .map_err(|e| format!("relay http client: {e}"))?;
        // trace-egress: public — presigned blob URLs; the signed headers go only to the relay.
        let download = reqwest::Client::builder()
            .connect_timeout(CONNECT_TIMEOUT)
            .read_timeout(STALL_TIMEOUT)
            .build()
            .map_err(|e| format!("relay http client: {e}"))?;
        Ok(Self {
            base: base.trim_end_matches('/').to_owned(),
            signing_key,
            node_key_hex,
            device_id: device_id.to_string(),
            http,
            transfer,
            download,
        })
    }

    /// A client for the relay at `base` with this node's keys and device id.
    pub(super) fn for_host(host: &crate::host::Host, base: &str) -> Result<Self, String> {
        let signing_key = SigningKey::from_bytes(&host.keypair.secret_key_bytes());
        let node_key_hex =
            mutation_log::NodeKey::from_bytes(host.keypair.public_key_bytes()).to_hex();
        let device_id = super::device::device_id(&host.home)?;
        Self::new(base, signing_key, node_key_hex, &device_id)
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
        let value = self
            .post("/relay/v1/log/read", database_id, &body)
            .await
            .map_err(|error| {
                if error.contains("HTTP 410") {
                    format!("{BEHIND_THE_TRIM}: {error}")
                } else {
                    error
                }
            })?;
        let entries: Vec<SignedEntry> = serde_json::from_value(value["entries"].clone())
            .map_err(|_| "relay log.read returned invalid entries".to_owned())?;
        let cursor = value["cursor"].as_str().map(str::to_owned);
        Ok((entries, cursor))
    }

    /// Ask the relay to drop what the snapshot entry `writer_seq` of the snapshot stream
    /// `device_id` covers, and the snapshot entries and files older than it. The relay reads
    /// that entry itself. `true` when it has nothing more to drop, `false` when it stopped
    /// at its work limit for one call and a repeat continues.
    pub(super) async fn trim(
        &self,
        database_id: &str,
        device_id: &str,
        writer_seq: u64,
    ) -> Result<bool, String> {
        let body = json!({
            "database_id": database_id,
            "device_id": device_id,
            "writer_seq": writer_seq,
        });
        let value = self.post("/relay/v1/log/trim", database_id, &body).await?;
        Ok(value["done"].as_bool().unwrap_or(false))
    }

    /// The device streams of `writer_key` and how far each reaches: for each,
    /// the contiguous head (the highest `writer_seq` such that every seq from 1
    /// up to it is on the relay) and the hash of that entry. A writer with no
    /// stream gives an empty list. Access is the access of an all-devices
    /// `log.read` of the same writer.
    pub(super) async fn streams(
        &self,
        database_id: &str,
        writer_key: &str,
    ) -> Result<Vec<StreamHead>, String> {
        let body = json!({ "database_id": database_id, "writer_key": writer_key });
        let value = self
            .post("/relay/v1/log/streams", database_id, &body)
            .await?;
        serde_json::from_value(value["streams"].clone())
            .map_err(|_| "relay log.streams returned an invalid list".to_owned())
    }

    pub(super) async fn post(
        &self,
        path: &str,
        database_id: &str,
        body: &Value,
    ) -> Result<Value, String> {
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
            let resp = req
                .send()
                .await
                .map_err(|e| format!("relay request failed: {e}"))?;
            let status = resp.status();
            let text = resp
                .text()
                .await
                .map_err(|e| format!("relay response body: {e}"))?;
            if status.is_success() {
                return serde_json::from_str(&text)
                    .map_err(|_| "relay response is not JSON".into());
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

/// The name and value of each string in a JSON object of headers.
pub(super) fn header_pairs(value: &Value) -> Vec<(String, String)> {
    let Some(map) = value.as_object() else {
        return Vec::new();
    };
    map.iter()
        .filter_map(|(name, val)| val.as_str().map(|v| (name.clone(), v.to_owned())))
        .collect()
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
