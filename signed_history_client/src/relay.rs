//! HTTP client for `/relay/v1/log/*`, speaking the types of [`relay_protocol`].

use std::time::{Duration, Instant};

use app_identity_crypto::{hex_lower, sign, SigningKey};
use base64::Engine as _;
use mutation_log::SignedEntry;
use relay_protocol::{
    headers, Endpoint, LogAppend, LogAppendAnswer, LogAppendRequest, LogRead, LogReadRequest,
    ReadItem,
};
use sha2::{Digest, Sha256};

use crate::retry::{async_sleep, is_transient, retry_wait, retryable_transport, RETRIES};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(60);
const APPEND_MAX_ENTRIES: usize = 90;
const APPEND_MAX_BYTES: usize = 3 * 1024 * 1024;
const MAX_RESPONSE_BYTES: usize = 8 * 1024 * 1024;

/// Signed HTTP client for one node/device identity.
pub struct RelayClient {
    base: String,
    signing_key: SigningKey,
    node_key_hex: String,
    device_id: String,
    http: reqwest::Client,
}

enum BodyRead {
    Ok(String),
    Retryable,
    Fatal(String),
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
        // trace-egress: public — relay gates by grants; node key only in signed headers.
        let http = reqwest::Client::builder()
            .connect_timeout(CONNECT_TIMEOUT)
            .timeout(REQUEST_TIMEOUT)
            .redirect(reqwest::redirect::Policy::none())
            .pool_max_idle_per_host(0)
            .build()
            .expect("relay http client");
        Self {
            base: base.into().trim_end_matches('/').to_owned(),
            signing_key,
            node_key_hex: node_key_hex.into(),
            device_id: device_id.into(),
            http,
        }
    }

    /// Append signed entries to the relay log, chunked to `<= 90` entries and `<= 3 MiB` JSON.
    pub async fn append(
        &self,
        database_id: &str,
        entries: &[SignedEntry],
    ) -> Result<(u64, u64), String> {
        if entries.is_empty() {
            return Ok((0, 0));
        }
        let mut total_stored = 0u64;
        let mut total_dup = 0u64;
        let mut offset = 0usize;
        while offset < entries.len() {
            let take = next_append_chunk_len(database_id, &entries[offset..])?;
            let chunk = &entries[offset..offset + take];
            let values = chunk
                .iter()
                .map(serde_json::to_value)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|_| "cannot encode signed entry".to_owned())?;
            let request = LogAppendRequest::entries(database_id.to_owned(), values);
            let answer = self.call::<LogAppend>(&request).await?;
            let (stored, dup) = append_counts(answer, chunk.len() as u64);
            total_stored = total_stored.saturating_add(stored);
            total_dup = total_dup.saturating_add(dup);
            offset += take;
        }
        Ok((total_stored, total_dup))
    }

    /// Read one device stream page. When `cursor` is `Some`, resumes from that cursor.
    pub async fn read(
        &self,
        database_id: &str,
        writer_key: &str,
        device_id: &str,
        from_seq: u64,
        cursor: Option<&str>,
        limit: u32,
    ) -> Result<(Vec<SignedEntry>, Option<String>), String> {
        let request = match cursor {
            Some(cursor) => LogReadRequest {
                database_id: database_id.to_owned(),
                cursor: Some(cursor.to_owned()),
                limit: Some(limit),
                ..LogReadRequest::default()
            },
            None => LogReadRequest {
                database_id: database_id.to_owned(),
                writer_key: Some(writer_key.to_owned()),
                device_id: Some(device_id.to_owned()),
                from_seq: Some(from_seq),
                cursor: None,
                limit: Some(limit),
            },
        };
        let answer = self.call::<LogRead>(&request).await?;
        // This client reads inline entries only: a log segment is not an entry it can restore.
        let entries = answer
            .entries
            .into_iter()
            .map(|item| match item {
                ReadItem::Entry(value) => serde_json::from_value::<SignedEntry>(value).ok(),
                ReadItem::Segment { .. } => None,
            })
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| "relay log.read returned invalid entries".to_owned())?;
        Ok((entries, answer.cursor))
    }

    /// Send `request` to the route of `E` and parse its 200 answer.
    async fn call<E: Endpoint>(&self, request: &E::Request) -> Result<E::Answer, String> {
        let path = E::ROUTE.path();
        let database_id = E::database_id(request);
        let raw = serde_json::to_vec(request).map_err(|_| "cannot encode relay body")?;
        let url = format!("{}{path}", self.base);
        let mut retried = 0u32;
        loop {
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
                    async_sleep(retry_wait(retried)).await;
                    retried += 1;
                    continue;
                }
                Err(e) => return Err(format!("relay request failed: {e}")),
            };
            let status = resp.status();
            let text = match read_bounded_text(resp, started, retried < RETRIES).await {
                BodyRead::Ok(text) => text,
                BodyRead::Retryable => {
                    async_sleep(retry_wait(retried)).await;
                    retried += 1;
                    continue;
                }
                BodyRead::Fatal(err) => return Err(err),
            };
            if status.is_success() {
                return serde_json::from_str(&text).map_err(|error| {
                    if error.is_data() {
                        format!("relay {path} returned an invalid answer")
                    } else {
                        "relay response is not JSON".into()
                    }
                });
            }
            if retried < RETRIES && is_transient(status.as_u16(), &text) {
                async_sleep(retry_wait(retried)).await;
                retried += 1;
                continue;
            }
            return Err(format!("relay {path} HTTP {status}: {text}"));
        }
    }
}

async fn read_bounded_text(
    mut resp: reqwest::Response,
    started: Instant,
    may_retry: bool,
) -> BodyRead {
    if resp
        .content_length()
        .is_some_and(|len| len > MAX_RESPONSE_BYTES as u64)
    {
        return BodyRead::Fatal(format!("relay response exceeds {MAX_RESPONSE_BYTES} bytes"));
    }
    let mut buf = Vec::new();
    loop {
        match resp.chunk().await {
            Ok(Some(chunk)) => {
                if buf.len().saturating_add(chunk.len()) > MAX_RESPONSE_BYTES {
                    return BodyRead::Fatal(format!(
                        "relay response exceeds {MAX_RESPONSE_BYTES} bytes"
                    ));
                }
                buf.extend_from_slice(&chunk);
            }
            Ok(None) => break,
            Err(e) if may_retry && retryable_transport(&e, started.elapsed()) => {
                return BodyRead::Retryable;
            }
            Err(e) => return BodyRead::Fatal(format!("relay response body: {e}")),
        }
    }
    match String::from_utf8(buf) {
        Ok(text) => BodyRead::Ok(text),
        Err(_) => BodyRead::Fatal("relay response is not UTF-8".into()),
    }
}

fn next_append_chunk_len(database_id: &str, remaining: &[SignedEntry]) -> Result<usize, String> {
    let overhead = 32usize.saturating_add(database_id.len());
    let mut used = overhead;
    let mut count = 0usize;
    for entry in remaining.iter().take(APPEND_MAX_ENTRIES) {
        let item_len = serde_json::to_vec(entry)
            .map_err(|_| "cannot encode signed entry".to_owned())?
            .len();
        let next_used = used.saturating_add(item_len).saturating_add(1);
        if next_used > APPEND_MAX_BYTES {
            if count == 0 {
                return Err(format!(
                    "signed entry seq {} exceeds {APPEND_MAX_BYTES} byte append limit ({item_len} bytes)",
                    entry.header().writer_seq
                ));
            }
            break;
        }
        used = next_used;
        count += 1;
    }
    Ok(count)
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

/// `(stored, duplicate)` in entries for an append of `sent` entries. A duplicate answer says that
/// the entries the relay did not store it held already.
fn append_counts(answer: LogAppendAnswer, sent: u64) -> (u64, u64) {
    if answer.stored < sent && answer.duplicate {
        (answer.stored, sent.saturating_sub(answer.stored))
    } else {
        (answer.stored, 0)
    }
}

fn rand_32() -> [u8; 32] {
    use rand::RngCore;
    let mut out = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut out);
    out
}
